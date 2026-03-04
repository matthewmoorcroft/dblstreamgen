"""ScenarioRunner -- streaming lifecycle management.

Single-threaded sequential state machine.  All timing via ``time.sleep()``.
No background timers, no ``threading.Timer``, no ``asyncio``.
"""

import logging
import shutil
import time
from collections import deque
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from dblstreamgen.config import Config, ConfigurationError
from dblstreamgen.scenario.builder import ScenarioBuilder
from dblstreamgen.scenario.spec_dedup import SpecDeduplicator
from dblstreamgen.scenario.types import ScenarioResult, SinkPlan

logger = logging.getLogger(__name__)

SinkFactory = Callable[[DataFrame, str], StreamingQuery]


class ScenarioRunner:
    """Schedules builder calls and manages StreamingQuery lifecycle.

    Streaming-only -- raises ``ConfigurationError`` for batch mode.
    """

    def __init__(self, builder: ScenarioBuilder, config: Config):
        self._builder = builder
        self._config = config

    def run(self, sink_factory: SinkFactory, checkpoint_base: str) -> ScenarioResult:
        """Run full scenario.  Blocks until duration_seconds expires.

        All active queries are stopped in a ``finally`` block on any exception.
        Supports baseline-only, spike, and ramp scenarios.  Ramp and spike
        are mutually exclusive -- if both are present, spike is ignored.
        """
        if self._config.generation_mode != "streaming":
            raise ConfigurationError(
                "ScenarioRunner.run() requires generation_mode='streaming'.  "
                "For batch, use Scenario.build() directly."
            )

        scenario = self._config.scenario
        duration = int(scenario["duration_seconds"])
        spike_cfg = scenario.get("spike")
        ramp_cfg = scenario.get("ramp")

        active_queries: list[StreamingQuery] = []
        errors: list = []
        queries_started = 0
        queries_stopped = 0
        t_start = time.monotonic()

        try:
            # -- Baseline -------------------------------------------------
            baseline_df = self._builder.build()
            cp_baseline = f"{checkpoint_base}/baseline"
            q_baseline = sink_factory(baseline_df, cp_baseline)
            active_queries.append(q_baseline)
            queries_started += 1
            logger.info("Baseline query started: %s", cp_baseline)

            # -- Ramp (takes precedence over spike if both present) --------
            if ramp_cfg:
                step_s = int(ramp_cfg["step_seconds"])
                add_rate = int(ramp_cfg["additional_rows_per_second"])
                max_rate = ramp_cfg.get("max_rows_per_second")

                # Rolling window: at most 2 concurrent queries.
                # Each entry is (query, rate).
                rolling: deque = deque()
                rolling.append((q_baseline, int(scenario["baseline_rows_per_second"])))

                step_num = 0
                while True:
                    step_num += 1
                    t_next = step_num * step_s
                    if t_next >= duration:
                        break

                    elapsed = time.monotonic() - t_start
                    wait = max(0.0, t_next - elapsed)
                    if wait > 0:
                        time.sleep(wait)

                    # Compute new rate from the most recent query in the window.
                    _prev_q, prev_rate = rolling[-1]
                    new_rate = prev_rate + add_rate
                    if max_rate is not None and new_rate >= max_rate:
                        new_rate = int(max_rate)

                    cp_ramp = f"{checkpoint_base}/ramp_step_{step_num}"
                    ramp_df = self._builder.build(rows_per_second=new_rate)
                    q_ramp = sink_factory(ramp_df, cp_ramp)
                    active_queries.append(q_ramp)
                    queries_started += 1
                    logger.info(
                        "Ramp step %d started: rate=%d, checkpoint=%s",
                        step_num,
                        new_rate,
                        cp_ramp,
                    )

                    # Make-before-break: new query is live; now stop oldest
                    # if we already have 2 in the window.
                    if len(rolling) >= 2:
                        old_q, _ = rolling.popleft()
                        old_q.stop()
                        active_queries.remove(old_q)
                        queries_stopped += 1
                        logger.info("Ramp: stopped previous query.")

                    rolling.append((q_ramp, new_rate))

                    if max_rate is not None and new_rate >= int(max_rate):
                        logger.info(
                            "Ramp: reached max_rows_per_second=%d, stopping ramp.", max_rate
                        )
                        break

            # -- Spike (only if no ramp) ----------------------------------
            elif spike_cfg:
                at_s = int(spike_cfg["at_seconds"])
                for_s = int(spike_cfg["for_seconds"])
                add_rate = int(spike_cfg["additional_rows_per_second"])

                elapsed = time.monotonic() - t_start
                wait = max(0.0, at_s - elapsed)
                if wait > 0:
                    time.sleep(wait)

                spike_types = self._resolve_spike_types(spike_cfg)
                spike_df = self._builder.build(
                    event_types_with_weights=spike_types,
                    rows_per_second=add_rate,
                )
                cp_spike = f"{checkpoint_base}/spike_{at_s}"
                q_spike = sink_factory(spike_df, cp_spike)
                active_queries.append(q_spike)
                queries_started += 1
                logger.info("Spike query started: rate=%d, duration=%ds", add_rate, for_s)

                time.sleep(for_s)

                q_spike.stop()
                active_queries.remove(q_spike)
                queries_stopped += 1
                logger.info("Spike query stopped.")

            # -- Wait for remaining duration ------------------------------
            elapsed = time.monotonic() - t_start
            remaining = max(0.0, duration - elapsed)
            if remaining > 0:
                time.sleep(remaining)

            # -- Stop all remaining active queries ------------------------
            for q in list(active_queries):
                q.stop()
                queries_stopped += 1
            active_queries.clear()
            logger.info("All queries stopped.")

        except Exception as e:
            logger.error("Scenario failed: %s", e)
            errors.append(e)
        finally:
            for q in list(active_queries):
                try:
                    q.stop()
                    queries_stopped += 1
                except Exception:
                    pass

        total_time = time.monotonic() - t_start
        return ScenarioResult(
            duration_seconds=total_time,
            total_rows_written=None,
            queries_started=queries_started,
            queries_stopped=queries_stopped,
            errors=errors,
        )

    def plan(self) -> list[SinkPlan]:
        """Return execution plan without running.  Pure computation."""
        scenario = self._config.scenario
        mode = self._config.generation_mode
        if mode != "streaming":
            raise ConfigurationError("plan() requires streaming mode.")

        duration = int(scenario["duration_seconds"])
        baseline_rate = int(scenario["baseline_rows_per_second"])
        active = [et for et in self._config.event_types if float(et.get("weight", 0)) > 0]
        active_ids = [et["event_type_id"] for et in active]
        active_weights = {et["event_type_id"]: float(et["weight"]) for et in active}

        hidden_count, dedup_ratio = self._compute_dedup_stats(active)

        plans: list = []

        ramp_cfg = scenario.get("ramp")
        spike_cfg = scenario.get("spike")

        if ramp_cfg:
            step_s = int(ramp_cfg["step_seconds"])
            add_rate = int(ramp_cfg["additional_rows_per_second"])
            max_rate = ramp_cfg.get("max_rows_per_second")

            # Baseline runs until first ramp step.
            baseline_stop = min(step_s, duration)
            plans.append(
                SinkPlan(
                    name="baseline",
                    start_seconds=0,
                    stop_seconds=baseline_stop,
                    rows_per_second=baseline_rate,
                    event_types=active_ids,
                    weights=active_weights,
                    hidden_column_count=hidden_count,
                    dedup_ratio=dedup_ratio,
                )
            )

            # Compute the rolling ramp schedule.
            rolling_rates: deque = deque()
            rolling_rates.append(baseline_rate)

            step_num = 0
            while True:
                step_num += 1
                t_start_step = step_num * step_s
                if t_start_step >= duration:
                    break

                prev_rate = rolling_rates[-1]
                new_rate = prev_rate + add_rate
                if max_rate is not None and new_rate >= int(max_rate):
                    new_rate = int(max_rate)

                t_stop_step = min((step_num + 1) * step_s, duration)
                plans.append(
                    SinkPlan(
                        name=f"ramp_step_{step_num}",
                        start_seconds=t_start_step,
                        stop_seconds=t_stop_step,
                        rows_per_second=new_rate,
                        event_types=active_ids,
                        weights=active_weights,
                        hidden_column_count=hidden_count,
                        dedup_ratio=dedup_ratio,
                    )
                )

                if len(rolling_rates) >= 2:
                    rolling_rates.popleft()
                rolling_rates.append(new_rate)

                if max_rate is not None and new_rate >= int(max_rate):
                    break

        else:
            plans.append(
                SinkPlan(
                    name="baseline",
                    start_seconds=0,
                    stop_seconds=duration,
                    rows_per_second=baseline_rate,
                    event_types=active_ids,
                    weights=active_weights,
                    hidden_column_count=hidden_count,
                    dedup_ratio=dedup_ratio,
                )
            )

            if spike_cfg:
                at_s = int(spike_cfg["at_seconds"])
                for_s = int(spike_cfg["for_seconds"])
                add_rate = int(spike_cfg["additional_rows_per_second"])
                targets = spike_cfg.get("targets")

                if targets:
                    spike_ids = [t["event_type_id"] for t in targets]
                    spike_weights = {t["event_type_id"]: float(t["weight"]) for t in targets}
                else:
                    spike_ids = active_ids
                    spike_weights = active_weights

                plans.append(
                    SinkPlan(
                        name="spike",
                        start_seconds=at_s,
                        stop_seconds=at_s + for_s,
                        rows_per_second=add_rate,
                        event_types=spike_ids,
                        weights=spike_weights,
                        hidden_column_count=hidden_count,
                        dedup_ratio=dedup_ratio,
                    )
                )

        return plans

    def cleanup_checkpoints(self, checkpoint_base: str) -> None:
        """Delete all checkpoint directories under the base path."""
        try:
            shutil.rmtree(checkpoint_base)
            logger.info("Cleaned up checkpoints: %s", checkpoint_base)
        except FileNotFoundError:
            pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _compute_dedup_stats(self, active_types: list[dict]) -> tuple[int, float]:
        """Return (hidden_column_count, dedup_ratio) for the active event types.

        Mirrors the logic in ``ScenarioBuilder.explain()`` without requiring Spark.
        """
        registry: dict[str, dict] = {}
        for et in active_types:
            for fname, fspec in et.get("fields", {}).items():
                registry.setdefault(fname, {})[et["event_type_id"]] = fspec

        total_defs = 0
        total_hidden = 0
        for field_name, event_specs in registry.items():
            dedup = SpecDeduplicator()
            result = dedup.deduplicate(field_name, event_specs)
            total_defs += len(event_specs)
            total_hidden += len(result.hidden_columns)

        ratio = 1.0 - (total_hidden / total_defs) if total_defs > 0 else 0.0
        return total_hidden, ratio

    def _resolve_spike_types(self, spike_cfg: dict) -> list[dict]:
        """Build event_types list for the spike query."""
        targets = spike_cfg.get("targets")
        if targets:
            types_by_id = {et["event_type_id"]: et for et in self._config.event_types}
            result = []
            for t in targets:
                eid = t["event_type_id"]
                base = dict(types_by_id[eid])
                base["weight"] = float(t["weight"])
                result.append(base)
            return result
        else:
            return [et for et in self._config.event_types if float(et.get("weight", 0)) > 0]
