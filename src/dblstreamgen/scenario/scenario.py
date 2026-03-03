"""Scenario -- public entry point facade.

One class for the user to learn.  Delegates to ``ScenarioBuilder`` and
``ScenarioRunner``.
"""

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from dblstreamgen.config import Config, ConfigurationError
from dblstreamgen.scenario.builder import ScenarioBuilder
from dblstreamgen.scenario.runner import ScenarioRunner
from dblstreamgen.scenario.types import ScenarioResult, SinkPlan

logger = logging.getLogger(__name__)


class Scenario:
    """Public entry point.  One class for the user to learn."""

    def __init__(self, spark: SparkSession, config: Config):
        self._spark = spark
        self._config = config
        self._builder = ScenarioBuilder(spark, config)
        self._runner = ScenarioRunner(self._builder, config)

    def build(self, serialize: Optional[bool] = None) -> DataFrame:
        """Build a DataFrame.  Works for streaming and batch.

        Parameters
        ----------
        serialize:
            None = use config, True = force narrow, False = force wide.
        """
        df = self._builder.build(serialize=serialize)

        should_serialize = serialize
        if should_serialize is None:
            should_serialize = self._config.serialization is not None

        if should_serialize:
            ser = self._config.serialization
            if ser is None:
                raise ConfigurationError(
                    "serialize=True requested but no 'serialization' section in config."
                )
            from dblstreamgen.serialization import serialize_dataframe

            df = serialize_dataframe(df, ser)

        return df

    def dry_run(self) -> DataFrame:
        """Build DataFrame and validate Spark plan compiles."""
        df = self.build()
        df.explain(True)
        return df

    def explain(self) -> str:
        """Human-readable breakdown of hidden columns and dedup groups."""
        return self._builder.explain()

    def plan(self) -> list[SinkPlan]:
        """Return execution plan without running."""
        return self._runner.plan()

    def run(self, sink_factory: Any, checkpoint_base: str) -> ScenarioResult:
        """Run full scenario.  Blocks until duration_seconds expires.

        Streaming only -- raises ``ConfigurationError`` for batch mode.
        """
        return self._runner.run(sink_factory, checkpoint_base)

    def cleanup_checkpoints(self, checkpoint_base: str) -> None:
        """Delete all checkpoint directories under the base path."""
        self._runner.cleanup_checkpoints(checkpoint_base)

    def __repr__(self) -> str:
        n_types = len([et for et in self._config.event_types if float(et.get("weight", 0)) > 0])
        n_total = len(self._config.event_types)
        mode = self._config.generation_mode
        zero = f" (+{n_total - n_types} zero-weight)" if n_total > n_types else ""
        return (
            f"Scenario(mode={mode}, event_types={n_types}{zero}, "
            f"config={self._config.source_name!r})"
        )
