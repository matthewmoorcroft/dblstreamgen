"""Unit tests for ScenarioRunner.plan().  No Spark required."""

import pytest

from dblstreamgen.config import Config, ConfigurationError
from dblstreamgen.scenario.runner import ScenarioRunner


class FakeBuilder:
    """Mock builder that should never be called during plan()."""

    def build(self, **kwargs):
        raise AssertionError("plan() should not call builder.build()")


def _streaming_config(**overrides):
    base = {
        "generation_mode": "streaming",
        "scenario": {
            "duration_seconds": 300,
            "baseline_rows_per_second": 1000,
        },
        "common_fields": {"event_name": {"event_type_id": True}},
        "event_types": [
            {"event_type_id": "a", "weight": 0.60, "fields": {}},
            {"event_type_id": "b", "weight": 0.30, "fields": {}},
            {"event_type_id": "c", "weight": 0.10, "fields": {}},
        ],
    }
    base.update(overrides)
    return Config.from_dict(base)


class TestBaselinePlan:
    def test_baseline_only_single_entry(self):
        config = _streaming_config()
        runner = ScenarioRunner(FakeBuilder(), config)
        plans = runner.plan()

        assert len(plans) == 1
        p = plans[0]
        assert p.name == "baseline"
        assert p.start_seconds == 0
        assert p.stop_seconds == 300
        assert p.rows_per_second == 1000
        assert set(p.event_types) == {"a", "b", "c"}

    def test_zero_weight_excluded_from_plan(self):
        config = Config.from_dict({
            "generation_mode": "streaming",
            "scenario": {"duration_seconds": 60, "baseline_rows_per_second": 500},
            "event_types": [
                {"event_type_id": "active", "weight": 1.0, "fields": {}},
                {"event_type_id": "dormant", "weight": 0.0, "fields": {}},
            ],
        })
        runner = ScenarioRunner(FakeBuilder(), config)
        plans = runner.plan()

        assert plans[0].event_types == ["active"]


class TestSpikePlan:
    def test_spike_produces_two_entries(self):
        config = _streaming_config(scenario={
            "duration_seconds": 300,
            "baseline_rows_per_second": 1000,
            "spike": {
                "at_seconds": 60,
                "for_seconds": 120,
                "additional_rows_per_second": 5000,
            },
        })
        runner = ScenarioRunner(FakeBuilder(), config)
        plans = runner.plan()

        assert len(plans) == 2
        assert plans[0].name == "baseline"
        assert plans[1].name == "spike"
        assert plans[1].start_seconds == 60
        assert plans[1].stop_seconds == 180
        assert plans[1].rows_per_second == 5000

    def test_targeted_spike_subset(self):
        config = _streaming_config(scenario={
            "duration_seconds": 300,
            "baseline_rows_per_second": 1000,
            "spike": {
                "at_seconds": 10,
                "for_seconds": 20,
                "additional_rows_per_second": 3000,
                "targets": [
                    {"event_type_id": "a", "weight": 0.80},
                    {"event_type_id": "b", "weight": 0.20},
                ],
            },
        })
        runner = ScenarioRunner(FakeBuilder(), config)
        plans = runner.plan()

        spike_plan = plans[1]
        assert set(spike_plan.event_types) == {"a", "b"}
        assert spike_plan.weights["a"] == 0.80
        assert spike_plan.weights["b"] == 0.20


class TestPlanBatchRejection:
    def test_batch_mode_raises(self):
        config = Config.from_dict({
            "generation_mode": "batch",
            "scenario": {"total_rows": 10000},
            "event_types": [
                {"event_type_id": "a", "weight": 1.0, "fields": {}},
            ],
        })
        runner = ScenarioRunner(FakeBuilder(), config)
        with pytest.raises(ConfigurationError, match="streaming"):
            runner.plan()


class TestRampPlan:
    def _ramp_config(self, **ramp_overrides):
        ramp = {
            "step_seconds": 30,
            "additional_rows_per_second": 1000,
        }
        ramp.update(ramp_overrides)
        return Config.from_dict({
            "generation_mode": "streaming",
            "scenario": {
                "duration_seconds": 120,
                "baseline_rows_per_second": 1000,
                "ramp": ramp,
            },
            "common_fields": {"event_name": {"event_type_id": True}},
            "event_types": [
                {"event_type_id": "a", "weight": 0.60, "fields": {}},
                {"event_type_id": "b", "weight": 0.40, "fields": {}},
            ],
        })

    def test_ramp_produces_baseline_plus_steps(self):
        config = self._ramp_config()
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        # duration=120, step_seconds=30 → baseline + steps at t=30, 60, 90
        names = [p.name for p in plans]
        assert names[0] == "baseline"
        assert "ramp_step_1" in names
        assert "ramp_step_2" in names
        assert "ramp_step_3" in names
        # Step at t=120 is excluded (== duration)
        assert "ramp_step_4" not in names

    def test_ramp_baseline_stops_at_first_step(self):
        config = self._ramp_config()
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        baseline = plans[0]
        assert baseline.stop_seconds == 30

    def test_ramp_rates_follow_rolling_formula(self):
        # baseline=1000, additional=1000
        # Step 1 rate = oldest(baseline=1000) + 1000 = 2000
        # Step 2 rate = oldest(step1=2000) + 1000 = 3000
        # Step 3 rate = oldest(step2=3000) + 1000 = 4000
        config = self._ramp_config(step_seconds=30, additional_rows_per_second=1000)
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        by_name = {p.name: p for p in plans}
        assert by_name["ramp_step_1"].rows_per_second == 2000
        assert by_name["ramp_step_2"].rows_per_second == 3000
        assert by_name["ramp_step_3"].rows_per_second == 4000

    def test_ramp_max_rows_per_second_caps_rate(self):
        config = self._ramp_config(
            step_seconds=30,
            additional_rows_per_second=1000,
            max_rows_per_second=2500,
        )
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        # Step 1 = 2000, step 2 would be 3000 but capped at 2500 → ramp stops
        rates = [p.rows_per_second for p in plans if p.name.startswith("ramp")]
        assert rates[0] == 2000
        assert rates[-1] == 2500
        assert all(r <= 2500 for r in rates)

    def test_ramp_step_times(self):
        config = self._ramp_config(step_seconds=30)
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        by_name = {p.name: p for p in plans}
        assert by_name["ramp_step_1"].start_seconds == 30
        assert by_name["ramp_step_2"].start_seconds == 60
        assert by_name["ramp_step_3"].start_seconds == 90

    def test_ramp_last_step_clipped_to_duration(self):
        config = self._ramp_config(step_seconds=30)
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        by_name = {p.name: p for p in plans}
        assert by_name["ramp_step_3"].stop_seconds == 120  # clipped to duration

    def test_ramp_event_types_inherited(self):
        config = self._ramp_config()
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        for p in plans:
            assert set(p.event_types) == {"a", "b"}

    def test_ramp_and_spike_spike_ignored(self):
        """When both ramp and spike are present, ramp takes precedence."""
        config = Config.from_dict({
            "generation_mode": "streaming",
            "scenario": {
                "duration_seconds": 120,
                "baseline_rows_per_second": 1000,
                "ramp": {"step_seconds": 30, "additional_rows_per_second": 1000},
                "spike": {"at_seconds": 10, "for_seconds": 20, "additional_rows_per_second": 5000},
            },
            "event_types": [{"event_type_id": "a", "weight": 1.0, "fields": {}}],
        })
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        names = [p.name for p in plans]
        assert "spike" not in names
        assert any(n.startswith("ramp") for n in names)

    def test_plan_populates_hidden_column_count(self):
        """hidden_column_count reflects actual dedup analysis (not always 0)."""
        config = Config.from_dict({
            "generation_mode": "streaming",
            "scenario": {"duration_seconds": 60, "baseline_rows_per_second": 1000},
            "event_types": [
                {"event_type_id": "a", "weight": 0.50, "fields": {
                    "price": {"type": "float", "range": [1.0, 100.0]},
                }},
                {"event_type_id": "b", "weight": 0.50, "fields": {
                    "price": {"type": "float", "range": [1.0, 100.0]},
                }},
            ],
        })
        plans = ScenarioRunner(FakeBuilder(), config).plan()
        # 2 event types share identical spec → 1 hidden column
        assert plans[0].hidden_column_count == 1
        assert plans[0].dedup_ratio == 0.5  # 1 hidden / 2 defs = 50% dedup
