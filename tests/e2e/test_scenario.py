"""End-to-end tests for the Scenario facade.  Requires Spark.

Covers the full public API surface: build(), dry_run(), explain(), plan(),
serialization modes, streaming flag, and batch row count.
"""

import pytest
from pyspark.sql import functions as F

from dblstreamgen.config import Config, ConfigurationError
from dblstreamgen.scenario.scenario import Scenario

# ---------------------------------------------------------------------------
# Shared configs
# ---------------------------------------------------------------------------

_BATCH_CONFIG = {
    "generation_mode": "batch",
    "scenario": {"total_rows": 2000, "partitions": 2},
    "common_fields": {
        "event_name": {"event_type_id": True},
        "event_id": {"type": "uuid"},
        "event_key": {
            "type": "string",
            "values": ["k1", "k2", "k3"],
        },
    },
    "event_types": [
        {
            "event_type_id": "order.placed",
            "weight": 0.60,
            "fields": {
                "amount": {"type": "double", "range": [1.0, 500.0]},
            },
        },
        {
            "event_type_id": "order.cancelled",
            "weight": 0.40,
            "fields": {
                "amount": {"type": "double", "range": [1.0, 500.0]},
                "reason": {"type": "string", "values": ["changed_mind", "too_slow"]},
            },
        },
    ],
}

_STREAMING_CONFIG = {
    "generation_mode": "streaming",
    "scenario": {"duration_seconds": 60, "baseline_rows_per_second": 500},
    "common_fields": {
        "event_name": {"event_type_id": True},
    },
    "event_types": [
        {"event_type_id": "click", "weight": 0.70, "fields": {}},
        {"event_type_id": "view", "weight": 0.30, "fields": {}},
    ],
}

_SERIALIZED_CONFIG = {
    **_BATCH_CONFIG,
    "serialization": {"format": "json", "partition_key_field": "event_key"},
}


# ---------------------------------------------------------------------------
# build() — batch
# ---------------------------------------------------------------------------


class TestBatchBuild:
    def test_row_count(self, spark):
        scenario = Scenario(spark, Config.from_dict(_BATCH_CONFIG))
        df = scenario.build(serialize=False)
        assert df.count() == 2000

    def test_expected_columns_present(self, spark):
        df = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).build(serialize=False)
        assert "event_name" in df.columns
        assert "event_id" in df.columns
        assert "event_key" in df.columns
        assert "amount" in df.columns
        assert "reason" in df.columns

    def test_no_internal_columns_leaked(self, spark):
        df = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).build(serialize=False)
        internal = [c for c in df.columns if c.startswith("__")]
        assert internal == [], f"Internal columns leaked: {internal}"

    def test_event_name_values(self, spark):
        df = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).build(serialize=False)
        names = {r["event_name"] for r in df.select("event_name").distinct().collect()}
        assert names == {"order.placed", "order.cancelled"}

    def test_conditional_null_correctness(self, spark):
        df = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).build(serialize=False)
        # 'reason' is only on order.cancelled — should be NULL for order.placed
        placed = df.filter(F.col("event_name") == "order.placed")
        assert placed.filter(F.col("reason").isNotNull()).count() == 0
        # 'amount' is on both types — should never be NULL
        assert df.filter(F.col("amount").isNull()).count() == 0

    def test_event_distribution(self, spark):
        df = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).build(serialize=False)
        counts = {r["event_name"]: r["count"] for r in df.groupBy("event_name").count().collect()}
        total = sum(counts.values())
        assert abs(counts["order.placed"] / total - 0.60) < 0.05
        assert abs(counts["order.cancelled"] / total - 0.40) < 0.05


# ---------------------------------------------------------------------------
# build() — serialize=True (JSON)
# ---------------------------------------------------------------------------


class TestSerializedBuild:
    def test_serialize_true_gives_narrow_schema(self, spark):
        df = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build(serialize=True)
        assert set(df.columns) == {"partition_key", "data"}

    def test_serialize_none_uses_config(self, spark):
        """Config has serialization section → default build() serializes."""
        df = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build()
        assert set(df.columns) == {"partition_key", "data"}

    def test_serialize_false_overrides_config(self, spark):
        """serialize=False forces wide output even when config has serialization."""
        df = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build(serialize=False)
        assert "partition_key" not in df.columns
        assert "event_name" in df.columns

    def test_partition_key_values(self, spark):
        df = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build(serialize=True)
        keys = {r["partition_key"] for r in df.select("partition_key").distinct().collect()}
        assert keys.issubset({"k1", "k2", "k3"})

    def test_data_column_is_json_string(self, spark):
        df = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build(serialize=True)
        sample = df.limit(5).collect()
        import json

        for row in sample:
            parsed = json.loads(row["data"])
            assert "event_name" in parsed

    def test_null_fields_excluded_from_json(self, spark):
        """ignoreNullFields=true means conditional-null fields are absent from JSON."""
        import json

        wide = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build(serialize=False)
        narrow = Scenario(spark, Config.from_dict(_SERIALIZED_CONFIG)).build(serialize=True)
        placed_ids = {
            r["event_id"]
            for r in wide.filter(F.col("event_name") == "order.placed").select("event_id").collect()
        }
        narrow_rows = {r["data"] for r in narrow.collect()}
        for data_str in narrow_rows:
            parsed = json.loads(data_str)
            if parsed.get("event_id") in placed_ids:
                assert "reason" not in parsed, "reason should be absent from order.placed JSON"
                break


# ---------------------------------------------------------------------------
# build() — streaming
# ---------------------------------------------------------------------------


class TestStreamingBuild:
    def test_streaming_df_is_streaming(self, spark):
        df = Scenario(spark, Config.from_dict(_STREAMING_CONFIG)).build(serialize=False)
        assert df.isStreaming

    def test_streaming_schema(self, spark):
        df = Scenario(spark, Config.from_dict(_STREAMING_CONFIG)).build(serialize=False)
        assert "event_name" in df.columns
        assert not any(c.startswith("__") for c in df.columns)


# ---------------------------------------------------------------------------
# dry_run()
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_returns_dataframe(self, spark):
        from pyspark.sql import DataFrame

        df = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).dry_run()
        assert isinstance(df, DataFrame)

    def test_dry_run_compiles_without_error(self, spark):
        # If the Spark plan doesn't compile, dry_run() raises.
        Scenario(spark, Config.from_dict(_BATCH_CONFIG)).dry_run()


# ---------------------------------------------------------------------------
# explain()
# ---------------------------------------------------------------------------


class TestExplain:
    def test_explain_returns_string(self, spark):
        result = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).explain()
        assert isinstance(result, str)

    def test_explain_contains_event_type_count(self, spark):
        result = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).explain()
        assert "2 active event types" in result

    def test_explain_shows_dedup_info(self, spark):
        # 'amount' is shared across both event types with identical spec → dedup
        result = Scenario(spark, Config.from_dict(_BATCH_CONFIG)).explain()
        assert "amount" in result
        assert "hidden" in result


# ---------------------------------------------------------------------------
# plan()
# ---------------------------------------------------------------------------


class TestPlan:
    def test_plan_returns_list_of_sink_plans(self, spark):
        from dblstreamgen.scenario.types import SinkPlan

        plans = Scenario(spark, Config.from_dict(_STREAMING_CONFIG)).plan()
        assert isinstance(plans, list)
        assert all(isinstance(p, SinkPlan) for p in plans)

    def test_plan_baseline_entry(self, spark):
        plans = Scenario(spark, Config.from_dict(_STREAMING_CONFIG)).plan()
        assert plans[0].name == "baseline"
        assert plans[0].rows_per_second == 500
        assert plans[0].start_seconds == 0
        assert plans[0].stop_seconds == 60

    def test_plan_raises_for_batch(self, spark):
        with pytest.raises(ConfigurationError, match="streaming"):
            Scenario(spark, Config.from_dict(_BATCH_CONFIG)).plan()


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_batch(self, spark):
        r = repr(Scenario(spark, Config.from_dict(_BATCH_CONFIG)))
        assert "mode=batch" in r
        assert "event_types=2" in r

    def test_repr_streaming(self, spark):
        r = repr(Scenario(spark, Config.from_dict(_STREAMING_CONFIG)))
        assert "mode=streaming" in r
