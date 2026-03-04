"""Integration tests for ScenarioBuilder.  Requires Spark."""

from pyspark.sql import functions as F

from dblstreamgen.config import Config
from dblstreamgen.scenario.builder import ScenarioBuilder


def _cfg(**overrides):
    base = {
        "generation_mode": "batch",
        "scenario": {"total_rows": 5000, "partitions": 4},
        "common_fields": {
            "event_name": {"event_type_id": True},
        },
        "event_types": [
            {"event_type_id": "a", "weight": 0.60, "fields": {
                "val": {"type": "int", "range": [1, 100]},
            }},
            {"event_type_id": "b", "weight": 0.40, "fields": {
                "val": {"type": "int", "range": [1, 100]},
            }},
        ],
    }
    base.update(overrides)
    return Config.from_dict(base)


class TestSchemaCorrectness:
    def test_expected_columns(self, spark):
        config = _cfg()
        builder = ScenarioBuilder(spark, config)
        df = builder.build()
        assert set(df.columns) == {"event_name", "val"}

    def test_no_internal_columns(self, spark):
        config = _cfg()
        df = ScenarioBuilder(spark, config).build()
        internal = [c for c in df.columns if c.startswith("__dsg_")]
        assert internal == []

    def test_row_count(self, spark):
        config = _cfg()
        df = ScenarioBuilder(spark, config).build()
        assert df.count() == 5000


class TestEventTypeDistribution:
    def test_proportions(self, spark):
        config = _cfg()
        df = ScenarioBuilder(spark, config).build()
        counts = {r["event_name"]: r["count"]
                  for r in df.groupBy("event_name").count().collect()}
        total = sum(counts.values())
        a_pct = counts["a"] / total
        b_pct = counts["b"] / total
        assert abs(a_pct - 0.60) < 0.05
        assert abs(b_pct - 0.40) < 0.05


class TestConditionalNulls:
    def test_exclusive_fields(self, spark):
        config = Config.from_dict({
            "generation_mode": "batch",
            "scenario": {"total_rows": 5000, "partitions": 2},
            "common_fields": {"event_name": {"event_type_id": True}},
            "event_types": [
                {"event_type_id": "x", "weight": 0.50, "fields": {
                    "only_x": {"type": "int", "range": [1, 10]},
                }},
                {"event_type_id": "y", "weight": 0.50, "fields": {
                    "only_y": {"type": "string", "values": ["hello", "world"]},
                }},
            ],
        })
        df = ScenarioBuilder(spark, config).build()

        x_rows = df.filter(F.col("event_name") == "x")
        y_rows = df.filter(F.col("event_name") == "y")

        assert x_rows.filter(F.col("only_x").isNull()).count() == 0
        assert x_rows.filter(F.col("only_y").isNotNull()).count() == 0
        assert y_rows.filter(F.col("only_y").isNull()).count() == 0
        assert y_rows.filter(F.col("only_x").isNotNull()).count() == 0


class TestDerivedFields:
    def test_topo_sort_derived(self, spark):
        config = Config.from_dict({
            "generation_mode": "batch",
            "scenario": {"total_rows": 1000, "partitions": 2},
            "common_fields": {
                "event_name": {"event_type_id": True},
                "is_high": {
                    "type": "boolean",
                    "expr": "val > 50",
                    "base_columns": ["val"],
                },
            },
            "event_types": [
                {"event_type_id": "a", "weight": 1.0, "fields": {
                    "val": {"type": "int", "range": [1, 100]},
                }},
            ],
        })
        df = ScenarioBuilder(spark, config).build()
        assert "is_high" in df.columns
        high_rows = df.filter(F.col("is_high")).select("val")
        assert high_rows.filter(F.col("val") <= 50).count() == 0


class TestZeroWeightExclusion:
    def test_zero_weight_excluded(self, spark):
        config = Config.from_dict({
            "generation_mode": "batch",
            "scenario": {"total_rows": 2000, "partitions": 2},
            "common_fields": {"event_name": {"event_type_id": True}},
            "event_types": [
                {"event_type_id": "active", "weight": 1.0, "fields": {}},
                {"event_type_id": "inactive", "weight": 0.0, "fields": {}},
            ],
        })
        df = ScenarioBuilder(spark, config).build()
        types = {r["event_name"] for r in df.select("event_name").distinct().collect()}
        assert types == {"active"}


class TestSpecDedup:
    def test_shared_spec_reduces_hidden_cols(self, spark):
        config = Config.from_dict({
            "generation_mode": "batch",
            "scenario": {"total_rows": 1000, "partitions": 2},
            "common_fields": {"event_name": {"event_type_id": True}},
            "event_types": [
                {"event_type_id": "a", "weight": 0.50, "fields": {
                    "price": {"type": "float", "range": [10.0, 500.0]},
                }},
                {"event_type_id": "b", "weight": 0.50, "fields": {
                    "price": {"type": "float", "range": [10.0, 500.0]},
                }},
            ],
        })
        builder = ScenarioBuilder(spark, config)
        df = builder.build()
        assert "price" in df.columns
        explain_text = builder.explain()
        assert "1 hidden" in explain_text
