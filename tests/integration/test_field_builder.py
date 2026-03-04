"""Integration tests for FieldBuilder.add_field().  Requires Spark.

Validates that each field type produces the expected column values in a
real DataGenerator, including type correctness, range bounds, null rates,
step granularity, and equal-weight distribution.
"""

import dbldatagen as dg
from pyspark.sql import functions as F

from dblstreamgen.scenario.field_builder import FieldBuilder

N_ROWS = 5_000
TOLERANCE = 0.05  # ±5% for distribution checks


def _build_single_column(spark, field_name: str, field_spec: dict, rows: int = N_ROWS):
    """Build a DataGenerator with one test column and return its DataFrame."""
    resolution = FieldBuilder.resolve_params(field_spec)
    fb = FieldBuilder()
    spec = dg.DataGenerator(
        sparkSession=spark, name="test", rows=rows,
        partitions=2, seedMethod="hash_fieldname",
    )
    # Add a base id column so omit=True columns have something to depend on.
    spec = spec.withColumn("__dsg_id", "long", minValue=0, maxValue=10**9, random=True, omit=True)
    spec = fb.add_field(spec, field_name, resolution)
    return spec.build()


class TestNumericTypes:
    def test_int_range_bounds(self, spark):
        df = _build_single_column(spark, "v", {"type": "int", "range": [10, 90]})
        row = df.agg(F.min("v"), F.max("v")).collect()[0]
        assert row[0] >= 10
        assert row[1] <= 90

    def test_long_range_bounds(self, spark):
        df = _build_single_column(spark, "v", {"type": "long", "range": [1_000_000, 9_000_000]})
        row = df.agg(F.min("v"), F.max("v")).collect()[0]
        assert row[0] >= 1_000_000
        assert row[1] <= 9_000_000

    def test_float_range_bounds(self, spark):
        df = _build_single_column(spark, "v", {"type": "float", "range": [0.0, 10.0]})
        row = df.agg(F.min("v").cast("double"), F.max("v").cast("double")).collect()[0]
        assert row[0] >= 0.0
        assert row[1] <= 10.0

    def test_double_step_produces_fractional_values(self, spark):
        """With step=0.01 the max should reach close to the declared bound."""
        df = _build_single_column(spark, "v", {
            "type": "double", "range": [0.5, 2.0], "step": 0.01
        })
        mx = df.agg(F.max("v").cast("double")).collect()[0][0]
        assert mx >= 1.9, f"expected max near 2.0, got {mx}"

    def test_float_step_distinct_values(self, spark):
        """step=0.1 over [0.0, 1.0] → 11 distinct values."""
        df = _build_single_column(spark, "v", {
            "type": "float", "range": [0.0, 1.0], "step": 0.1
        })
        n_distinct = df.select(F.countDistinct("v")).collect()[0][0]
        assert n_distinct >= 10

    def test_decimal_type(self, spark):
        df = _build_single_column(spark, "v", {
            "type": "decimal", "precision": 8, "scale": 2,
            "range": [1.0, 100.0]
        })
        assert df.schema["v"].dataType.typeName() == "decimal"
        row = df.agg(F.min("v"), F.max("v")).collect()[0]
        assert float(row[0]) >= 1.0
        assert float(row[1]) <= 100.0

    def test_short_type(self, spark):
        df = _build_single_column(spark, "v", {"type": "short", "range": [0, 100]})
        assert df.schema["v"].dataType.typeName() == "short"

    def test_byte_type(self, spark):
        df = _build_single_column(spark, "v", {"type": "byte", "range": [0, 127]})
        assert df.schema["v"].dataType.typeName() == "byte"


class TestStringType:
    def test_string_values_only_declared(self, spark):
        vals = ["alpha", "beta", "gamma"]
        df = _build_single_column(spark, "v", {"type": "string", "values": vals})
        found = {r["v"] for r in df.select("v").distinct().collect()}
        assert found.issubset(set(vals))
        assert found == set(vals)

    def test_string_equal_weights_uniform(self, spark):
        """Values without explicit weights should distribute uniformly."""
        vals = ["a", "b", "c", "d"]
        df = _build_single_column(spark, "v", {"type": "string", "values": vals}, rows=10_000)
        counts = {r["v"]: r["count"] for r in df.groupBy("v").count().collect()}
        total = sum(counts.values())
        for v in vals:
            pct = counts.get(v, 0) / total
            assert abs(pct - 0.25) < TOLERANCE, f"Expected ~25% for {v!r}, got {pct:.2%}"

    def test_string_explicit_weights_respected(self, spark):
        df = _build_single_column(spark, "v", {
            "type": "string",
            "values": ["x", "y"],
            "weights": [8, 2],
        }, rows=10_000)
        counts = {r["v"]: r["count"] for r in df.groupBy("v").count().collect()}
        total = sum(counts.values())
        assert abs(counts["x"] / total - 0.8) < TOLERANCE
        assert abs(counts["y"] / total - 0.2) < TOLERANCE


class TestBooleanType:
    def test_boolean_default_50_50(self, spark):
        df = _build_single_column(spark, "v", {"type": "boolean"}, rows=10_000)
        counts = {r["v"]: r["count"] for r in df.groupBy("v").count().collect()}
        total = sum(counts.values())
        assert abs(counts[True] / total - 0.5) < TOLERANCE

    def test_boolean_explicit_weights(self, spark):
        df = _build_single_column(spark, "v", {
            "type": "boolean", "values": [True, False], "weights": [9, 1]
        }, rows=10_000)
        counts = {r["v"]: r["count"] for r in df.groupBy("v").count().collect()}
        total = sum(counts.values())
        assert abs(counts[True] / total - 0.9) < TOLERANCE


class TestTemporalTypes:
    def test_timestamp_within_range(self, spark):
        df = _build_single_column(spark, "v", {
            "type": "timestamp",
            "begin": "2024-01-01 00:00:00",
            "end": "2024-12-31 23:59:59",
            "random": True,
        })
        row = df.agg(
            F.min("v").cast("string"),
            F.max("v").cast("string"),
        ).collect()[0]
        assert row[0] >= "2024-01-01"
        assert row[1] <= "2025-01-01"

    def test_date_within_range(self, spark):
        df = _build_single_column(spark, "v", {
            "type": "date", "begin": "2023-01-01", "end": "2023-12-31"
        })
        row = df.agg(
            F.min("v").cast("string"),
            F.max("v").cast("string"),
        ).collect()[0]
        assert row[0] >= "2023-01-01"
        assert row[1] <= "2023-12-31"


class TestUuidAndBinary:
    def test_uuid_all_unique(self, spark):
        df = _build_single_column(spark, "v", {"type": "uuid"})
        assert df.select(F.countDistinct("v")).collect()[0][0] == N_ROWS

    def test_uuid_format(self, spark):
        sample = _build_single_column(spark, "v", {"type": "uuid"}).limit(5).collect()
        import re
        pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        )
        for row in sample:
            assert pattern.match(row["v"]), f"Not a UUID: {row['v']}"

    def test_binary_type_and_length(self, spark):
        df = _build_single_column(spark, "v", {"type": "binary"})
        assert df.schema["v"].dataType.typeName() == "binary"
        lengths = df.select(F.length("v").alias("l")).agg(
            F.min("l"), F.max("l")
        ).collect()[0]
        assert lengths[0] == 16
        assert lengths[1] == 16


class TestPercentNulls:
    def test_percent_nulls_applied(self, spark):
        df = _build_single_column(spark, "v", {
            "type": "int", "range": [1, 100], "percent_nulls": 0.2
        }, rows=10_000)
        null_count = df.filter(F.col("v").isNull()).count()
        null_pct = null_count / 10_000
        assert abs(null_pct - 0.2) < TOLERANCE


class TestHiddenColumn:
    def test_hidden_column_omitted_from_output(self, spark):
        """add_field(..., hidden=True) must not appear in the built DataFrame."""
        resolution = FieldBuilder.resolve_params({"type": "int", "range": [1, 100]})
        fb = FieldBuilder()
        spec = dg.DataGenerator(
            sparkSession=spark, name="test_hidden", rows=100,
            partitions=2, seedMethod="hash_fieldname",
        )
        spec = fb.add_field(spec, "hidden_col", resolution, hidden=True)
        # Add a visible column so the DataFrame has something
        spec = spec.withColumn("visible", "int", minValue=1, maxValue=10, random=True)
        df = spec.build()
        assert "hidden_col" not in df.columns
        assert "visible" in df.columns
