"""Integration tests for serialization module.  Requires Spark."""

import json

from pyspark.sql import functions as F

from dblstreamgen.config import Config
from dblstreamgen.scenario.builder import ScenarioBuilder
from dblstreamgen.serialization import serialize_to_json


def _build_wide_df(spark):
    config = Config.from_dict(
        {
            "generation_mode": "batch",
            "scenario": {"total_rows": 500, "partitions": 2},
            "common_fields": {
                "event_name": {"event_type_id": True},
                "event_key": {"type": "string", "values": ["k1", "k2"]},
            },
            "event_types": [
                {
                    "event_type_id": "a",
                    "weight": 0.50,
                    "fields": {
                        "val": {"type": "int", "range": [1, 10]},
                    },
                },
                {
                    "event_type_id": "b",
                    "weight": 0.50,
                    "fields": {
                        "name": {"type": "string", "values": ["alice", "bob"]},
                    },
                },
            ],
        }
    )
    return ScenarioBuilder(spark, config).build()


class TestJsonSerialization:
    def test_output_has_two_columns(self, spark):
        wide = _build_wide_df(spark)
        narrow = serialize_to_json(wide, "event_key")
        assert set(narrow.columns) == {"partition_key", "data"}

    def test_partition_key_populated(self, spark):
        wide = _build_wide_df(spark)
        narrow = serialize_to_json(wide, "event_key")
        assert narrow.filter(F.col("partition_key").isNull()).count() == 0

    def test_data_is_valid_json(self, spark):
        wide = _build_wide_df(spark)
        narrow = serialize_to_json(wide, "event_key")
        sample = narrow.limit(20).collect()
        for row in sample:
            parsed = json.loads(row["data"])
            assert "event_name" in parsed

    def test_null_fields_excluded(self, spark):
        """ignoreNullFields should omit conditional null fields from JSON."""
        wide = _build_wide_df(spark)
        narrow = serialize_to_json(wide, "event_key")
        b_rows = (
            narrow.filter(F.get_json_object(F.col("data"), "$.event_name") == "b")
            .limit(10)
            .collect()
        )
        for row in b_rows:
            parsed = json.loads(row["data"])
            assert "val" not in parsed, "Conditional null 'val' should be omitted"

    def test_row_count_preserved(self, spark):
        wide = _build_wide_df(spark)
        narrow = serialize_to_json(wide, "event_key")
        assert narrow.count() == wide.count()
