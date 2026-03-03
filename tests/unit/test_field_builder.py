"""Unit tests for FieldBuilder.resolve_params.  No Spark required."""

import sys

import pytest

from dblstreamgen.scenario.field_builder import FieldBuilder, FieldResolution


class TestResolveParamsStrategy:
    """Verify each field type maps to the correct strategy."""

    def test_int_with_range(self):
        r = FieldBuilder.resolve_params({"type": "int", "range": [1, 100]})
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["minValue"] == 1
        assert r.dbldatagen_kwargs["maxValue"] == 100
        assert r.dbldatagen_kwargs["random"] is True
        assert r.spark_type == "int"

    def test_float_with_range(self):
        r = FieldBuilder.resolve_params({"type": "float", "range": [0.5, 99.5]})
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["minValue"] == 0.5
        assert r.dbldatagen_kwargs["maxValue"] == 99.5

    def test_long_with_range(self):
        r = FieldBuilder.resolve_params({"type": "long", "range": [0, 999999]})
        assert r.strategy == "native"
        assert r.spark_type == "bigint"

    def test_double_with_range(self):
        r = FieldBuilder.resolve_params({"type": "double", "range": [0.0, 1.0]})
        assert r.strategy == "native"
        assert r.spark_type == "double"

    def test_short_with_range(self):
        r = FieldBuilder.resolve_params({"type": "short", "range": [0, 100]})
        assert r.strategy == "native"
        assert r.spark_type == "smallint"

    def test_byte_with_range(self):
        r = FieldBuilder.resolve_params({"type": "byte", "range": [0, 50]})
        assert r.strategy == "native"
        assert r.spark_type == "tinyint"

    def test_decimal_with_range(self):
        r = FieldBuilder.resolve_params({
            "type": "decimal", "range": [0, 999.99], "precision": 10, "scale": 2
        })
        assert r.strategy == "native"
        assert r.spark_type == "decimal(10,2)"

    def test_int_without_range_uses_defaults(self):
        r = FieldBuilder.resolve_params({"type": "int"})
        assert r.strategy == "native"
        assert "minValue" in r.dbldatagen_kwargs
        assert "maxValue" in r.dbldatagen_kwargs

    def test_string_with_values(self):
        r = FieldBuilder.resolve_params({
            "type": "string", "values": ["a", "b", "c"], "weights": [5, 3, 2]
        })
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["values"] == ["a", "b", "c"]
        assert r.dbldatagen_kwargs["weights"] == [5, 3, 2]
        assert r.dbldatagen_kwargs["random"] is True

    def test_string_without_values(self):
        r = FieldBuilder.resolve_params({"type": "string"})
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["values"] == ["value"]

    def test_boolean_without_values(self):
        r = FieldBuilder.resolve_params({"type": "boolean"})
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["values"] == [True, False]
        assert r.dbldatagen_kwargs["random"] is True

    def test_boolean_with_values(self):
        r = FieldBuilder.resolve_params({
            "type": "boolean", "values": [True, False], "weights": [7, 3]
        })
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["weights"] == [7, 3]

    def test_timestamp_historical_random(self):
        r = FieldBuilder.resolve_params({
            "type": "timestamp",
            "begin": "2024-01-01 00:00:00",
            "end": "2024-12-31 23:59:59",
            "random": True,
        })
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["begin"] == "2024-01-01 00:00:00"
        assert r.dbldatagen_kwargs["end"] == "2024-12-31 23:59:59"
        assert r.dbldatagen_kwargs["random"] is True
        assert r.spark_type == "timestamp"

    def test_timestamp_historical_linear(self):
        r = FieldBuilder.resolve_params({
            "type": "timestamp",
            "begin": "2024-06-15 00:00:00",
            "end": "2024-06-15 23:59:59",
            "random": False,
            "interval": "1 second",
        })
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs.get("random") is not True
        assert "interval" in r.dbldatagen_kwargs

    def test_timestamp_current_mode(self):
        r = FieldBuilder.resolve_params({
            "type": "timestamp", "mode": "current", "jitter_seconds": 5
        })
        assert r.strategy == "sql_expr"
        assert "current_timestamp()" in r.sql_expr
        assert "make_interval" in r.sql_expr

    def test_timestamp_current_no_jitter(self):
        r = FieldBuilder.resolve_params({"type": "timestamp", "mode": "current"})
        assert r.strategy == "sql_expr"
        assert r.sql_expr == "current_timestamp()"

    def test_date_with_begin_end(self):
        r = FieldBuilder.resolve_params({
            "type": "date", "begin": "2024-01-01", "end": "2024-12-31"
        })
        assert r.strategy == "native"
        assert r.spark_type == "date"

    def test_uuid(self):
        r = FieldBuilder.resolve_params({"type": "uuid"})
        assert r.strategy == "sql_expr"
        assert r.sql_expr == "uuid()"
        assert r.spark_type == "string"

    def test_binary(self):
        r = FieldBuilder.resolve_params({"type": "binary"})
        assert r.strategy == "sql_expr"
        assert "unhex" in r.sql_expr
        assert r.spark_type == "binary"

    def test_faker(self):
        r = FieldBuilder.resolve_params({"type": "string", "faker": "name"})
        assert r.strategy == "faker"
        assert r.dbldatagen_kwargs["faker_method"] == "name"

    def test_faker_with_args(self):
        r = FieldBuilder.resolve_params({
            "type": "string", "faker": "bothify", "faker_args": {"text": "##-??"}
        })
        assert r.strategy == "faker"
        assert r.dbldatagen_kwargs["faker_args"] == {"text": "##-??"}

    def test_expr_passthrough(self):
        r = FieldBuilder.resolve_params({"type": "string", "expr": "concat('a', 'b')"})
        assert r.strategy == "sql_expr"
        assert r.sql_expr == "concat('a', 'b')"

    def test_derived_field_expr_with_base_columns(self):
        r = FieldBuilder.resolve_params({
            "type": "boolean", "expr": "amount > 100", "base_columns": ["amount"]
        })
        assert r.strategy == "sql_expr"
        assert r.sql_expr == "amount > 100"


class TestResolveParamsComplexTypes:
    def test_struct(self):
        r = FieldBuilder.resolve_params({
            "type": "struct",
            "fields": {"price": {"type": "float"}, "qty": {"type": "int"}},
        })
        assert r.strategy == "complex"

    def test_array(self):
        r = FieldBuilder.resolve_params({
            "type": "array", "item_type": "string",
            "values": ["a", "b", "c"], "num_features": [1, 3],
        })
        assert r.strategy == "complex"

    def test_map(self):
        r = FieldBuilder.resolve_params({
            "type": "map", "key_type": "string", "value_type": "string",
        })
        assert r.strategy == "complex"


class TestPercentNulls:
    def test_native_percent_nulls_no_outliers(self):
        r = FieldBuilder.resolve_params({
            "type": "int", "range": [0, 100], "percent_nulls": 0.1
        })
        assert r.strategy == "native"
        assert r.dbldatagen_kwargs["percentNulls"] == 0.1

    def test_percent_nulls_with_outliers_stays_native(self):
        """When outliers present, percent_nulls is removed from kwargs
        (SQL wrapping will handle it in the builder)."""
        r = FieldBuilder.resolve_params({
            "type": "int", "range": [0, 100], "percent_nulls": 0.1,
            "outliers": [{"percent": 0.01, "expr": "-999"}],
        })
        assert r.strategy == "native"
        assert "percentNulls" not in r.dbldatagen_kwargs


class TestSparkTypeResolution:
    def test_int(self):
        assert FieldBuilder.resolve_params({"type": "int"}).spark_type == "int"

    def test_long(self):
        assert FieldBuilder.resolve_params({"type": "long"}).spark_type == "bigint"

    def test_decimal(self):
        r = FieldBuilder.resolve_params({
            "type": "decimal", "precision": 18, "scale": 6
        })
        assert r.spark_type == "decimal(18,6)"

    def test_array_type(self):
        r = FieldBuilder.resolve_params({"type": "array", "item_type": "int"})
        assert r.spark_type == "array<int>"

    def test_map_type(self):
        r = FieldBuilder.resolve_params({
            "type": "map", "key_type": "string", "value_type": "int"
        })
        assert r.spark_type == "map<string,int>"

    def test_struct_type(self):
        r = FieldBuilder.resolve_params({
            "type": "struct",
            "fields": {"a": {"type": "int"}, "b": {"type": "string"}},
        })
        assert r.spark_type == "struct<a:int,b:string>"


class TestNoSparkImports:
    """Verify resolve_params works without pyspark in sys.modules."""

    def test_resolve_params_no_spark_dependency(self):
        saved = {}
        modules_to_hide = [k for k in sys.modules if k.startswith("pyspark")]
        for mod in modules_to_hide:
            saved[mod] = sys.modules.pop(mod)
        try:
            r = FieldBuilder.resolve_params({"type": "int", "range": [1, 10]})
            assert r.strategy == "native"
        finally:
            sys.modules.update(saved)


class TestFieldResolutionDataclass:
    def test_invalid_strategy_raises(self):
        with pytest.raises(ValueError, match="strategy must be one of"):
            FieldResolution(
                strategy="invalid",
                dbldatagen_kwargs={},
                sql_expr=None,
                spark_type="int",
            )
