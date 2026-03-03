"""Unit tests for SpecDeduplicator.  No Spark required."""

import pytest

from dblstreamgen.scenario.spec_dedup import (
    DeduplicationResult,
    HiddenColumn,
    SpecDeduplicator,
    _normalize_temporal,
)


class TestSignatureComputation:
    """Verify canonical signatures are deterministic and normalize correctly."""

    def test_same_spec_same_signature(self):
        spec = {"type": "float", "range": [10.0, 500.0]}
        s1 = SpecDeduplicator.compute_signature(spec)
        s2 = SpecDeduplicator.compute_signature(spec)
        assert s1 == s2

    def test_different_spec_different_signature(self):
        s1 = SpecDeduplicator.compute_signature({"type": "float", "range": [10.0, 500.0]})
        s2 = SpecDeduplicator.compute_signature({"type": "float", "range": [-500.0, -10.0]})
        assert s1 != s2

    def test_int_float_canonicalization(self):
        """range: [1, 100] and range: [1.0, 100.0] produce the same signature."""
        s1 = SpecDeduplicator.compute_signature({"type": "int", "range": [1, 100]})
        s2 = SpecDeduplicator.compute_signature({"type": "int", "range": [1.0, 100.0]})
        assert s1 == s2

    def test_weight_normalization(self):
        """[6, 3, 1] and [60, 30, 10] normalize to the same ratios."""
        s1 = SpecDeduplicator.compute_signature({
            "type": "string", "values": ["a", "b", "c"], "weights": [6, 3, 1]
        })
        s2 = SpecDeduplicator.compute_signature({
            "type": "string", "values": ["a", "b", "c"], "weights": [60, 30, 10]
        })
        assert s1 == s2

    def test_weight_different_ratios(self):
        s1 = SpecDeduplicator.compute_signature({
            "type": "string", "values": ["a", "b"], "weights": [1, 1]
        })
        s2 = SpecDeduplicator.compute_signature({
            "type": "string", "values": ["a", "b"], "weights": [9, 1]
        })
        assert s1 != s2

    def test_percent_nulls_included(self):
        s1 = SpecDeduplicator.compute_signature({"type": "int", "range": [0, 10]})
        s2 = SpecDeduplicator.compute_signature({
            "type": "int", "range": [0, 10], "percent_nulls": 0.05
        })
        assert s1 != s2

    def test_faker_included(self):
        s1 = SpecDeduplicator.compute_signature({"type": "string", "faker": "name"})
        s2 = SpecDeduplicator.compute_signature({"type": "string", "faker": "email"})
        assert s1 != s2

    def test_temporal_normalization(self):
        s1 = SpecDeduplicator.compute_signature({
            "type": "timestamp", "begin": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"
        })
        s2 = SpecDeduplicator.compute_signature({
            "type": "timestamp", "begin": "2024-01-01 00:00:00", "end": "2024-12-31 23:59:59"
        })
        assert s1 == s2

    def test_type_matters(self):
        s1 = SpecDeduplicator.compute_signature({"type": "int", "range": [0, 100]})
        s2 = SpecDeduplicator.compute_signature({"type": "float", "range": [0, 100]})
        assert s1 != s2

    def test_expr_included(self):
        s1 = SpecDeduplicator.compute_signature({"type": "string", "expr": "uuid()"})
        s2 = SpecDeduplicator.compute_signature({"type": "string", "expr": "'hello'"})
        assert s1 != s2

    def test_decimal_precision_scale(self):
        s1 = SpecDeduplicator.compute_signature({
            "type": "decimal", "precision": 10, "scale": 2, "range": [0, 100]
        })
        s2 = SpecDeduplicator.compute_signature({
            "type": "decimal", "precision": 18, "scale": 6, "range": [0, 100]
        })
        assert s1 != s2


class TestNormalizeTemporal:
    def test_timestamp_parsed(self):
        result = _normalize_temporal("2024-06-15 12:30:00", "timestamp")
        assert result == "2024-06-15T12:30:00"

    def test_date_parsed(self):
        result = _normalize_temporal("2024-06-15", "date")
        assert result == "2024-06-15T00:00:00"

    def test_invalid_passthrough(self):
        result = _normalize_temporal("not-a-date", "timestamp")
        assert result == "not-a-date"


class TestDeduplication:
    """Verify grouping and routing expression generation."""

    def test_all_identical_single_hidden_column(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "user.purchase": {"type": "float", "range": [10.0, 500.0]},
            "user.checkout": {"type": "float", "range": [10.0, 500.0]},
            "system.audit": {"type": "float", "range": [10.0, 500.0]},
        }
        result = dedup.deduplicate("price", event_specs)

        assert len(result.hidden_columns) == 1
        hc = result.hidden_columns[0]
        assert hc.name.startswith("__base_price_")
        assert set(hc.event_type_ids) == {"user.purchase", "user.checkout", "system.audit"}
        assert result.dedup_ratio == pytest.approx(2 / 3)

    def test_all_different_no_dedup(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "a": {"type": "float", "range": [0, 10]},
            "b": {"type": "float", "range": [10, 20]},
            "c": {"type": "float", "range": [20, 30]},
        }
        result = dedup.deduplicate("val", event_specs)

        assert len(result.hidden_columns) == 3
        assert result.dedup_ratio == pytest.approx(0.0)

    def test_mixed_groups(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "a": {"type": "float", "range": [10, 500]},
            "b": {"type": "float", "range": [10, 500]},
            "c": {"type": "float", "range": [-500, -10]},
        }
        result = dedup.deduplicate("price", event_specs)

        assert len(result.hidden_columns) == 2
        assert result.dedup_ratio == pytest.approx(1 / 3)

    def test_routing_expr_has_case_when(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "a": {"type": "int", "range": [0, 10]},
            "b": {"type": "int", "range": [0, 10]},
        }
        result = dedup.deduplicate("count", event_specs)

        assert "CASE" in result.routing_expr
        assert "ELSE NULL END" in result.routing_expr

    def test_single_event_type_uses_equals(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "only_one": {"type": "int", "range": [0, 10]},
        }
        result = dedup.deduplicate("val", event_specs)

        assert "__dsg_event_type_id = 'only_one'" in result.routing_expr

    def test_multiple_event_types_use_in(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "a": {"type": "int", "range": [0, 10]},
            "b": {"type": "int", "range": [0, 10]},
        }
        result = dedup.deduplicate("val", event_specs)

        assert "IN (" in result.routing_expr

    def test_base_columns_include_event_type_and_hidden(self):
        dedup = SpecDeduplicator()
        event_specs = {
            "a": {"type": "int", "range": [0, 10]},
        }
        result = dedup.deduplicate("val", event_specs)

        assert "__dsg_event_type_id" in result.base_columns
        assert result.hidden_columns[0].name in result.base_columns

    def test_hidden_column_name_stability(self):
        """Same input always produces the same hidden column name."""
        spec = {"type": "float", "range": [10.0, 500.0]}
        name1 = SpecDeduplicator._hidden_column_name(
            "price", SpecDeduplicator.compute_signature(spec)
        )
        name2 = SpecDeduplicator._hidden_column_name(
            "price", SpecDeduplicator.compute_signature(spec)
        )
        assert name1 == name2
        assert name1.startswith("__base_price_")

    def test_dedup_ratio_zero_for_single(self):
        dedup = SpecDeduplicator()
        result = dedup.deduplicate("x", {"a": {"type": "int", "range": [0, 1]}})
        assert result.dedup_ratio == 0.0

    def test_field_name_in_result(self):
        dedup = SpecDeduplicator()
        result = dedup.deduplicate("amount", {"a": {"type": "float", "range": [0, 1]}})
        assert result.field_name == "amount"
