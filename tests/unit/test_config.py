"""Unit tests for v0.4 Config validation.  No Spark required."""

import pytest

from dblstreamgen.config import Config, ConfigurationError, load_config_from_dict


# -----------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------

def _minimal_v04(**overrides):
    """Return a minimal valid v0.4 config dict with optional overrides."""
    cfg = {
        "generation_mode": "streaming",
        "scenario": {
            "duration_seconds": 300,
            "baseline_rows_per_second": 1000,
        },
        "common_fields": {
            "event_name": {"event_type_id": True},
        },
        "event_types": [
            {"event_type_id": "user.page_view", "weight": 0.60, "fields": {}},
            {"event_type_id": "user.click", "weight": 0.30, "fields": {}},
            {"event_type_id": "user.purchase", "weight": 0.10, "fields": {}},
        ],
    }
    cfg.update(overrides)
    return cfg


def _batch_v04(**overrides):
    """Return a minimal valid v0.4 batch config dict."""
    cfg = {
        "generation_mode": "batch",
        "scenario": {
            "total_rows": 10000,
        },
        "event_types": [
            {"event_type_id": "a", "weight": 0.50, "fields": {}},
            {"event_type_id": "b", "weight": 0.50, "fields": {}},
        ],
    }
    cfg.update(overrides)
    return cfg


# -----------------------------------------------------------------------
# Schema version detection
# -----------------------------------------------------------------------

class TestSchemaDetection:
    def test_v04_detected(self):
        c = Config.from_dict(_minimal_v04())
        assert c.schema_version == "v0.4"

    def test_v03_detected(self):
        c = Config.from_dict({
            "generation_mode": "batch",
            "batch_config": {"total_rows": 100, "partitions": 2},
            "sink_config": {"type": "delta", "partition_key_field": "event_key"},
            "event_types": [
                {"event_type_id": "a", "weight": 5, "fields": {}},
                {"event_type_id": "b", "weight": 5, "fields": {}},
            ],
        })
        assert c.schema_version == "v0.3"

    def test_unknown_schema_raises(self):
        with pytest.raises(ConfigurationError, match="Cannot determine config schema"):
            Config.from_dict({"generation_mode": "streaming", "event_types": []})


# -----------------------------------------------------------------------
# v0.4: top-level structure
# -----------------------------------------------------------------------

class TestV04TopLevel:
    def test_valid_streaming_config(self):
        c = Config.from_dict(_minimal_v04())
        assert c.generation_mode == "streaming"

    def test_valid_batch_config(self):
        c = Config.from_dict(_batch_v04())
        assert c.generation_mode == "batch"

    def test_missing_event_types(self):
        d = _minimal_v04()
        del d["event_types"]
        with pytest.raises(ConfigurationError, match="event_types"):
            Config.from_dict(d)

    def test_missing_generation_mode(self):
        d = _minimal_v04()
        del d["generation_mode"]
        with pytest.raises(ConfigurationError, match="generation_mode"):
            Config.from_dict(d)

    def test_invalid_generation_mode(self):
        with pytest.raises(ConfigurationError, match="must be 'streaming' or 'batch'"):
            Config.from_dict(_minimal_v04(generation_mode="realtime"))

    def test_sink_config_rejected(self):
        with pytest.raises(ConfigurationError, match="sink_config"):
            Config.from_dict(_minimal_v04(sink_config={"type": "delta"}))

    def test_derived_fields_rejected(self):
        with pytest.raises(ConfigurationError, match="derived_fields"):
            Config.from_dict(_minimal_v04(derived_fields={"x": {"expr": "1", "type": "int"}}))


# -----------------------------------------------------------------------
# v0.4: scenario section
# -----------------------------------------------------------------------

class TestV04Scenario:
    def test_missing_scenario(self):
        d = _minimal_v04()
        del d["scenario"]
        d["streaming_config"] = {}  # would trigger v0.3 detection
        # Actually, let's put scenario as None
        d2 = _minimal_v04()
        d2["scenario"] = "bad"
        with pytest.raises(ConfigurationError, match="scenario"):
            Config.from_dict(d2)

    def test_streaming_missing_duration(self):
        d = _minimal_v04()
        del d["scenario"]["duration_seconds"]
        with pytest.raises(ConfigurationError, match="duration_seconds"):
            Config.from_dict(d)

    def test_streaming_missing_rows_per_second(self):
        d = _minimal_v04()
        del d["scenario"]["baseline_rows_per_second"]
        with pytest.raises(ConfigurationError, match="baseline_rows_per_second"):
            Config.from_dict(d)

    def test_batch_missing_total_rows(self):
        d = _batch_v04()
        del d["scenario"]["total_rows"]
        with pytest.raises(ConfigurationError, match="total_rows"):
            Config.from_dict(d)

    def test_batch_total_rows_must_be_int(self):
        d = _batch_v04()
        d["scenario"]["total_rows"] = 100.5
        with pytest.raises(ConfigurationError, match="positive integer"):
            Config.from_dict(d)

    def test_seed_default(self):
        c = Config.from_dict(_minimal_v04())
        assert c.scenario.get("seed", 42) == 42

    def test_seed_custom(self):
        d = _minimal_v04()
        d["scenario"]["seed"] = 123
        c = Config.from_dict(d)
        assert c.scenario["seed"] == 123

    def test_seed_must_be_int(self):
        d = _minimal_v04()
        d["scenario"]["seed"] = "bad"
        with pytest.raises(ConfigurationError, match="seed must be an integer"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# v0.4: event type weights
# -----------------------------------------------------------------------

class TestV04Weights:
    def test_weights_sum_to_one(self):
        Config.from_dict(_minimal_v04())  # 0.60 + 0.30 + 0.10 = 1.0

    def test_weights_not_summing_to_one(self):
        d = _minimal_v04()
        d["event_types"][0]["weight"] = 0.50  # 0.50 + 0.30 + 0.10 = 0.90
        with pytest.raises(ConfigurationError, match="must sum to 1.0"):
            Config.from_dict(d)

    def test_weights_tolerance(self):
        d = _minimal_v04()
        d["event_types"] = [
            {"event_type_id": "a", "weight": 1 / 3, "fields": {}},
            {"event_type_id": "b", "weight": 1 / 3, "fields": {}},
            {"event_type_id": "c", "weight": 1 / 3, "fields": {}},
        ]
        Config.from_dict(d)  # 1/3 + 1/3 + 1/3 ≈ 1.0 within tolerance

    def test_zero_weight_allowed(self):
        d = _minimal_v04()
        d["event_types"] = [
            {"event_type_id": "a", "weight": 0.0, "fields": {}},
            {"event_type_id": "b", "weight": 1.0, "fields": {}},
        ]
        Config.from_dict(d)

    def test_negative_weight_rejected(self):
        d = _minimal_v04()
        d["event_types"][0]["weight"] = -0.1
        with pytest.raises(ConfigurationError, match="must be >= 0"):
            Config.from_dict(d)

    def test_missing_weight_rejected(self):
        d = _minimal_v04()
        del d["event_types"][0]["weight"]
        with pytest.raises(ConfigurationError, match="missing 'weight'"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# v0.4: event type IDs
# -----------------------------------------------------------------------

class TestV04EventTypeIds:
    def test_valid_ids(self):
        d = _minimal_v04()
        d["event_types"] = [
            {"event_type_id": "user.page_view", "weight": 0.5, "fields": {}},
            {"event_type_id": "system-audit_v2", "weight": 0.5, "fields": {}},
        ]
        Config.from_dict(d)

    def test_invalid_id_chars(self):
        d = _minimal_v04()
        d["event_types"][0]["event_type_id"] = "user page_view"
        with pytest.raises(ConfigurationError, match="invalid characters"):
            Config.from_dict(d)

    def test_duplicate_ids_rejected(self):
        d = _minimal_v04()
        d["event_types"] = [
            {"event_type_id": "a", "weight": 0.5, "fields": {}},
            {"event_type_id": "a", "weight": 0.5, "fields": {}},
        ]
        with pytest.raises(ConfigurationError, match="duplicated"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# v0.4: spike validation
# -----------------------------------------------------------------------

class TestV04Spike:
    def _spike_config(self, **spike_overrides):
        d = _minimal_v04()
        d["scenario"]["spike"] = {
            "at_seconds": 60,
            "for_seconds": 120,
            "additional_rows_per_second": 5000,
            **spike_overrides,
        }
        return d

    def test_valid_spike(self):
        Config.from_dict(self._spike_config())

    def test_missing_at_seconds(self):
        d = self._spike_config()
        del d["scenario"]["spike"]["at_seconds"]
        with pytest.raises(ConfigurationError, match="at_seconds"):
            Config.from_dict(d)

    def test_valid_spike_targets(self):
        d = self._spike_config(targets=[
            {"event_type_id": "user.page_view", "weight": 0.80},
            {"event_type_id": "user.click", "weight": 0.20},
        ])
        Config.from_dict(d)

    def test_spike_target_unknown_id(self):
        d = self._spike_config(targets=[
            {"event_type_id": "nonexistent", "weight": 1.0},
        ])
        with pytest.raises(ConfigurationError, match="does not exist"):
            Config.from_dict(d)

    def test_spike_target_weights_must_sum_to_one(self):
        d = self._spike_config(targets=[
            {"event_type_id": "user.page_view", "weight": 0.50},
            {"event_type_id": "user.click", "weight": 0.30},
        ])
        with pytest.raises(ConfigurationError, match="must sum to 1.0"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# v0.4: ramp validation
# -----------------------------------------------------------------------

class TestV04Ramp:
    def test_valid_ramp(self):
        d = _minimal_v04()
        d["scenario"]["ramp"] = {
            "step_seconds": 30,
            "additional_rows_per_second": 1000,
        }
        Config.from_dict(d)

    def test_ramp_with_max(self):
        d = _minimal_v04()
        d["scenario"]["ramp"] = {
            "step_seconds": 30,
            "additional_rows_per_second": 1000,
            "max_rows_per_second": 50000,
        }
        Config.from_dict(d)

    def test_ramp_missing_step_seconds(self):
        d = _minimal_v04()
        d["scenario"]["ramp"] = {"additional_rows_per_second": 1000}
        with pytest.raises(ConfigurationError, match="step_seconds"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# v0.4: serialization
# -----------------------------------------------------------------------

class TestV04Serialization:
    def test_no_serialization(self):
        c = Config.from_dict(_minimal_v04())
        assert c.serialization is None

    def test_json_serialization(self):
        d = _minimal_v04(serialization={"format": "json", "partition_key_field": "event_key"})
        c = Config.from_dict(d)
        assert c.serialization["format"] == "json"

    def test_avro_serialization(self):
        d = _minimal_v04(serialization={"format": "avro", "partition_key_field": "event_key"})
        Config.from_dict(d)

    def test_invalid_format(self):
        d = _minimal_v04(serialization={"format": "csv", "partition_key_field": "x"})
        with pytest.raises(ConfigurationError, match="'json' or 'avro'"):
            Config.from_dict(d)

    def test_missing_partition_key(self):
        d = _minimal_v04(serialization={"format": "json"})
        with pytest.raises(ConfigurationError, match="partition_key_field"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# v0.4: derived fields in common_fields
# -----------------------------------------------------------------------

class TestV04DerivedInCommon:
    def test_valid_derived(self):
        d = _minimal_v04()
        d["common_fields"]["is_big"] = {
            "type": "boolean",
            "expr": "amount > 100",
            "base_columns": ["amount"],
        }
        Config.from_dict(d)

    def test_derived_missing_expr(self):
        d = _minimal_v04()
        d["common_fields"]["is_big"] = {
            "type": "boolean",
            "base_columns": ["amount"],
        }
        with pytest.raises(ConfigurationError, match="no 'expr'"):
            Config.from_dict(d)

    def test_derived_missing_type(self):
        d = _minimal_v04()
        d["common_fields"]["is_big"] = {
            "expr": "amount > 100",
            "base_columns": ["amount"],
        }
        with pytest.raises(ConfigurationError, match="no 'type'"):
            Config.from_dict(d)


# -----------------------------------------------------------------------
# Properties & classmethods
# -----------------------------------------------------------------------

class TestConfigAccess:
    def test_source_name_dict(self):
        c = Config.from_dict(_minimal_v04())
        assert c.source_name == "<dict>"

    def test_event_types_property(self):
        c = Config.from_dict(_minimal_v04())
        assert len(c.event_types) == 3

    def test_common_fields_property(self):
        c = Config.from_dict(_minimal_v04())
        assert "event_name" in c.common_fields

    def test_generation_mode_property(self):
        c = Config.from_dict(_minimal_v04())
        assert c.generation_mode == "streaming"

    def test_scenario_property(self):
        c = Config.from_dict(_minimal_v04())
        assert c.scenario["duration_seconds"] == 300

    def test_get_dot_notation(self):
        c = Config.from_dict(_minimal_v04())
        assert c.get("scenario.duration_seconds") == 300

    def test_getitem(self):
        c = Config.from_dict(_minimal_v04())
        assert c["generation_mode"] == "streaming"

    def test_contains(self):
        c = Config.from_dict(_minimal_v04())
        assert "scenario" in c

    def test_load_config_from_dict_compat(self):
        c = load_config_from_dict(_minimal_v04())
        assert c.schema_version == "v0.4"


# -----------------------------------------------------------------------
# v0.3 backward compat (legacy)
# -----------------------------------------------------------------------

class TestV03Compat:
    def test_v03_batch_config_passes(self):
        c = Config.from_dict({
            "generation_mode": "batch",
            "batch_config": {"total_rows": 1000, "partitions": 4},
            "sink_config": {"type": "delta", "partition_key_field": "event_key"},
            "event_types": [
                {"event_type_id": "a", "weight": 6,
                 "fields": {"x": {"type": "int", "range": [1, 10]}}},
                {"event_type_id": "b", "weight": 4,
                 "fields": {"x": {"type": "int", "range": [1, 10]}}},
            ],
        })
        assert c.schema_version == "v0.3"

    def test_v03_integer_weights_enforced(self):
        with pytest.raises(ConfigurationError, match="positive integers"):
            Config.from_dict({
                "generation_mode": "batch",
                "batch_config": {"total_rows": 100, "partitions": 2},
                "sink_config": {"type": "delta"},
                "event_types": [
                    {"event_type_id": "a", "weight": 0.5, "fields": {}},
                    {"event_type_id": "b", "weight": 0.5, "fields": {}},
                ],
            })
