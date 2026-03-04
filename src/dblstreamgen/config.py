"""Configuration management for dblstreamgen.

Supports two schema versions:
- v0.4: ``scenario`` section, float weights summing to 1.0
- v0.3 (legacy): ``streaming_config``/``batch_config``/``sink_config``, integer weights
"""

import logging
import re
from pathlib import Path
from typing import Any, Optional, cast

import yaml

logger = logging.getLogger(__name__)

_EVENT_TYPE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9._\-]+$")
_WEIGHT_SUM_TOLERANCE = 1e-6

SUPPORTED_TYPES = frozenset(
    {
        "uuid",
        "int",
        "float",
        "string",
        "timestamp",
        "boolean",
        "long",
        "double",
        "date",
        "decimal",
        "byte",
        "short",
        "binary",
        "array",
        "struct",
        "map",
    }
)


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""

    pass


class Config:
    """Validated configuration.  Immutable after construction.

    Detects v0.3 (legacy) vs v0.4 schema automatically and validates
    accordingly.  The v0.3 code-path keeps ``StreamOrchestrator`` working
    until the deprecation shim is removed in a later release.
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, data: dict, *, source_name: str = "<dict>"):
        self.data = data
        self._source_name = source_name
        self._schema_version: str = self._detect_schema_version()
        self._validate()

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        """Load and validate configuration from a YAML file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        try:
            with open(p) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}")
        if not data:
            raise ConfigurationError(f"Empty configuration file: {path}")
        return cls(data, source_name=p.name)

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        """Create and validate configuration from a dictionary."""
        return cls(data, source_name="<dict>")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        """``'config.yaml'`` for files, ``'<dict>'`` for dicts."""
        return self._source_name

    @property
    def schema_version(self) -> str:
        return self._schema_version

    @property
    def event_types(self) -> list:
        return cast(list, self.data.get("event_types", []))

    @property
    def common_fields(self) -> dict:
        return cast(dict, self.data.get("common_fields", {}))

    @property
    def generation_mode(self) -> str:
        return cast(str, self.data.get("generation_mode", "streaming"))

    @property
    def scenario(self) -> dict:
        return cast(dict, self.data.get("scenario", {}))

    @property
    def serialization(self) -> Optional[dict]:
        return self.data.get("serialization")

    # ------------------------------------------------------------------
    # Dict-like access (backward compat)
    # ------------------------------------------------------------------

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot-notation support."""
        keys = key.split(".")
        value: Any = self.data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __contains__(self, key: str) -> bool:
        return key in self.data

    # ------------------------------------------------------------------
    # Version detection
    # ------------------------------------------------------------------

    def _detect_schema_version(self) -> str:
        if "scenario" in self.data:
            return "v0.4"
        if any(k in self.data for k in ("streaming_config", "batch_config", "sink_config")):
            return "v0.3"
        raise ConfigurationError(
            "Cannot determine config schema version.  "
            "v0.4 configs require a 'scenario' section.  "
            "v0.3 configs require 'streaming_config' or 'batch_config' plus 'sink_config'.  "
            "Example v0.4:\n"
            "  scenario:\n"
            "    duration_seconds: 300\n"
            "    baseline_rows_per_second: 1000\n"
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _validate(self) -> None:
        if self._schema_version == "v0.4":
            self._validate_v04()
        else:
            self._validate_v03()

    # ==================================================================
    # v0.4 validation
    # ==================================================================

    def _validate_v04(self) -> None:
        self._validate_v04_top_level()
        self._validate_v04_scenario()
        self._validate_v04_event_types()
        self._validate_v04_serialization()
        self._validate_v04_derived_in_common()
        self._validate_field_types()
        self._validate_field_options()
        self._validate_field_type_consistency()
        self._validate_faker_fields()
        self._validate_reserved_field_names()
        self._validate_event_type_id_fields()

    def _validate_v04_top_level(self) -> None:
        if "event_types" not in self.data:
            raise ConfigurationError(
                "Missing required section: 'event_types'.  "
                "Define at least one event type with fields and a weight."
            )
        if "generation_mode" not in self.data:
            raise ConfigurationError(
                "Missing required field: 'generation_mode'.  "
                "Must be 'streaming' or 'batch'."
            )
        mode = self.data["generation_mode"]
        if mode not in ("streaming", "batch"):
            raise ConfigurationError(
                f"generation_mode must be 'streaming' or 'batch', got '{mode}'."
            )
        if "sink_config" in self.data:
            raise ConfigurationError(
                "v0.4 configs do not use 'sink_config'.  "
                "Remove it -- the user manages sinks via sink_factory."
            )
        if "derived_fields" in self.data:
            raise ConfigurationError(
                "v0.4 configs do not use a top-level 'derived_fields' section.  "
                "Move derived fields into 'common_fields' with a 'base_columns' key.  "
                "Example:\n"
                "  common_fields:\n"
                "    is_high_value:\n"
                "      type: boolean\n"
                "      expr: \"amount > 200\"\n"
                "      base_columns: [\"amount\"]\n"
            )

    # -- scenario section -----------------------------------------------

    def _validate_v04_scenario(self) -> None:
        scenario = self.data.get("scenario")
        if not isinstance(scenario, dict):
            raise ConfigurationError(
                "Missing or invalid 'scenario' section.  "
                "Example:\n"
                "  scenario:\n"
                "    duration_seconds: 300\n"
                "    baseline_rows_per_second: 1000\n"
            )

        mode = self.data["generation_mode"]
        if mode == "streaming":
            for key in ("duration_seconds", "baseline_rows_per_second"):
                if key not in scenario:
                    raise ConfigurationError(
                        f"scenario.{key} is required for streaming mode.  "
                        f"Example: scenario:\n  {key}: {300 if 'duration' in key else 1000}"
                    )
                val = scenario[key]
                if not isinstance(val, (int, float)) or val <= 0:
                    raise ConfigurationError(
                        f"scenario.{key} must be a positive number, got {val!r}."
                    )
        elif mode == "batch":
            if "total_rows" not in scenario:
                raise ConfigurationError(
                    "scenario.total_rows is required for batch mode.  "
                    "Example: scenario:\n  total_rows: 100000"
                )
            val = scenario["total_rows"]
            if not isinstance(val, int) or val <= 0:
                raise ConfigurationError(
                    f"scenario.total_rows must be a positive integer, got {val!r}."
                )

        seed = scenario.get("seed", 42)
        if not isinstance(seed, int):
            raise ConfigurationError(
                f"scenario.seed must be an integer, got {type(seed).__name__}."
            )

        if "spike" in scenario:
            self._validate_v04_spike(scenario["spike"])
        if "ramp" in scenario:
            self._validate_v04_ramp(scenario["ramp"])

    def _validate_v04_spike(self, spike: Any) -> None:
        if not isinstance(spike, dict):
            raise ConfigurationError("scenario.spike must be a mapping.")

        for key in ("at_seconds", "for_seconds", "additional_rows_per_second"):
            if key not in spike:
                raise ConfigurationError(
                    f"scenario.spike.{key} is required.  "
                    f"Example: spike:\n  at_seconds: 60\n  for_seconds: 120\n  additional_rows_per_second: 5000"
                )
            val = spike[key]
            if not isinstance(val, (int, float)) or val <= 0:
                raise ConfigurationError(
                    f"scenario.spike.{key} must be a positive number, got {val!r}."
                )

        targets = spike.get("targets")
        if targets is not None:
            if not isinstance(targets, list) or len(targets) == 0:
                raise ConfigurationError(
                    "scenario.spike.targets must be a non-empty list of "
                    "{event_type_id, weight} mappings."
                )
            known_ids = {et["event_type_id"] for et in self.data.get("event_types", [])}
            target_weight_sum = 0.0
            for i, t in enumerate(targets):
                tid = t.get("event_type_id")
                if tid is None:
                    raise ConfigurationError(
                        f"scenario.spike.targets[{i}] missing 'event_type_id'."
                    )
                if tid not in known_ids:
                    raise ConfigurationError(
                        f"scenario.spike.targets[{i}].event_type_id '{tid}' "
                        f"does not exist in event_types.  Known IDs: {sorted(known_ids)}"
                    )
                w = t.get("weight")
                if w is None or not isinstance(w, (int, float)) or w < 0:
                    raise ConfigurationError(
                        f"scenario.spike.targets[{i}].weight must be a non-negative number, "
                        f"got {w!r}."
                    )
                target_weight_sum += float(w)
            if abs(target_weight_sum - 1.0) > _WEIGHT_SUM_TOLERANCE:
                raise ConfigurationError(
                    f"scenario.spike.targets weights must sum to 1.0 "
                    f"(tolerance {_WEIGHT_SUM_TOLERANCE}), got {target_weight_sum}.  "
                    f"Adjust the weights so they total exactly 1.0."
                )

    def _validate_v04_ramp(self, ramp: Any) -> None:
        if not isinstance(ramp, dict):
            raise ConfigurationError("scenario.ramp must be a mapping.")
        for key in ("step_seconds", "additional_rows_per_second"):
            if key not in ramp:
                raise ConfigurationError(
                    f"scenario.ramp.{key} is required.  "
                    f"Example: ramp:\n  step_seconds: 30\n  additional_rows_per_second: 1000"
                )
            val = ramp[key]
            if not isinstance(val, (int, float)) or val <= 0:
                raise ConfigurationError(
                    f"scenario.ramp.{key} must be a positive number, got {val!r}."
                )
        mx = ramp.get("max_rows_per_second")
        if mx is not None and (not isinstance(mx, (int, float)) or mx <= 0):
            raise ConfigurationError(
                f"scenario.ramp.max_rows_per_second must be a positive number, got {mx!r}."
            )

    # -- event types (v0.4) ---------------------------------------------

    def _validate_v04_event_types(self) -> None:
        event_types = self.data.get("event_types", [])
        if not event_types:
            raise ConfigurationError(
                "At least one event type must be defined in 'event_types'."
            )

        ids_seen: set = set()
        weight_sum = 0.0

        for i, et in enumerate(event_types):
            loc = f"event_types[{i}]"
            eid = et.get("event_type_id")
            if not eid:
                raise ConfigurationError(f"{loc} missing 'event_type_id'.")
            if not _EVENT_TYPE_ID_PATTERN.match(eid):
                raise ConfigurationError(
                    f"{loc}.event_type_id '{eid}' contains invalid characters.  "
                    f"Allowed: letters, digits, '.', '_', '-'.  "
                    f"Pattern: {_EVENT_TYPE_ID_PATTERN.pattern}"
                )
            if eid in ids_seen:
                raise ConfigurationError(
                    f"{loc}.event_type_id '{eid}' is duplicated.  IDs must be unique."
                )
            ids_seen.add(eid)

            w = et.get("weight")
            if w is None:
                raise ConfigurationError(
                    f"{loc} ('{eid}') missing 'weight'.  "
                    f"Provide a float weight (e.g., 0.60)."
                )
            if not isinstance(w, (int, float)):
                raise ConfigurationError(
                    f"{loc} ('{eid}') weight must be a number, got {type(w).__name__}."
                )
            if float(w) < 0:
                raise ConfigurationError(
                    f"{loc} ('{eid}') weight must be >= 0, got {w}.  "
                    f"Use weight: 0.0 to exclude a type from the baseline."
                )
            weight_sum += float(w)

        if abs(weight_sum - 1.0) > _WEIGHT_SUM_TOLERANCE:
            raise ConfigurationError(
                f"Event type weights must sum to 1.0 (tolerance {_WEIGHT_SUM_TOLERANCE}), "
                f"got {weight_sum}.  "
                f"Adjust the weights so they total exactly 1.0.  "
                f"Example: 3 types at 0.60, 0.30, 0.10."
            )

    # -- serialization (v0.4) -------------------------------------------

    def _validate_v04_serialization(self) -> None:
        ser = self.data.get("serialization")
        if ser is None:
            return
        if not isinstance(ser, dict):
            raise ConfigurationError("'serialization' must be a mapping.")
        fmt = ser.get("format")
        if fmt not in ("json", "avro"):
            raise ConfigurationError(
                f"serialization.format must be 'json' or 'avro', got {fmt!r}.  "
                f"Omit the serialization section entirely for wide typed output."
            )
        if "partition_key_field" not in ser:
            raise ConfigurationError(
                "serialization.partition_key_field is required when serialization is enabled.  "
                "Example: serialization:\n  format: json\n  partition_key_field: event_key"
            )

    # -- derived fields merged into common_fields -----------------------

    def _validate_v04_derived_in_common(self) -> None:
        """Validate fields in common_fields that have base_columns (derived fields)."""
        for name, spec in self.data.get("common_fields", {}).items():
            if "base_columns" not in spec:
                continue
            if "expr" not in spec:
                raise ConfigurationError(
                    f"common_fields.{name} has 'base_columns' but no 'expr'.  "
                    f"Derived fields must specify both 'expr' and 'base_columns'.  "
                    f"Example:\n"
                    f"  {name}:\n"
                    f"    type: boolean\n"
                    f"    expr: \"amount > 200\"\n"
                    f"    base_columns: [\"amount\"]"
                )
            if "type" not in spec:
                raise ConfigurationError(
                    f"common_fields.{name} has 'base_columns' but no 'type'.  "
                    f"Derived fields must specify 'type'."
                )

    # ==================================================================
    # v0.3 validation (legacy -- kept for StreamOrchestrator compat)
    # ==================================================================

    def _validate_v03(self) -> None:
        required_fields = ["event_types", "generation_mode", "sink_config"]
        for field in required_fields:
            if field not in self.data:
                raise ConfigurationError(f"Missing required field: {field}")

        event_types = self.data["event_types"]
        if not event_types:
            raise ConfigurationError("At least one event type must be defined")

        weights = [et.get("weight", 0) for et in event_types]
        if not all(isinstance(w, int) and w > 0 for w in weights):
            raise ConfigurationError(
                "Event type weights must be positive integers "
                "(e.g., [6, 3, 1] for 60%/30%/10%)"
            )

        event_ids = [et["event_type_id"] for et in event_types]
        if len(event_ids) != len(set(event_ids)):
            raise ConfigurationError("Event type IDs must be unique")

        common_fields = self.data.get("common_fields", {})
        for field_name, field_spec in common_fields.items():
            if "weights" in field_spec:
                ws = field_spec["weights"]
                if not all(isinstance(w, int) and w > 0 for w in ws):
                    raise ConfigurationError(
                        f"Weights for common_fields.{field_name} must be positive integers. "
                        f"Got: {ws}. Example: [2, 4, 4] instead of [0.2, 0.4, 0.4]"
                    )

        gen_mode = self.data["generation_mode"]
        if gen_mode not in ("streaming", "batch"):
            raise ConfigurationError(
                f"generation_mode must be 'streaming' or 'batch', got '{gen_mode}'"
            )
        if gen_mode == "streaming" and "streaming_config" not in self.data:
            raise ConfigurationError("streaming_config required for streaming mode")
        if gen_mode == "batch" and "batch_config" not in self.data:
            raise ConfigurationError("batch_config required for batch mode")

        sink_config = self.data["sink_config"]
        if "type" not in sink_config:
            raise ConfigurationError("sink_config.type is required")

        derived_fields = self.data.get("derived_fields", {})
        for field_name, field_spec in derived_fields.items():
            if "expr" not in field_spec:
                raise ConfigurationError(f"derived_fields.{field_name} must specify 'expr'")
            if "type" not in field_spec:
                raise ConfigurationError(f"derived_fields.{field_name} must specify 'type'")

        self._validate_field_types()
        self._validate_field_options()
        self._validate_field_type_consistency()
        self._validate_faker_fields()
        self._validate_reserved_field_names()
        self._validate_event_type_id_fields()

    # ==================================================================
    # Shared validation (works for both v0.3 and v0.4)
    # ==================================================================

    def _validate_field_types(self) -> None:
        for field_name, field_spec in self.data.get("common_fields", {}).items():
            if field_spec.get("event_type_id"):
                continue
            if "base_columns" in field_spec and "type" in field_spec:
                ft = field_spec["type"]
                if ft not in SUPPORTED_TYPES:
                    raise ConfigurationError(
                        f"Unsupported field type '{ft}' in common_fields.{field_name}. "
                        f"Supported: {sorted(SUPPORTED_TYPES)}"
                    )
                continue
            ft = field_spec.get("type")
            if ft not in SUPPORTED_TYPES:
                raise ConfigurationError(
                    f"Unsupported field type '{ft}' in common_fields.{field_name}. "
                    f"Supported: {sorted(SUPPORTED_TYPES)}"
                )

        for event_type in self.data.get("event_types", []):
            for field_name, field_spec in event_type.get("fields", {}).items():
                ft = field_spec.get("type")
                if ft not in SUPPORTED_TYPES:
                    raise ConfigurationError(
                        f"Unsupported field type '{ft}' in "
                        f"{event_type['event_type_id']}.fields.{field_name}. "
                        f"Supported: {sorted(SUPPORTED_TYPES)}"
                    )

    def _validate_field_options(self) -> None:
        for field_name, field_spec in self.data.get("common_fields", {}).items():
            self._validate_single_field_options(field_spec, f"common_fields.{field_name}")
        for event_type in self.data.get("event_types", []):
            et_id = event_type["event_type_id"]
            for field_name, field_spec in event_type.get("fields", {}).items():
                self._validate_single_field_options(field_spec, f"{et_id}.fields.{field_name}")
        for field_name, field_spec in self.data.get("derived_fields", {}).items():
            self._validate_single_field_options(field_spec, f"derived_fields.{field_name}")

    def _validate_single_field_options(self, field_spec: dict, location: str) -> None:
        outliers = field_spec.get("outliers", [])
        if not isinstance(outliers, list):
            raise ConfigurationError(f"{location}.outliers must be a list")
        for i, outlier in enumerate(outliers):
            if "percent" not in outlier:
                raise ConfigurationError(
                    f"{location}.outliers[{i}] missing required key 'percent'"
                )
            if "expr" not in outlier:
                raise ConfigurationError(
                    f"{location}.outliers[{i}] missing required key 'expr'"
                )
            pct = outlier["percent"]
            if not isinstance(pct, (int, float)) or pct <= 0 or pct >= 1:
                raise ConfigurationError(
                    f"{location}.outliers[{i}].percent must be between 0 and 1 (exclusive), "
                    f"got {pct}"
                )

        pn = field_spec.get("percent_nulls")
        if pn is not None and (not isinstance(pn, (int, float)) or pn < 0 or pn >= 1):
            raise ConfigurationError(
                f"{location}.percent_nulls must be between 0 and 1 (exclusive), got {pn}"
            )

        step = field_spec.get("step")
        if step is not None:
            ftype = field_spec.get("type", "")
            _NUMERIC = {"int", "long", "short", "byte", "float", "double", "decimal"}
            if ftype not in _NUMERIC:
                raise ConfigurationError(
                    f"{location}.step is only valid for numeric types "
                    f"(int/long/short/byte/float/double/decimal), got type='{ftype}'"
                )
            if not isinstance(step, (int, float)) or step <= 0:
                raise ConfigurationError(
                    f"{location}.step must be a positive number, got {step!r}"
                )

        if field_spec.get("type") == "struct":
            for sub_name, sub_spec in field_spec.get("fields", {}).items():
                self._validate_single_field_options(sub_spec, f"{location}.fields.{sub_name}")

    def _compare_field_specs(
        self, spec1: dict, spec2: dict, field_name: str, loc1: str, loc2: str
    ) -> None:
        type1 = spec1.get("type")
        type2 = spec2.get("type")
        if type1 != type2:
            raise ConfigurationError(
                f"Field '{field_name}' has inconsistent types: {loc1} uses '{type1}' "
                f"but {loc2} uses '{type2}'. Use different field names or standardize."
            )
        if type1 == "array" and spec1.get("item_type", "string") != spec2.get(
            "item_type", "string"
        ):
            raise ConfigurationError(
                f"Array field '{field_name}' has inconsistent item types between "
                f"{loc1} and {loc2}."
            )
        if type1 == "map" and (
            spec1.get("key_type", "string") != spec2.get("key_type", "string")
            or spec1.get("value_type", "string") != spec2.get("value_type", "string")
        ):
            raise ConfigurationError(
                f"Map field '{field_name}' has inconsistent key/value types between "
                f"{loc1} and {loc2}."
            )
        if type1 == "struct":
            fields1 = spec1.get("fields", {})
            fields2 = spec2.get("fields", {})
            keys1 = set(fields1.keys())
            keys2 = set(fields2.keys())
            if keys1 != keys2:
                raise ConfigurationError(
                    f"Struct field '{field_name}' has inconsistent nested fields "
                    f"between {loc1} and {loc2}."
                )
            for nested in keys1:
                self._compare_field_specs(
                    fields1[nested], fields2[nested], f"{field_name}.{nested}", loc1, loc2
                )

    def _validate_field_type_consistency(self) -> None:
        field_registry: dict = {}
        for field_name, field_spec in self.data.get("common_fields", {}).items():
            if field_spec.get("event_type_id"):
                continue
            field_registry[field_name] = (field_spec, "common_fields")
        for event_type in self.data.get("event_types", []):
            event_id = event_type["event_type_id"]
            for field_name, field_spec in event_type.get("fields", {}).items():
                if field_name in field_registry:
                    existing_spec, existing_location = field_registry[field_name]
                    self._compare_field_specs(
                        existing_spec, field_spec, field_name, existing_location, event_id
                    )
                else:
                    field_registry[field_name] = (field_spec, event_id)

    def _collect_struct_faker_specs(
        self, parent_spec: dict, parent_path: str, all_specs: list
    ) -> None:
        for sub_name, sub_spec in parent_spec.get("fields", {}).items():
            path = f"{parent_path}.{sub_name}"
            all_specs.append((path, sub_spec))
            if sub_spec.get("type") == "struct":
                self._collect_struct_faker_specs(sub_spec, path, all_specs)

    def _validate_faker_fields(self) -> None:
        all_specs: list = []
        for name, spec in self.data.get("common_fields", {}).items():
            if spec.get("event_type_id") or "base_columns" in spec:
                continue
            all_specs.append((f"common_fields.{name}", spec))
            if spec.get("type") == "struct":
                self._collect_struct_faker_specs(spec, f"common_fields.{name}", all_specs)
        for et in self.data.get("event_types", []):
            et_id = et["event_type_id"]
            for name, spec in et.get("fields", {}).items():
                all_specs.append((f"{et_id}.fields.{name}", spec))
                if spec.get("type") == "struct":
                    self._collect_struct_faker_specs(spec, f"{et_id}.fields.{name}", all_specs)

        for location, spec in all_specs:
            if "faker" not in spec:
                continue
            if spec.get("type") != "string":
                raise ConfigurationError(
                    f"{location}: 'faker' requires type 'string', got '{spec.get('type')}'"
                )
            conflicts = [k for k in ("values", "range", "expr") if k in spec]
            if conflicts:
                raise ConfigurationError(
                    f"{location}: 'faker' is mutually exclusive with {conflicts}"
                )
            if not isinstance(spec["faker"], str) or not spec["faker"]:
                raise ConfigurationError(
                    f"{location}: 'faker' must be a non-empty string "
                    "(Python Faker method name)"
                )
            faker_args = spec.get("faker_args")
            if faker_args is not None and not isinstance(faker_args, dict):
                raise ConfigurationError(
                    f"{location}: 'faker_args' must be a dict of kwargs for the Faker method"
                )

    def _validate_reserved_field_names(self) -> None:
        reserved = {"__dsg_id", "__dsg_event_type_id"}
        reserved_prefixes = ("__dsg_faker_", "__dsg_rand_", "__base_")

        def _check(name: str, location: str) -> None:
            if name in reserved or any(name.startswith(p) for p in reserved_prefixes):
                raise ConfigurationError(
                    f"{location}: '{name}' is a reserved internal column name.  "
                    f"Reserved names: {sorted(reserved)}, "
                    f"reserved prefixes: {list(reserved_prefixes)}"
                )

        for name in self.data.get("common_fields", {}):
            _check(name, f"common_fields.{name}")
        for et in self.data.get("event_types", []):
            et_id = et["event_type_id"]
            for name in et.get("fields", {}):
                _check(name, f"{et_id}.fields.{name}")
        for name in self.data.get("derived_fields", {}):
            _check(name, f"derived_fields.{name}")

    def _validate_event_type_id_fields(self) -> None:
        for name, spec in self.data.get("common_fields", {}).items():
            if not spec.get("event_type_id"):
                continue
            conflicts = [k for k in ("values", "range", "expr", "faker") if k in spec]
            if conflicts:
                raise ConfigurationError(
                    f"common_fields.{name}: 'event_type_id: true' is mutually exclusive "
                    f"with {conflicts}"
                )
        for et in self.data.get("event_types", []):
            et_id = et["event_type_id"]
            for name, spec in et.get("fields", {}).items():
                if spec.get("event_type_id"):
                    raise ConfigurationError(
                        f"{et_id}.fields.{name}: 'event_type_id: true' is only valid "
                        f"in common_fields"
                    )
        for name, spec in self.data.get("derived_fields", {}).items():
            if spec.get("event_type_id"):
                raise ConfigurationError(
                    f"derived_fields.{name}: 'event_type_id: true' is only valid "
                    f"in common_fields"
                )


# ------------------------------------------------------------------
# Module-level convenience functions (backward compat)
# ------------------------------------------------------------------


def load_config(config_path: str) -> Config:
    """Load configuration from YAML file (legacy wrapper for Config.from_yaml)."""
    return Config.from_yaml(config_path)


def load_config_from_dict(data: dict) -> Config:
    """Load configuration from dictionary (legacy wrapper for Config.from_dict)."""
    return Config.from_dict(data)
