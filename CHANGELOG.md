# Changelog

## v0.4.0

### Breaking Changes
- **Config schema v0.4**: `scenario` section replaces `streaming_config`/`batch_config`.
  Event type weights are now **floats summing to 1.0** (not positive integers).
  `sink_config` removed -- users manage sinks via `sink_factory`.
  `derived_fields` merged into `common_fields` (use `base_columns` key).
- **New public API**: `Scenario(spark, Config.from_yaml(...)).build()` replaces
  `StreamOrchestrator(spark, config).create_unified_stream()`.
- `pyspark` and `dbldatagen` moved to optional dependencies (`pip install dblstreamgen[spark]`).

### Added
- **Hybrid native generation**: Field values generated via dbldatagen's native parameters
  (`minValue`/`maxValue`, `begin`/`end`, `values`/`weights`, `percentNulls`) instead of
  hand-rolled SQL. CASE WHEN expressions do routing only.
- **Spec deduplication**: Conditional fields with identical specs across event types share
  a single hidden column, reducing compile time at scale.
- **Scenario runner**: `scenario.run(sink_factory, checkpoint_base)` manages baseline and
  spike streaming queries with `try/finally` cleanup.
- **Spike scenarios**: Burst additional load at a specific time offset with optional
  targeted event types and independent weights.
- **Serialization module**: `serialize_to_json` and `serialize_to_avro` with config-driven
  dispatch and `ignoreNullFields`.
- **Scenario facade**: `Scenario.build()`, `.dry_run()`, `.explain()`, `.plan()`, `.run()`.
- **Timestamp modes**: Historical random, historical linear, and current-time with jitter.
- **Derived field topological sort**: Multi-level derivation with cycle detection.
- **Zero-weight type exclusion**: Types with `weight: 0.0` excluded from baseline, available
  for spike activation.
- **GitHub Actions CI**: Lint, type-check, unit tests (Python 3.9-3.12), integration tests,
  coverage.

### Deprecated
- `StreamOrchestrator` -- use `Scenario` instead. Will be removed in v0.5.
- `load_config()` / `load_config_from_dict()` -- use `Config.from_yaml()` / `Config.from_dict()`.
  Legacy wrappers still work.

### Migration
```python
# Before (v0.3)
from dblstreamgen import StreamOrchestrator, load_config
config = load_config("config.yaml")
df = StreamOrchestrator(spark, config).create_unified_stream()

# After (v0.4)
from dblstreamgen import Config, Scenario
df = Scenario(spark, Config.from_yaml("config.yaml")).build()
```

## v0.3.0

### Added
- **Faker integration** -- Generate realistic data using Python Faker via dbldatagen's `fakerText()`.
  Use `faker: "method_name"` in YAML field specs, with optional `faker_args` for method kwargs.
  Works in common_fields, per-event-type fields, and struct sub-fields.
- **`event_type_id: true` flag** -- Expose the internal event type discriminator as a visible column
  in `common_fields`. Use any column name (e.g., `event_name`) and it maps to `event_type_id` values.
  Fixes serialized output routing via `partition_key_field`.
- Config validation for faker fields: type must be `string`, mutually exclusive with `values`/`range`/`expr`.
- Config validation rejecting reserved internal field names (`__dsg_id`, `__dsg_event_type_id`, `__dsg_faker_*`, `__dsg_rand_*`).
- Config validation for `event_type_id` flag: string-only, common_fields-only, mutually exclusive.
- Runtime import guard with actionable error message when `faker` package is missing.
- New sample config: `sample/configs/faker_config.yaml`
- Faker section in `docs/TYPE_SYSTEM.md`
- This changelog.

### Changed
- Updated `nested_types_config.yaml` to use faker for profile struct fields (first_name, last_name, email).
- Updated all sample configs to use `event_type_id: true` for `event_name` field.
- `faker` is an optional dependency (`pip install dblstreamgen[faker]`),
  pre-installed on Databricks Runtime 13.3+.

## v0.2.0

- Wide schema approach with conditional CASE WHEN expressions.
- Weighted distribution fix using hidden rand columns.
- Nested types: array, struct, map with recursive generation.
- Outlier injection and null injection for data quality testing.
- Derived fields referencing other generated columns.
- Event type weights as positive integers.

## v0.1.0

- Initial release with streaming/batch generation.
- Kinesis custom DataSource sink.
- Basic field types: uuid, string, int, float, timestamp, boolean.
