# Changelog

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
