# dblstreamgen YAML Configuration Reference

**Version:** 0.3.0  
**Purpose:** Complete specification for building dblstreamgen YAML configs. Suitable for humans and LLMs.

---

## Top-Level Structure

Every config must contain these root-level keys:

```yaml
generation_mode: "batch"          # REQUIRED: "streaming" or "batch"
streaming_config:                  # REQUIRED if generation_mode = "streaming"
  total_rows_per_second: 1000
batch_config:                      # REQUIRED if generation_mode = "batch"
  total_rows: 10000
  partitions: 8                    # optional, default 8
sink_config:                       # REQUIRED
  type: "delta"                    # REQUIRED: "kinesis", "kafka", "eventhubs", "delta"
  partition_key_field: "event_name"  # REQUIRED for serialize=True output
  # Additional sink-specific keys (stream_name, region, etc.) as needed
common_fields: {}                  # optional: fields on every row
event_types: []                    # REQUIRED: at least one event type
derived_fields: {}                 # optional: computed columns referencing other fields
```

---

## `event_types` (required)

A list of event type objects. At least one is required.

```yaml
event_types:
  - event_type_id: "order.placed"   # REQUIRED: unique string identifier
    weight: 3                        # REQUIRED: positive integer (relative proportion)
    fields:                          # optional: event-specific fields
      field_name:
        <field_spec>                 # see Field Specification below
```

### Rules
- `event_type_id` must be unique across all event types.
- `weight` must be a **positive integer** (not a float). Weights are relative: `[6, 3, 1]` means 60%/30%/10%.
- Fields with the **same name** across event types must have the **same type** (and same nested structure for struct/array/map).

---

## `common_fields` (optional)

Fields that appear on every row regardless of event type.

```yaml
common_fields:
  event_name:
    event_type_id: true   # populates with event_type_id values (always string)
  event_id:
    type: "uuid"
  event_timestamp:
    type: "timestamp"
```

Same field specification rules as event type fields (see below). Additionally, `common_fields` supports `event_type_id: true` to create a visible column that maps to the internal event type discriminator. No `type` is needed -- it's always string.

---

## `derived_fields` (optional)

Computed columns that reference other generated columns. Added last in the pipeline.

```yaml
derived_fields:
  full_name:
    type: "string"                         # REQUIRED
    expr: "concat(first_name, ' ', last_name)"  # REQUIRED: Spark SQL expression
    base_columns: ["first_name", "last_name"]    # optional: dependency hints
    percent_nulls: 0.05                    # optional
```

### Rules
- `expr` and `type` are both required.
- `base_columns` is optional but recommended for column dependency ordering.
- Cannot use `faker`, `values`, or `range` -- only `expr`.

---

## Field Specification

Every field (in `common_fields`, `event_types[].fields`, or nested `struct.fields`) follows this spec.

### Required Property

| Property | Type | Description |
|----------|------|-------------|
| `type` | string | **Required.** One of the supported types below. |

### Value Source Properties (mutually exclusive -- pick ONE)

| Property | Type | Applies to | Description |
|----------|------|------------|-------------|
| `values` | list | string, boolean | Fixed list of possible values. |
| `range` | `[min, max]` | int, long, short, byte, float, double, decimal, date | Numeric or date range. |
| `expr` | string | any | Raw Spark SQL expression (bypasses all generation). |
| `faker` | string | **string only** | Python Faker method name (e.g. `"name"`, `"email"`, `"city"`). |
| `event_type_id` | boolean | **common_fields only** | When `true`, populates the column with `event_type_id` values from `event_types`. Type is always string (no `type` needed). |
| *(none)* | | uuid, timestamp, binary | Default generation (no value source needed). |

If none of these are specified, type-specific defaults apply (e.g., uuid generates `uuid()`, timestamp generates `current_timestamp()`).

### Optional Properties (combinable with any value source)

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `weights` | list[int] | equal | Probability weights for `values`. Same length as `values`. Positive integers. |
| `percent_nulls` | float | 0 | Fraction of rows set to NULL. Range: `0 < x < 1`. |
| `outliers` | list[object] | `[]` | Outlier injection rules (see below). |
| `faker_args` | dict | `{}` | Keyword arguments forwarded to the Faker method. Only valid with `faker`. |

### Outlier Objects

```yaml
outliers:
  - percent: 0.05         # fraction of rows (0 < x < 1)
    expr: "'BAD_VALUE'"    # Spark SQL expression for the outlier value
  - percent: 0.02
    expr: "NULL"
```

Total outlier percentages are additive. Applied before `percent_nulls`.

---

## Supported Types

### Simple Types

| Type | Value Source Options | Properties | Default Output |
|------|---------------------|------------|----------------|
| `uuid` | *(none needed)* | -- | Random UUID string |
| `string` | `values`, `faker`, `expr` | `weights` (with `values`) | -- |
| `int` | `range`, `expr` | -- | `range: [0, 100]` |
| `long` | `range`, `expr` | -- | `range: [0, 9223372036854775807]` |
| `short` | `range`, `expr` | -- | `range: [0, 32767]` |
| `byte` | `range`, `expr` | -- | `range: [0, 127]` |
| `float` | `range`, `expr` | -- | `range: [0.0, 100.0]` |
| `double` | `range`, `expr` | -- | `range: [0.0, 100.0]` |
| `decimal` | `range`, `expr` | `precision` (default 10), `scale` (default 2) | `range: [0, 99999999.99]` |
| `boolean` | `values`, `expr` | `weights` (with `values`) | 50/50 true/false |
| `timestamp` | `expr` | `begin`, `end` (datetime strings) | `current_timestamp()` |
| `date` | `range`, `expr` | `begin`, `end` (date strings) | `begin: "2020-01-01", end: "2024-12-31"` |
| `binary` | `expr` | -- | Random 16-byte binary |

### Complex Types

#### `array`

```yaml
field_name:
  type: "array"
  item_type: "string"              # REQUIRED: element type (any simple type)
  values: ["a", "b", "c"]          # optional: possible element values
  num_features: [1, 5]             # optional: [min_length, max_length] or fixed int
  weights: [3, 2, 1]              # optional: element selection weights
  range: [0, 100]                  # optional: for numeric item_type
```

#### `struct`

```yaml
field_name:
  type: "struct"
  fields:                          # REQUIRED: nested field specifications
    sub_field_1:
      type: "string"
      faker: "city"
    sub_field_2:
      type: "int"
      range: [0, 100]
```

Structs can be nested to arbitrary depth. Each sub-field follows the same field specification.

#### `map`

```yaml
field_name:
  type: "map"
  key_type: "string"               # optional, default "string"
  value_type: "string"             # optional, default "string"
  values:                          # list of possible map values
    - {"key1": "val1", "key2": "val2"}
    - {"key1": "val3", "key2": "val4"}
  weights: [7, 3]                  # optional: selection weights
```

---

## Faker Integration (v0.3.0)

Use Python Faker to generate realistic data. Requires `pip install faker` (pre-installed on Databricks Runtime 13.3+).

### Syntax

```yaml
field_name:
  type: "string"               # MUST be "string"
  faker: "<method_name>"       # Python Faker method (e.g. "name", "email", "city")
  faker_args:                  # optional: kwargs passed to the Faker method
    nb_words: 8
```

### Rules
- `type` must be `"string"`.
- `faker` is mutually exclusive with `values`, `range`, and `expr`.
- `faker_args` must be a dict (if present). Keys are the Faker method's keyword argument names.
- Can be combined with `percent_nulls` and `outliers`.
- Works in `common_fields`, `event_types[].fields`, and `struct` sub-fields at any depth.

### Common Methods

| Method | Output | faker_args |
|--------|--------|------------|
| `name` | "John Smith" | -- |
| `first_name` | "Alice" | -- |
| `last_name` | "Williams" | -- |
| `email` | "jsmith@example.com" | -- |
| `ascii_company_email` | "alice@acme.com" | -- |
| `street_address` | "123 Main St" | -- |
| `city` | "New York" | -- |
| `zipcode` | "10001" | -- |
| `phone_number` | "(555) 123-4567" | -- |
| `sentence` | "The quick brown fox..." | `nb_words` (int) |
| `paragraph` | Multi-sentence text | `nb_sentences` (int) |
| `text` | Block of text | `max_nb_chars` (int) |
| `user_name` | "alice_42" | -- |
| `url` | "https://example.com" | -- |
| `company` | "Acme Corp" | -- |
| `credit_card_number` | "4111111111111111" | -- |

Full list: https://faker.readthedocs.io/en/master/providers.html

---

## Validation Rules Summary

These rules are enforced by the config validator:

1. **Required keys:** `generation_mode`, `event_types`, `sink_config`.
2. **Mode config:** `streaming_config` required when mode is `"streaming"`; `batch_config` required when mode is `"batch"`.
3. **Sink type:** `sink_config.type` is required.
4. **Event types:** At least one. Each needs `event_type_id` (unique) and `weight` (positive integer).
5. **Field weights:** Must be positive integers (e.g., `[6, 3, 1]`), not floats.
6. **Field types:** Must be one of the 13 supported types.
7. **Type consistency:** Same field name across event types must have the same type, array item_type, map key/value types, and struct field structure.
8. **Faker rules:** `type` must be `"string"`, mutually exclusive with `values`/`range`/`expr`/`event_type_id`, method must be a non-empty string, `faker_args` must be a dict.
9. **`event_type_id` flag:** Only valid in `common_fields`, mutually exclusive with `values`/`range`/`expr`/`faker`. Always produces a string column (`type` is optional/ignored).
10. **Reserved names:** Field names `__dsg_id`, `__dsg_event_type_id`, and names starting with `__dsg_faker_` or `__dsg_rand_` are reserved for internal use and rejected.
11. **Outliers:** Each must have `percent` (0 < x < 1) and `expr` (string).
10. **percent_nulls:** Must be 0 <= x < 1.
11. **Derived fields:** Must have `expr` and `type`.

---

## Complete Minimal Example

```yaml
generation_mode: "batch"
batch_config:
  total_rows: 10000
  partitions: 8
sink_config:
  type: "delta"
  partition_key_field: "event_name"

common_fields:
  event_name:
    event_type_id: true
  event_id:
    type: "uuid"
  event_timestamp:
    type: "timestamp"

event_types:
  - event_type_id: "order.placed"
    weight: 7
    fields:
      amount:
        type: "double"
        range: [10.0, 500.0]
      customer_name:
        type: "string"
        faker: "name"

  - event_type_id: "order.cancelled"
    weight: 3
    fields:
      amount:
        type: "double"
        range: [10.0, 500.0]
      reason:
        type: "string"
        values: ["changed_mind", "found_cheaper", "too_slow", "other"]
        weights: [4, 3, 2, 1]
```

---

## Complete Complex Example

```yaml
generation_mode: "batch"
batch_config:
  total_rows: 50000
  partitions: 8
sink_config:
  type: "delta"
  partition_key_field: "event_name"

common_fields:
  event_name:
    event_type_id: true
  event_id:
    type: "uuid"
  event_timestamp:
    type: "timestamp"
  user_id:
    type: "long"
    range: [1000000, 999999999]
  customer_name:
    type: "string"
    faker: "name"
  contact_email:
    type: "string"
    faker: "ascii_company_email"
    percent_nulls: 0.1

event_types:
  - event_type_id: "customer.signup"
    weight: 4
    fields:
      address:
        type: "struct"
        fields:
          street:
            type: "string"
            faker: "street_address"
          city:
            type: "string"
            faker: "city"
          zip_code:
            type: "string"
            faker: "zipcode"
      referral_source:
        type: "string"
        values: ["organic", "paid", "referral", "social"]
        weights: [4, 3, 2, 1]

  - event_type_id: "order.placed"
    weight: 3
    fields:
      amount:
        type: "decimal"
        precision: 10
        scale: 2
        range: [5.00, 2000.00]
        outliers:
          - percent: 0.02
            expr: "CAST(99999.99 AS DECIMAL(10,2))"
      items:
        type: "array"
        item_type: "string"
        values: ["widget_a", "widget_b", "gadget_c", "tool_d"]
        num_features: [1, 5]

  - event_type_id: "support.ticket"
    weight: 3
    fields:
      subject:
        type: "string"
        faker: "sentence"
        faker_args:
          nb_words: 6
      priority:
        type: "string"
        values: ["low", "medium", "high", "critical"]
        weights: [4, 4, 2, 1]

derived_fields:
  event_date:
    type: "date"
    expr: "to_date(event_timestamp)"
    base_columns: ["event_timestamp"]
```

---

## Sample Configs

| File | Description |
|------|-------------|
| `sample/configs/simple_config.yaml` | Basic web analytics with Kinesis sink |
| `sample/configs/extended_types_config.yaml` | All 13 simple types |
| `sample/configs/nested_types_config.yaml` | Arrays, structs, maps with Faker |
| `sample/configs/faker_config.yaml` | Faker integration examples |
| `sample/configs/1500_events_config.yaml` | 1500+ event types at scale |
