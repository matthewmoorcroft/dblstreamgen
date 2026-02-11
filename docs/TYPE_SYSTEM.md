# dblstreamgen Type System Reference

**Version:** 0.2.0  
**Last Updated:** February 2026

---

## Overview

dblstreamgen supports **13 data types** covering simple primitives, complex nested structures, and specialized types. This document provides comprehensive reference for all supported types with YAML configuration examples.

---

## Simple Types

### 1. String

**Description:** Text and categorical values

**YAML Configuration:**
```yaml
# Equal probability
field_name:
  type: string
  values: ["value1", "value2", "value3"]

# Weighted distribution
field_name:
  type: string
  values: ["iOS", "Android", "Web"]
  weights: [0.4, 0.4, 0.2]  # Must sum to 1.0
```

**Examples:**
```yaml
device_type:
  type: string
  values: ["iOS", "Android", "Web"]
  weights: [0.4, 0.4, 0.2]

page_url:
  type: string
  values: ["/home", "/products", "/cart", "/checkout"]

status:
  type: string
  values: ["SUCCESS", "FAILURE", "PENDING"]
```

---

### 2. UUID

**Description:** Universally unique identifier (generates random UUIDs)

**YAML Configuration:**
```yaml
field_name:
  type: uuid
```

**Examples:**
```yaml
event_id:
  type: uuid

session_id:
  type: uuid

user_guid:
  type: uuid
```

**Generated Value:** `"550e8400-e29b-41d4-a716-446655440000"`

---

### 3. Integer (int)

**Description:** 32-bit signed integer

**YAML Configuration:**
```yaml
field_name:
  type: int
  range: [min, max]  # Optional, defaults to [0, 100]
```

**Examples:**
```yaml
quantity:
  type: int
  range: [1, 100]

user_id:
  type: int
  range: [1, 1000000]

age:
  type: int
  range: [18, 99]
```

**Range:** -2,147,483,648 to 2,147,483,647

---

### 4. Long

**Description:** 64-bit signed integer (for large IDs, timestamps)

**YAML Configuration:**
```yaml
field_name:
  type: long
  range: [min, max]  # Optional, defaults to [0, 9223372036854775807]
```

**Examples:**
```yaml
transaction_id:
  type: long
  range: [1000000000, 9999999999]

account_number:
  type: long
  range: [100000000000, 999999999999]

unix_timestamp:
  type: long
  range: [1609459200, 1735689600]  # 2021-2025
```

**Range:** -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

---

### 5. Short

**Description:** 16-bit signed integer

**YAML Configuration:**
```yaml
field_name:
  type: short
  range: [min, max]  # Optional, defaults to [0, 32767]
```

**Examples:**
```yaml
port_number:
  type: short
  range: [1024, 65535]

item_count:
  type: short
  range: [1, 1000]
```

**Range:** -32,768 to 32,767

---

### 6. Byte

**Description:** 8-bit signed integer

**YAML Configuration:**
```yaml
field_name:
  type: byte
  range: [min, max]  # Optional, defaults to [0, 127]
```

**Examples:**
```yaml
status_code:
  type: byte
  range: [0, 100]

priority:
  type: byte
  range: [1, 10]
```

**Range:** -128 to 127

---

### 7. Float

**Description:** 32-bit floating point number

**YAML Configuration:**
```yaml
field_name:
  type: float
  range: [min, max]  # Optional, defaults to [0.0, 100.0]
```

**Examples:**
```yaml
price:
  type: float
  range: [0.99, 999.99]

temperature:
  type: float
  range: [-20.0, 40.0]

battery_voltage:
  type: float
  range: [3.0, 4.2]
```

---

### 8. Double

**Description:** 64-bit floating point number (higher precision than float)

**YAML Configuration:**
```yaml
field_name:
  type: double
  range: [min, max]  # Optional, defaults to [0.0, 100.0]
```

**Examples:**
```yaml
latitude:
  type: double
  range: [-90.0, 90.0]

longitude:
  type: double
  range: [-180.0, 180.0]

exchange_rate:
  type: double
  range: [0.5, 2.5]
```

**Use Case:** Geographic coordinates, scientific measurements, financial calculations

---

### 9. Decimal

**Description:** Arbitrary precision decimal (for exact monetary values)

**YAML Configuration:**
```yaml
field_name:
  type: decimal
  precision: 10  # Total digits (default: 10)
  scale: 2       # Decimal places (default: 2)
  range: [min, max]
```

**Examples:**
```yaml
amount:
  type: decimal
  precision: 10
  scale: 2
  range: [1.00, 999999.99]

tax_amount:
  type: decimal
  precision: 8
  scale: 2
  range: [0.00, 9999.99]

price:
  type: decimal
  precision: 6
  scale: 2
  range: [0.99, 9999.99]
```

**Use Case:** Monetary values where exact precision is required

---

### 10. Boolean

**Description:** True/false values

**YAML Configuration:**
```yaml
# Equal probability (50/50)
field_name:
  type: boolean

# Weighted probability
field_name:
  type: boolean
  values: [true, false]
  weights: [0.8, 0.2]  # 80% true, 20% false
```

**Examples:**
```yaml
is_active:
  type: boolean
  values: [true, false]
  weights: [0.9, 0.1]  # 90% active

is_premium:
  type: boolean
  values: [true, false]
  weights: [0.15, 0.85]  # 15% premium users

email_verified:
  type: boolean
  values: [true, false]
  weights: [0.85, 0.15]
```

---

### 11. Timestamp

**Description:** Date and time with millisecond precision

**YAML Configuration:**
```yaml
# Current timestamp (default)
field_name:
  type: timestamp

# Timestamp within range
field_name:
  type: timestamp
  begin: "2024-01-01 00:00:00"
  end: "2024-12-31 23:59:59"
```

**Examples:**
```yaml
event_timestamp:
  type: timestamp

created_at:
  type: timestamp
  begin: "2024-01-01 00:00:00"
  end: "2024-12-31 23:59:59"

last_login:
  type: timestamp
  begin: "2024-06-01 00:00:00"
  end: "2024-06-30 23:59:59"
```

**Format:** `2026-02-04 10:30:45.123`

---

### 12. Date

**Description:** Date without time component

**YAML Configuration:**
```yaml
field_name:
  type: date
  begin: "2020-01-01"  # Optional, defaults to "2020-01-01"
  end: "2024-12-31"    # Optional, defaults to "2024-12-31"
```

**Examples:**
```yaml
birth_date:
  type: date
  begin: "1950-01-01"
  end: "2005-12-31"

registration_date:
  type: date
  begin: "2020-01-01"
  end: "2024-12-31"

expiration_date:
  type: date
  begin: "2025-01-01"
  end: "2030-12-31"
```

**Format:** `2024-06-15`

---

### 13. Binary

**Description:** Binary data (generates random bytes from UUID)

**YAML Configuration:**
```yaml
field_name:
  type: binary
```

**Examples:**
```yaml
raw_data:
  type: binary

thumbnail:
  type: binary

encrypted_payload:
  type: binary
```

**Generated Value:** Random 16-byte binary data

---

## Complex/Nested Types

### Array Type

**Description:** List of values with the same type

**YAML Configuration:**
```yaml
# Fixed length array
field_name:
  type: array
  item_type: string  # Type of array elements
  values: ["value1", "value2", "value3"]
  num_features: 3  # Fixed size: 3 elements

# Variable length array
field_name:
  type: array
  item_type: string
  values: ["value1", "value2", "value3"]
  num_features: [1, 5]  # Variable size: 1-5 elements
```

**Supported Item Types:** All simple types (string, int, long, float, double, boolean, etc.)

**Examples:**
```yaml
# Array of strings - tags
tags:
  type: array
  item_type: string
  values: ["urgent", "important", "normal", "low"]
  num_features: [1, 4]

# Array of integers - scores
scores:
  type: array
  item_type: int
  values: [1, 2, 3, 4, 5]
  num_features: [3, 10]

# Array of floats - measurements
measurements:
  type: array
  item_type: float
  range: [0.0, 100.0]
  num_features: [5, 5]  # Fixed 5 elements
```

**Generated Value:** `["urgent", "important", "normal"]`

---

### Struct Type

**Description:** Nested structure with named fields (can contain other structs, arrays, maps)

**YAML Configuration:**
```yaml
field_name:
  type: struct
  fields:
    sub_field1:
      type: string
      values: ["A", "B"]
    sub_field2:
      type: int
      range: [1, 100]
```

**Nested Structs:**
```yaml
user_profile:
  type: struct
  fields:
    name:
      type: struct  # Struct within struct
      fields:
        first:
          type: string
          values: ["Alice", "Bob"]
        last:
          type: string
          values: ["Smith", "Jones"]
    age:
      type: int
      range: [18, 75]
    contact:
      type: struct
      fields:
        email:
          type: string
          values: ["user@example.com"]
        phone:
          type: string
          values: ["555-0100", "555-0200"]
```

**Examples:**
```yaml
# Simple address struct
address:
  type: struct
  fields:
    street:
      type: string
      values: ["123 Main St", "456 Oak Ave"]
    city:
      type: string
      values: ["NYC", "LA", "Chicago"]
    zip:
      type: int
      range: [10000, 99999]

# Product pricing struct
pricing:
  type: struct
  fields:
    base_price:
      type: decimal
      precision: 10
      scale: 2
      range: [10.00, 999.99]
    currency:
      type: string
      values: ["USD", "EUR", "GBP"]
    discount_percent:
      type: int
      range: [0, 50]
```

**Generated Value:**
```json
{
  "street": "123 Main St",
  "city": "NYC",
  "zip": 10001
}
```

---

### Map Type

**Description:** Key-value pairs

**YAML Configuration:**
```yaml
field_name:
  type: map
  key_type: string    # Type of keys (default: string)
  value_type: string  # Type of values (default: string)
  values:  # List of dict options
    - {"key1": "value1", "key2": "value2"}
    - {"key3": "value3", "key4": "value4"}
  weights: [0.6, 0.4]  # Optional weights
```

**Examples:**
```yaml
# Simple metadata map
metadata:
  type: map
  key_type: string
  value_type: string
  values:
    - {"env": "prod", "region": "us-east"}
    - {"env": "dev", "region": "us-west"}

# Configuration map
config:
  type: map
  key_type: string
  value_type: string
  values:
    - {"theme": "dark", "lang": "en"}
    - {"theme": "light", "lang": "es"}
    - {"theme": "auto", "lang": "fr"}
  weights: [0.5, 0.3, 0.2]
```

**Generated Value:** `{"env": "prod", "region": "us-east"}`

---

## Type Comparison Matrix

| Type | Spark Type | Size | Range | Precision | Use Case |
|------|-----------|------|-------|-----------|----------|
| **byte** | tinyint | 8-bit | -128 to 127 | Integer | Status codes, flags |
| **short** | smallint | 16-bit | -32,768 to 32,767 | Integer | Ports, small counts |
| **int** | int | 32-bit | -2.1B to 2.1B | Integer | User IDs, counts |
| **long** | bigint | 64-bit | -9.2E18 to 9.2E18 | Integer | Transaction IDs, timestamps |
| **float** | float | 32-bit | ±3.4E38 | ~7 digits | Measurements, prices |
| **double** | double | 64-bit | ±1.7E308 | ~15 digits | Coordinates, rates |
| **decimal** | decimal | Variable | User-defined | Exact | Money, percentages |
| **boolean** | boolean | 1-bit | true/false | N/A | Flags, states |
| **string** | string | Variable | Unlimited | N/A | Text, categories |
| **uuid** | string | 36 chars | UUID v4 | N/A | Unique IDs |
| **date** | date | 32-bit | Date range | Day | Birth dates, events |
| **timestamp** | timestamp | 64-bit | Timestamp range | Millisecond | Event times |
| **binary** | binary | Variable | Unlimited | Byte | Raw data, images |

---

## Complete Configuration Examples

### Example 1: E-commerce Event with All Simple Types

```yaml
event_types:
  - event_type_id: "ecommerce.order"
    weight: 1.0
    fields:
      # String
      order_status:
        type: string
        values: ["pending", "processing", "shipped", "delivered"]
      
      # UUID
      order_id:
        type: uuid
      
      # Integer
      item_count:
        type: int
        range: [1, 50]
      
      # Long
      transaction_id:
        type: long
        range: [1000000000, 9999999999]
      
      # Short
      warehouse_id:
        type: short
        range: [1, 500]
      
      # Byte
      priority:
        type: byte
        range: [1, 10]
      
      # Float
      weight_kg:
        type: float
        range: [0.1, 50.0]
      
      # Double
      latitude:
        type: double
        range: [25.0, 48.0]
      
      # Decimal
      total_amount:
        type: decimal
        precision: 10
        scale: 2
        range: [10.00, 99999.99]
      
      # Boolean
      is_gift:
        type: boolean
        values: [true, false]
        weights: [0.2, 0.8]
      
      # Date
      estimated_delivery:
        type: date
        begin: "2024-01-01"
        end: "2024-12-31"
      
      # Timestamp
      order_timestamp:
        type: timestamp
        begin: "2024-01-01 00:00:00"
        end: "2024-12-31 23:59:59"
      
      # Binary
      receipt_image:
        type: binary
```

---

### Example 2: User Profile with Nested Structs

```yaml
common_fields:
  user_profile:
    type: struct
    fields:
      # Basic info
      user_id:
        type: long
        range: [1000000, 9999999]
      
      # Nested name struct
      name:
        type: struct
        fields:
          first:
            type: string
            values: ["Alice", "Bob", "Charlie", "Diana"]
          last:
            type: string
            values: ["Smith", "Johnson", "Williams"]
          middle_initial:
            type: string
            values: ["A", "B", "C", "D", ""]
      
      # Nested address struct
      address:
        type: struct
        fields:
          street:
            type: string
            values: ["123 Main St", "456 Oak Ave"]
          city:
            type: string
            values: ["NYC", "LA", "Chicago"]
          state:
            type: string
            values: ["NY", "CA", "IL"]
          zip:
            type: int
            range: [10000, 99999]
          # Nested coordinates struct
          coordinates:
            type: struct
            fields:
              lat:
                type: double
                range: [25.0, 48.0]
              lon:
                type: double
                range: [-125.0, -65.0]
      
      # Contact info struct
      contact:
        type: struct
        fields:
          email:
            type: string
            values: ["user@example.com", "test@demo.com"]
          phone:
            type: string
            values: ["555-0100", "555-0200"]
          verified:
            type: boolean
            values: [true, false]
            weights: [0.85, 0.15]
```

---

### Example 3: IoT Event with Arrays

```yaml
event_types:
  - event_type_id: "iot.telemetry"
    weight: 1.0
    fields:
      device_id:
        type: uuid
      
      # Array of temperature readings
      temperature_readings:
        type: array
        item_type: double
        range: [-40.0, 85.0]
        num_features: [5, 10]  # 5-10 readings per batch
      
      # Array of timestamps
      reading_times:
        type: array
        item_type: string
        values: ["00:00", "00:15", "00:30", "00:45"]
        num_features: [5, 10]
      
      # Array of sensor IDs
      sensor_ids:
        type: array
        item_type: int
        range: [1, 100]
        num_features: 5  # Fixed 5 sensors
```

---

### Example 4: Complex Order with Everything

```yaml
event_types:
  - event_type_id: "order.complex"
    weight: 1.0
    fields:
      order_id:
        type: long
        range: [1000000000, 9999999999]
      
      # Struct containing arrays
      line_items:
        type: struct
        fields:
          product_ids:
            type: array
            item_type: long
            range: [1000, 999999]
            num_features: [1, 10]
          quantities:
            type: array
            item_type: int
            range: [1, 5]
            num_features: [1, 10]
          prices:
            type: array
            item_type: decimal
            precision: 10
            scale: 2
            range: [1.00, 999.99]
            num_features: [1, 10]
      
      # Nested struct - shipping
      shipping:
        type: struct
        fields:
          address:
            type: struct
            fields:
              street:
                type: string
                values: ["123 Main", "456 Oak"]
              city:
                type: string
                values: ["NYC", "LA"]
              zip:
                type: int
                range: [10000, 99999]
          method:
            type: string
            values: ["standard", "express", "overnight"]
          tracking:
            type: array
            item_type: string
            values: ["TRK001", "TRK002", "TRK003"]
            num_features: [1, 2]
      
      # Map - custom attributes
      attributes:
        type: map
        key_type: string
        value_type: string
        values:
          - {"gift_wrap": "true", "message": "Happy Birthday"}
          - {"priority": "high", "notes": "Fragile"}
```

---

## Type Conversion Matrix

### Serialization Behavior

When `serialize=True` (for Kinesis/Kafka):

| Type | JSON Serialization |
|------|-------------------|
| **Simple types** | JSON primitives (number, string, boolean) |
| **uuid** | String |
| **date** | ISO 8601 string: "2024-06-15" |
| **timestamp** | ISO 8601 string: "2024-06-15T10:30:45.123Z" |
| **binary** | Base64 encoded string |
| **array** | JSON array: `[1, 2, 3]` |
| **struct** | JSON object: `{"field1": "value1", "field2": 123}` |
| **map** | JSON object: `{"key1": "val1", "key2": "val2"}` |
| **decimal** | Number or string (depending on precision) |

When `serialize=False` (for Delta/Parquet):
- All types retain their native Spark types
- Optimal columnar storage
- Type-safe queries

---

## Best Practices

### Choosing Numeric Types

```yaml
# Use int for most counts and IDs
user_count:
  type: int
  range: [0, 1000000]

# Use long for very large IDs or timestamps
transaction_id:
  type: long
  range: [1000000000, 9999999999]

# Use float for measurements (single precision OK)
temperature:
  type: float
  range: [-40.0, 85.0]

# Use double for coordinates (need precision)
latitude:
  type: double
  range: [-90.0, 90.0]

# Use decimal for money (exact precision required)
price:
  type: decimal
  precision: 10
  scale: 2
  range: [0.01, 99999.99]
```

### Nesting Depth Recommendations

- **Level 1-2**: Optimal performance, easy to query
- **Level 3-4**: Good for complex domain models
- **Level 5+**: Use sparingly, impacts serialization performance

### Array Size Recommendations

```yaml
# Small arrays (1-5 elements) - Common, efficient
tags:
  num_features: [1, 5]

# Medium arrays (5-20 elements) - Good for most use cases
product_images:
  num_features: [5, 15]

# Large arrays (20+ elements) - Use carefully
sensor_readings:
  num_features: [50, 100]  # Can impact memory
```

---

## Type Consistency Rules

### Same Field Name = Same Type

Fields with the same name across event types MUST have consistent types:

```yaml
# CORRECT
event_types:
  - event_type_id: "event_a"
    fields:
      user_id:
        type: long
        range: [1, 1000000]
  
  - event_type_id: "event_b"
    fields:
      user_id:
        type: long  # Same type
        range: [1, 1000000]
```

```yaml
# WRONG - Will raise ConfigurationError
event_types:
  - event_type_id: "event_a"
    fields:
      user_id:
        type: long
  
  - event_type_id: "event_b"
    fields:
      user_id:
        type: int  # Different type!
```

### Array Item Type Consistency

```yaml
# CORRECT
event_types:
  - event_type_id: "event_a"
    fields:
      tags:
        type: array
        item_type: string
  
  - event_type_id: "event_b"
    fields:
      tags:
        type: array
        item_type: string  # Same item_type
```

### Struct Field Consistency

```yaml
# CORRECT - Struct fields must match
event_types:
  - event_type_id: "event_a"
    fields:
      address:
        type: struct
        fields:
          city: {type: string}
          zip: {type: int}
  
  - event_type_id: "event_b"
    fields:
      address:
        type: struct
        fields:
          city: {type: string}  # Same fields, same types
          zip: {type: int}
```

---

## Migration from v0.1.0

### Backward Compatibility

All v0.1.0 configs work unchanged:
- `uuid`, `int`, `float`, `string`, `timestamp` - Same behavior
- No breaking changes

### Adding New Types

Simply update your YAML config:

```yaml
# Before (v0.1.0)
user_id:
  type: int
  range: [1, 1000000]

# After (v0.2.0) - Use long for larger range
user_id:
  type: long
  range: [1, 9999999999]
```

---

## Performance Considerations

### Type Impact on Performance

| Type Category | Generation Speed | Storage Size | Query Speed |
|--------------|-----------------|--------------|-------------|
| **Simple types** | Fast | Small | Fast |
| **Arrays (small)** | Fast | Medium | Fast |
| **Arrays (large)** | Medium | Large | Medium |
| **Structs (2 levels)** | Fast | Medium | Fast |
| **Structs (5+ levels)** | Medium | Large | Medium |
| **Maps** | Fast | Medium | Medium |

### Recommendations

1. **Use appropriate precision**: Don't use `double` if `float` suffices
2. **Limit array sizes**: Keep `num_features` under 20 for best performance
3. **Limit nesting depth**: 2-3 levels is optimal for most use cases
4. **Use structs over maps**: Structs are more type-safe and efficient

---

## Examples Gallery

See complete working examples:
- [extended_types_config.yaml](../sample/configs/extended_types_config.yaml) - All simple types
- [nested_types_config.yaml](../sample/configs/nested_types_config.yaml) - Complex nested types
- [simple_config.yaml](../sample/configs/simple_config.yaml) - Basic types (v0.1.0 compatible)

---

## Reference Documentation

- **dbldatagen Documentation**: https://databrickslabs.github.io/dbldatagen/
- **Spark SQL Data Types**: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
- **dblstreamgen README**: [README.md](../README.md)

---

**Last Updated:** February 2026 - v0.2.0
