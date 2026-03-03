# dblstreamgen - Databricks Sample Setup

Example configurations and notebooks for running `dblstreamgen` v0.4 in Databricks.

---

## Folder Structure

```
sample/
├── README.md                      # This file
├── UNITY_CATALOG_SETUP.md         # Detailed Unity Catalog setup
├── configs/
│   ├── simple_config.yaml         # Web analytics (streaming, Kinesis)
│   ├── faker_config.yaml          # Realistic data via Python Faker (batch)
│   ├── nested_types_config.yaml   # Structs, arrays, and maps (streaming)
│   ├── extended_types_config.yaml # Full type system reference (streaming)
│   └── 1500_events_config.yaml    # Scale test: 1500 event types
└── notebooks/
    └── 01_simple_example.py       # Complete end-to-end example
```

---

## Quick Start in Databricks

### Step 1: Create Unity Catalog Volumes

```sql
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libraries
  COMMENT 'Shared Python wheels and libraries';

CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.configs
  COMMENT 'Configuration files for dblstreamgen';
```

### Step 2: Upload the Wheel

Build the wheel (if not already built):
```bash
cd /path/to/dblstreamgen
python -m build
```

Upload to Unity Catalog:
```bash
databricks fs cp dist/dblstreamgen-0.4.0-py3-none-any.whl \
  dbfs:/Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.4.0-py3-none-any.whl
```

### Step 3: Upload Configuration

```bash
databricks fs cp sample/configs/simple_config.yaml \
  dbfs:/Volumes/<catalog>/<schema>/configs/simple_config.yaml
```

### Step 4: Configure AWS Credentials (for Kinesis)

Unity Catalog service credentials are recommended (automatic token refresh, no secrets):

```bash
# In Databricks UI: Catalog > External Locations > Credentials > Create
# Then reference the credential name in the sink factory options.
```

For direct key-based auth:
```bash
databricks secrets create-scope dblstreamgen
databricks secrets put-secret --scope dblstreamgen --key aws-access-key-id
databricks secrets put-secret --scope dblstreamgen --key aws-secret-access-key
```

### Step 5: Run the Example Notebook

1. Import `notebooks/01_simple_example.py` into Databricks
2. Update paths to your Unity Catalog volumes and credential names
3. Run all cells

---

## Configuration Format (v0.4)

The v0.4 config has three required top-level keys and two optional ones:

```yaml
# Required
generation_mode: "streaming"   # or "batch"

scenario:
  seed: 42
  duration_seconds: 300          # streaming only
  baseline_rows_per_second: 1000 # streaming only
  # total_rows: 10000            # batch only

# Optional: omit for wide-schema output (Delta / Parquet)
serialization:
  format: "json"                 # or "avro"
  partition_key_field: "event_key"

# Schema
common_fields:
  event_name:
    event_type_id: true          # populated from event_type_id values
  event_key:
    type: "string"
    values: ["user_1", "user_2"]

# Event type weights must be floats summing to 1.0
event_types:
  - event_type_id: "product_view"
    weight: 0.60
    fields:
      product_id:
        type: "int"
        range: [1, 10000]

  - event_type_id: "purchase"
    weight: 0.40
    fields:
      amount:
        type: "float"
        range: [10.0, 1000.0]
```

---

## Python API

```python
from dblstreamgen import Config, Scenario, KinesisDataSource
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

config = Config.from_yaml("/path/to/simple_config.yaml")
scenario = Scenario(spark, config)

# Build a DataFrame only
df = scenario.build()          # serialized (partition_key, data) if config has serialization
df = scenario.build(serialize=False)  # wide typed columns

# Run the full scenario (blocks for duration_seconds, then stops all queries)
spark.dataSource.register(KinesisDataSource)

def kinesis_sink(df, checkpoint_path):
    return (
        df.writeStream
        .format("dblstreamgen_kinesis")
        .option("stream_name", "my-stream")
        .option("region", "us-east-1")
        .option("service_credential", "my-credential")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 second")
        .start()
    )

result = scenario.run(kinesis_sink, "/tmp/checkpoints/scenario")
print(result)
```

---

## Notebooks

### `01_simple_example.py`

Complete end-to-end example:
1. Install dblstreamgen wheel
2. Load configuration via `Config.from_yaml()`
3. Preview execution plan with `scenario.plan()` and `scenario.dry_run()`
4. Register `KinesisDataSource` and define a `sink_factory`
5. Run the full scenario with `scenario.run()`
6. Read back from Kinesis and analyse event distribution

**Runtime**: Databricks Runtime 15.4 LTS or above

---

## Security Best Practices

Never commit credentials. Always use Databricks secrets:

```python
aws_key    = dbutils.secrets.get(scope="dblstreamgen", key="aws-access-key-id")
aws_secret = dbutils.secrets.get(scope="dblstreamgen", key="aws-secret-access-key")

def kinesis_sink(df, checkpoint_path):
    return (
        df.writeStream
        .format("dblstreamgen_kinesis")
        .option("stream_name", "my-stream")
        .option("region", "us-east-1")
        .option("aws_access_key_id", aws_key)
        .option("aws_secret_access_key", aws_secret)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )
```

---

## Common Use Cases

### E-commerce Platform
```yaml
generation_mode: "streaming"
scenario:
  duration_seconds: 300
  baseline_rows_per_second: 1000
serialization:
  format: "json"
  partition_key_field: "customer_id"
common_fields:
  customer_id: {type: "int", range: [100000, 999999]}
  session_id: {type: "uuid"}
event_types:
  - event_type_id: "product_view"
    weight: 0.50
    fields:
      product_id: {type: "int", range: [1, 10000]}
  - event_type_id: "add_to_cart"
    weight: 0.30
    fields:
      product_id: {type: "int", range: [1, 10000]}
      quantity: {type: "int", range: [1, 5]}
  - event_type_id: "purchase"
    weight: 0.20
    fields:
      order_id: {type: "uuid"}
      total_amount: {type: "float", range: [10.0, 1000.0]}
```

### IoT Sensors
```yaml
generation_mode: "streaming"
scenario:
  duration_seconds: 300
  baseline_rows_per_second: 5000
common_fields:
  device_id: {type: "int", range: [1, 1000]}
  event_timestamp:
    type: "timestamp"
    mode: "current"
    jitter_seconds: 5
event_types:
  - event_type_id: "temperature_reading"
    weight: 0.60
    fields:
      temperature_celsius: {type: "float", range: [-20.0, 50.0]}
  - event_type_id: "battery_status"
    weight: 0.40
    fields:
      battery_percent: {type: "int", range: [0, 100]}
```

### Gaming Platform
```yaml
generation_mode: "streaming"
scenario:
  duration_seconds: 300
  baseline_rows_per_second: 2000
common_fields:
  player_id: {type: "int", range: [1, 100000]}
  session_id: {type: "uuid"}
event_types:
  - event_type_id: "game_start"
    weight: 0.10
    fields:
      game_mode: {type: "string", values: ["solo", "duo", "squad"]}
  - event_type_id: "player_action"
    weight: 0.70
    fields:
      action_type: {type: "string", values: ["move", "shoot", "jump"]}
  - event_type_id: "game_end"
    weight: 0.20
    fields:
      final_score: {type: "int", range: [0, 10000]}
```

---

## Troubleshooting

### ModuleNotFoundError: No module named 'dblstreamgen'
```python
%pip install /Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.4.0-py3-none-any.whl
dbutils.library.restartPython()
```

### ConfigurationError: Total weights must sum to 1.0
```yaml
event_types:
  - event_type_id: "type_a"
    weight: 0.60
  - event_type_id: "type_b"
    weight: 0.40
# Total: 1.0
```

### Kinesis: Stream not found
```bash
aws kinesis create-stream --stream-name my-events-stream --shard-count 2
```

---

## Additional Resources

- **Quick Start Guide**: [QUICKSTART.md](../QUICKSTART.md)
- **Build Guide**: [BUILD.md](../BUILD.md)
- **Unity Catalog Setup**: [UNITY_CATALOG_SETUP.md](./UNITY_CATALOG_SETUP.md)
- **GitHub Issues**: https://github.com/matthewmoorcroft/dblstreamgen/issues

---

**Version**: v0.4
