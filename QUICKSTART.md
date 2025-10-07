# dblstreamgen - Quick Start Guide

**Version**: v0.1.0  
**Target**: Databricks Runtime 15.4 LTS or above

Get up and running with `dblstreamgen` in minutes!

---

## Overview

`dblstreamgen` is a Spark/Databricks library for generating synthetic streaming data using `dbldatagen`. It supports multiple event types, weighted rate distribution, and streaming to various sinks (Kinesis, Kafka, Event Hubs, Delta).

**Key Features**:
- 🎯 **Config-driven** - Define schemas in YAML
- ⚡ **dbldatagen-powered** - Leverages Spark for scale
- 🔀 **Multiple event types** - Union strategy for 10K+ types
- 📤 **Flexible sinks** - Kinesis, Kafka, Event Hubs, Delta (planned)
- 🎲 **Use-case agnostic** - Works for any domain

---

## Installation

### Option 1: Databricks (Recommended)

#### Step 1: Upload Wheel to Unity Catalog Volume

```python
# In a Databricks notebook
# Create a volume if needed
%sql
CREATE VOLUME IF NOT EXISTS catalog.schema.libraries;

# Upload the wheel using Databricks UI or CLI
# File location: /Volumes/catalog/schema/libraries/dblstreamgen-0.1.0-py3-none-any.whl
```

#### Step 2: Install in Notebook

```python
# Install the library
%pip install /Volumes/catalog/schema/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Restart Python to load the library
dbutils.library.restartPython()
```

#### Step 3: Verify Installation

```python
import dblstreamgen

print(f"✅ dblstreamgen v{dblstreamgen.__version__} installed successfully!")
```

### Option 2: Local Development (Limited)

**Note**: v0.1.0 requires PySpark and is designed for Databricks. Local use is limited.

```bash
# Install from wheel
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl

# Or install in development mode
pip install -e .
```

---

## Quick Start (5 Minutes)

### Step 1: Create Configuration

Create a config file or use the example:

```yaml
# my_config.yaml
common_fields:
  user_id:
    type: "int"
    range: [1, 10000]
  session_id:
    type: "uuid"

event_types:
  - event_type_id: "page_view"
    weight: 0.6  # 60% of events
    fields:
      page_url:
        type: "string"
        values: ["/home", "/products", "/cart", "/checkout"]
        weights: [0.4, 0.3, 0.2, 0.1]
      duration_seconds:
        type: "int"
        range: [1, 300]
  
  - event_type_id: "purchase"
    weight: 0.3  # 30% of events
    fields:
      product_id:
        type: "int"
        range: [1, 1000]
      amount:
        type: "float"
        range: [10.0, 500.0]
  
  - event_type_id: "user_signup"
    weight: 0.1  # 10% of events
    fields:
      email_domain:
        type: "string"
        values: ["gmail.com", "yahoo.com", "outlook.com"]

generation_mode: "streaming"

streaming_config:
  total_rows_per_second: 1000

batch_config:
  total_rows: 100000
  partitions: 8

serialization_format: "json"

sink_config:
  type: "kinesis"
  stream_name: "my-test-stream"
  region: "us-east-1"
  partition_key_field: "user_id"
  auto_shard_calculation: true
  max_parallel_requests: 10
  aws_access_key_id: "{{secrets/my-scope/aws-key}}"
  aws_secret_access_key: "{{secrets/my-scope/aws-secret}}"
```

Upload to Unity Catalog volume:
```bash
# Upload via Databricks UI or CLI to:
# /Volumes/catalog/schema/configs/my_config.yaml
```

### Step 2: Generate and Stream Data

```python
from pyspark.sql import SparkSession
import dblstreamgen

# Get Spark session
spark = SparkSession.getActiveSession()

# Load configuration
config = dblstreamgen.load_config("/Volumes/catalog/schema/configs/my_config.yaml")

# Create orchestrator
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)

# Generate unified stream
unified_stream = orchestrator.create_unified_stream()

# Display the stream schema
unified_stream.printSchema()

# Preview the data (for testing)
display(unified_stream)
```

### Step 3: Write to Kinesis

```python
# Register Kinesis DataSource
spark.dataSource.register(dblstreamgen.KinesisDataSource)

# Get AWS credentials from Databricks secrets
aws_key = dbutils.secrets.get("my-scope", "aws-key")
aws_secret = dbutils.secrets.get("my-scope", "aws-secret")

# Write stream to Kinesis
query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "my-test-stream") \
    .option("region", "us-east-1") \
    .option("partition_key_field", "user_id") \
    .option("aws_access_key_id", aws_key) \
    .option("aws_secret_access_key", aws_secret) \
    .option("max_parallel_requests", 10) \
    .option("checkpointLocation", "/tmp/checkpoints/kinesis") \
    .start()

# Monitor the stream
query.status

# Stop after some time
# query.stop()
```

### Step 4: Verify Events in Kinesis

```bash
# Using AWS CLI
aws kinesis get-records \
  --shard-iterator $(aws kinesis get-shard-iterator \
    --stream-name my-test-stream \
    --shard-id shardId-000000000000 \
    --shard-iterator-type LATEST \
    --query 'ShardIterator' --output text) \
  --limit 10
```

---

## Configuration Guide

### Event Type Structure

```yaml
event_types:
  - event_type_id: "my_event"      # Unique identifier
    weight: 0.5                     # Proportion of total rate (must sum to 1.0)
    fields:                         # Event-specific fields
      field_name:
        type: "int"                 # Field type
        range: [1, 100]             # Type-specific config
```

### Field Types

| Type | Configuration | Example |
|------|--------------|---------|
| `uuid` | None | `session_id: {type: "uuid"}` |
| `int` | `range: [min, max]` | `user_id: {type: "int", range: [1, 10000]}` |
| `float` | `range: [min, max]` | `amount: {type: "float", range: [0.99, 99.99]}` |
| `string` | `values: [...]` or `values: [...], weights: [...]` | `status: {type: "string", values: ["active", "inactive"]}` |
| `timestamp` | None (uses current time) | `created_at: {type: "timestamp"}` |

### Common Fields

Fields shared across all event types:

```yaml
common_fields:
  user_id:
    type: "int"
    range: [1, 1000000]
  session_id:
    type: "uuid"
  region:
    type: "string"
    values: ["us-east-1", "us-west-2", "eu-west-1"]
    weights: [0.5, 0.3, 0.2]
```

### Weights Must Sum to 1.0

```yaml
event_types:
  - event_type_id: "type_a"
    weight: 0.6    # 60%
  - event_type_id: "type_b"
    weight: 0.3    # 30%
  - event_type_id: "type_c"
    weight: 0.1    # 10%
# Total: 1.0 ✅
```

---

## Common Use Cases

### E-commerce Events

```yaml
common_fields:
  customer_id: {type: "int", range: [100000, 999999]}
  session_id: {type: "uuid"}

event_types:
  - event_type_id: "product_view"
    weight: 0.6
    fields:
      product_id: {type: "int", range: [1, 10000]}
      category: {type: "string", values: ["electronics", "clothing", "books"]}
  
  - event_type_id: "add_to_cart"
    weight: 0.25
    fields:
      product_id: {type: "int", range: [1, 10000]}
      quantity: {type: "int", range: [1, 5]}
  
  - event_type_id: "purchase"
    weight: 0.15
    fields:
      order_id: {type: "uuid"}
      total_amount: {type: "float", range: [10.0, 1000.0]}
```

### IoT Sensor Data

```yaml
common_fields:
  device_id: {type: "int", range: [1, 1000]}
  timestamp: {type: "timestamp"}

event_types:
  - event_type_id: "temperature_reading"
    weight: 0.4
    fields:
      temperature_celsius: {type: "float", range: [-20.0, 50.0]}
      humidity: {type: "float", range: [0.0, 100.0]}
  
  - event_type_id: "motion_detected"
    weight: 0.3
    fields:
      confidence: {type: "float", range: [0.5, 1.0]}
  
  - event_type_id: "battery_status"
    weight: 0.3
    fields:
      battery_percent: {type: "int", range: [0, 100]}
```

### Gaming Events

```yaml
common_fields:
  player_id: {type: "int", range: [1, 100000]}
  session_id: {type: "uuid"}

event_types:
  - event_type_id: "game_start"
    weight: 0.1
    fields:
      game_mode: {type: "string", values: ["solo", "duo", "squad"]}
  
  - event_type_id: "player_action"
    weight: 0.7
    fields:
      action_type: {type: "string", values: ["move", "shoot", "jump", "crouch"]}
      x_coordinate: {type: "float", range: [0.0, 1000.0]}
      y_coordinate: {type: "float", range: [0.0, 1000.0]}
  
  - event_type_id: "game_end"
    weight: 0.2
    fields:
      final_score: {type: "int", range: [0, 10000]}
      won: {type: "string", values: ["true", "false"]}
```

---

## Troubleshooting

### ModuleNotFoundError: No module named 'dblstreamgen'

```python
# Make sure the wheel is installed
%pip install /Volumes/catalog/schema/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Restart Python
dbutils.library.restartPython()
```

### ModuleNotFoundError: No module named 'pyspark'

**This is expected locally** - v0.1.0 requires a Spark environment (Databricks). Use Databricks Runtime 15.4 LTS or above.

### ConfigurationError: Total weights must sum to 1.0

Check that your event type weights sum to exactly 1.0:

```yaml
# ❌ Wrong
event_types:
  - event_type_id: "a"
    weight: 0.5
  - event_type_id: "b"
    weight: 0.6
# Total: 1.1 (error!)

# ✅ Correct
event_types:
  - event_type_id: "a"
    weight: 0.5
  - event_type_id: "b"
    weight: 0.5
# Total: 1.0
```

### Kinesis: Stream Not Found

Create the stream first:

```bash
aws kinesis create-stream \
  --stream-name my-test-stream \
  --shard-count 2
```

Or let the library calculate shard count:

```yaml
sink_config:
  auto_shard_calculation: true
  # Library will calculate based on throughput
```

### AWS Credentials Error

Use Databricks secrets (recommended):

```python
# Store secrets
dbutils.secrets.put(scope="my-scope", key="aws-key", string_value="your-key")
dbutils.secrets.put(scope="my-scope", key="aws-secret", string_value="your-secret")

# Use in config or code
aws_key = dbutils.secrets.get("my-scope", "aws-key")
```

Or use IAM role (if Databricks cluster has Kinesis permissions):

```yaml
sink_config:
  # Remove aws_access_key_id and aws_secret_access_key
  # Library will use instance profile
```

---

## Advanced Usage

### Batch Mode

Generate a fixed number of events:

```yaml
generation_mode: "batch"

batch_config:
  total_rows: 1000000
  partitions: 16
```

```python
# Build batch DataFrame
spec = builder.build_spec_for_event_type(event_type_config)
df = spec.build()

# Write to Delta
df.write.format("delta").mode("append").save("/path/to/table")
```

### Multiple Event Types (10K+)

The union strategy scales to thousands of event types:

```yaml
event_types:
  - event_type_id: "event_0001"
    weight: 0.0001
    fields: {...}
  - event_type_id: "event_0002"
    weight: 0.0001
    fields: {...}
  # ... up to 10,000+ event types
```

### Custom Partition Keys

```yaml
sink_config:
  partition_key_field: "custom_field"  # Use any field from your config
```

---

## Next Steps

- 📖 **Full Documentation**: See `README.md`
- 🏗️ **Architecture Details**: See `docs/agent_context/TECHNICAL_SPECIFICATION.md`
- 📝 **Example Config**: See `sample/configs/config_v0.1.0.yaml`
- 📓 **Example Notebook**: See `sample/notebooks/01_simple_example_v0.1.0.py`
- 🎯 **Roadmap**: See `docs/agent_context/PROJECT_STATUS.md`

### Upcoming Features (v0.2.0+)

- ✅ **Unit tests** - Comprehensive test coverage
- ✅ **Delta sink** - Write to Delta tables
- ✅ **Kafka sink** - Stream to Apache Kafka
- ✅ **Event Hubs sink** - Stream to Azure Event Hubs
- ✅ **Advanced distributions** - Normal, Zipfian, Exponential
- ✅ **Template fields** - Email, phone, URL patterns
- ✅ **Dependent fields** - Country → City → Zip
- ✅ **Rate variance** - Sinusoidal, bursts, random walk

---

## Need Help?

- 📖 **README**: Main documentation
- 📋 **Technical Spec**: `docs/agent_context/TECHNICAL_SPECIFICATION.md`
- 🚀 **Release Notes**: `V0.1.0_RELEASE.md`
- 📊 **Project Status**: `docs/agent_context/PROJECT_STATUS.md`
- 🐛 **Issues**: `docs/agent_context/github_issues/`

---

**Happy streaming!** 🚀

*dblstreamgen v0.1.0 - Synthetic streaming data generation for Databricks*