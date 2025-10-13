# dblstreamgen

**Generate synthetic streaming data at scale for Databricks using dbldatagen**

[![Databricks](https://img.shields.io/badge/Databricks-15.4%2B-orange)](https://databricks.com)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-Databricks-green)](LICENSE.md)

---

## What is dblstreamgen?

`dblstreamgen` is a **Databricks-native library** for generating realistic synthetic streaming data at scale. It's designed to test and validate data pipelines, streaming applications, and Spark Structured Streaming without needing production data.

**Use Cases:**
- Testing streaming pipelines (Kinesis, Kafka, Event Hubs)
- Validating Spark Structured Streaming pipelines
- Load testing and performance benchmarking
- Training and demonstrations with realistic data
- Development environments without production data access

---

## Overview

`dblstreamgen` is a Spark/Databricks library for generating synthetic streaming data using `dbldatagen`. It supports multiple event types, weighted rate distribution, and streaming to various sinks (Kinesis, Kafka, Event Hubs, Delta).

**Key Features:**
- **Config-driven** - Define schemas in YAML
- **dbldatagen-powered** - Leverages Spark for scale
- **Multiple event types** - Wide schema approach for 1500+ types
- **Flexible sinks** - Kinesis, Kafka, Event Hubs, Delta (planned)
- **Use-case agnostic** - Works for any domain

---

## Installation

### Databricks

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

print(f"dblstreamgen v{dblstreamgen.__version__} installed successfully")
```

---

## Quick Start - Two Approaches

Choose your preferred approach:
- **Notebook Quickstart** (Recommended): Complete end-to-end workflow in 4 cells
- **Step-by-Step Guide**: Detailed walkthrough of each component

### Notebook Quickstart (4 Cells)

Copy these cells into a Databricks notebook or see the complete example at `sample/notebooks/01_simple_example.py`:

**Cell 1 - Install:**
```python
# Install from Unity Catalog volume
%pip install /Volumes/catalog/schema/libraries/dblstreamgen-0.1.0-py3-none-any.whl
dbutils.library.restartPython()
```

**Cell 2 - Setup:**
```python
from pyspark.sql import SparkSession
import dblstreamgen
from dblstreamgen.sinks import KinesisDataSource

spark = SparkSession.getActiveSession()
spark.dataSource.register(KinesisDataSource)

# Load configuration (upload simple_config.yaml to your workspace first)
config = dblstreamgen.load_config("/Workspace/path/to/simple_config.yaml")

# Create orchestrator
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)
print("Setup complete")
```

**Cell 3 - Generate & Write to Kinesis:**
```python
# Create unified stream
unified_stream = orchestrator.create_unified_stream()

# Write to Kinesis
query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "web-events-stream") \
    .option("region", "us-east-1") \
    .option("partition_key_field", "event_key") \
    .option("service_credential", "my-kinesis-credential") \
    .option("checkpointLocation", "/tmp/dblstreamgen/checkpoints") \
    .trigger(processingTime='1 second') \
    .start()

print(f"Streaming to Kinesis - Query ID: {query.id}")
```

**Cell 4 - Monitor (Optional):**
```python
# Check stream status
query.status

# View progress
query.recentProgress

# Stop when done
# query.stop()
```

**See the complete example:** `sample/notebooks/01_simple_example.py` includes reading back from Kinesis and analyzing event distributions.

---

## Step-by-Step Guide

### Step 1: Use or Customize Sample Configuration

We provide a complete example configuration in `sample/configs/simple_config.yaml`:

```yaml
# Key sections from simple_config.yaml:
common_fields:
  event_name: {type: "string"}       # Event type identifier
  event_key: {type: "string", values: [...]}  # Partition key
  event_timestamp: {type: "timestamp"}
  event_id: {type: "uuid"}
  session_id: {type: "uuid"}

event_types:
  - event_type_id: "user.page_view"
    weight: 0.60
    fields:
      page_url: {...}
      referrer: {...}
```

See `sample/configs/simple_config.yaml` for the complete configuration.

### Step 2: Generate and Stream Data

```python
from pyspark.sql import SparkSession
import dblstreamgen

# Get Spark session
spark = SparkSession.getActiveSession()

# Load configuration
config = dblstreamgen.load_config("/Workspace/path/to/simple_config.yaml")

# Create orchestrator
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)

# Generate unified stream
unified_stream = orchestrator.create_unified_stream()

# Output schema:
# root
#  |-- partition_key: string (for Kinesis routing)
#  |-- data: string (flat JSON with all event fields)

# Display the stream schema
unified_stream.printSchema()

# Preview the data (for testing)
display(unified_stream)
```

### Understanding the Output

The library generates a DataFrame with two columns:
- **partition_key**: Used for Kinesis sharding (based on `partition_key_field` in config)
- **data**: Flat JSON string containing ALL event fields

Example `data` field content:
```json
{
  "event_name": "user.page_view",
  "event_key": "user_1",
  "event_timestamp": "2025-10-10T12:00:00.123Z",
  "event_id": "552c2a1e-5ab5-4a0f-95d6-57a7866fb624",
  "session_id": "e28c2df6-1b49-4bc2-be25-5515f39b721e",
  "page_url": "/home",
  "referrer": "google",
  "user_id": 12345
}
```

This flat structure is compatible with Spark Structured Streaming ingest patterns using `payload:field_name` syntax.

### Step 3: Write to Kinesis

```python
# Register Kinesis DataSource
spark.dataSource.register(dblstreamgen.KinesisDataSource)

# Option 1: Unity Catalog Service Credential (Recommended for single-user clusters)
query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "web-events-stream") \
    .option("region", "us-east-1") \
    .option("partition_key_field", "event_key") \
    .option("service_credential", "my-kinesis-credential") \
    .option("checkpointLocation", "/tmp/checkpoints/kinesis") \
    .trigger(processingTime='1 second') \
    .start()

# Option 2: Direct Credentials from Databricks Secrets
aws_key = dbutils.secrets.get("my-scope", "aws-key")
aws_secret = dbutils.secrets.get("my-scope", "aws-secret")

query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "web-events-stream") \
    .option("region", "us-east-1") \
    .option("partition_key_field", "event_key") \
    .option("aws_access_key_id", aws_key) \
    .option("aws_secret_access_key", aws_secret) \
    .option("checkpointLocation", "/tmp/checkpoints/kinesis") \
    .trigger(processingTime='1 second') \
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
| `uuid` | None | `event_id: {type: "uuid"}` |
| `int` | `range: [min, max]` | `user_id: {type: "int", range: [1, 1000000]}` |
| `float` | `range: [min, max]` | `amount: {type: "float", range: [10.0, 500.0]}` |
| `string` | `values: [...]` or `values: [...], weights: [...]` | `page_url: {type: "string", values: ["/home", "/products"]}` |
| `timestamp` | None (uses current time) | `event_timestamp: {type: "timestamp"}` |

### Common Fields

Fields shared across all event types (from simple_config.yaml):

```yaml
common_fields:
  event_name:
    type: "string"  # Automatically populated with event_type_id
  event_key:
    type: "string"
    values: ["user_1", "user_2", "user_3", "user_4", "user_5"]
  event_timestamp:
    type: "timestamp"
  event_id:
    type: "uuid"
  session_id:
    type: "uuid"
```

**Note:** Don't duplicate common fields in event type `fields` section.

### Weights Must Sum to 1.0

```yaml
event_types:
  - event_type_id: "type_a"
    weight: 0.6    # 60%
  - event_type_id: "type_b"
    weight: 0.3    # 30%
  - event_type_id: "type_c"
    weight: 0.1    # 10%
# Total: 1.0 (correct)
```

---

## Example Use Cases

### Web Analytics (see simple_config.yaml)

The included `simple_config.yaml` demonstrates a web analytics pattern:
- **user.page_view** (60%): Page navigation with URL and referrer
- **user.click** (30%): User interactions with UI elements
- **user.purchase** (10%): Transaction events

Adapt this pattern for your use case by:
1. Changing `event_type_id` to match your domain
2. Updating field names and value distributions
3. Adjusting weights to match your traffic patterns

**See it in action:** `sample/notebooks/01_simple_example.py` shows the complete workflow.

### Stress Testing (see 1500_events_config.yaml)

For scale testing with 1500+ event types, see `sample/configs/1500_events_config.yaml`.

---

## Troubleshooting

### ModuleNotFoundError: No module named 'dblstreamgen'

```python
# Make sure the wheel is installed
%pip install /Volumes/catalog/schema/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Restart Python
dbutils.library.restartPython()
```

### ConfigurationError: Total weights must sum to 1.0

Check that your event type weights sum to exactly 1.0:

```yaml
# Wrong - Total: 1.1
event_types:
  - event_type_id: "a"
    weight: 0.5
  - event_type_id: "b"
    weight: 0.6
# Total: 1.1 (error)

# Correct - Total: 1.0
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

### Kinesis Authentication Fails

**For service_credential:**
- Use single-user cluster (not shared or no-isolation)
- Verify credential exists: `SELECT * FROM system.information_schema.credentials`
- Ensure you have USE privilege on the credential

**For direct credentials:**
- Store in Databricks secrets, not plain text
- Verify scope exists: `dbutils.secrets.listScopes()`

```python
# Store secrets
dbutils.secrets.put(scope="my-scope", key="aws-key", string_value="your-key")
dbutils.secrets.put(scope="my-scope", key="aws-secret", string_value="your-secret")

# Use in config or code
aws_key = dbutils.secrets.get("my-scope", "aws-key")
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

### Multiple Event Types (1500+)

The wide schema approach scales to thousands of event types:

```yaml
event_types:
  - event_type_id: "event_type_0001"
    weight: 0.0006666667
    fields: {...}
  - event_type_id: "event_type_0002"
    weight: 0.0006666667
    fields: {...}
  # ... up to 1,500+ event types
```

See `sample/configs/1500_events_config.yaml` for a complete example.

### Custom Partition Keys

```yaml
sink_config:
  partition_key_field: "custom_field"  # Use any field from your config
```

---

## Documentation

- **Example Config**: `sample/configs/simple_config.yaml`
- **Stress Test Config**: `sample/configs/1500_events_config.yaml`
- **Example Notebook**: `sample/notebooks/01_simple_example.py`


---

## License

Databricks License - see [LICENSE.md](LICENSE.md) for details.

This library is licensed under the Databricks License and is intended for use in connection with Databricks Services.





---

## Acknowledgments

- [dbldatagen](https://github.com/databrickslabs/dbldatagen) - Data generation engine
- [Databricks](https://databricks.com) - Platform and runtime
- Community contributors

---

*dblstreamgen v0.1.0 - Synthetic streaming data generation for Databricks*
