# dblstreamgen

**Generate synthetic streaming data at scale for Databricks using dbldatagen**

[![Databricks](https://img.shields.io/badge/Databricks-15.4%2B-orange)](https://databricks.com)
[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green)](LICENSE)

---

## 🎯 What is dblstreamgen?

`dblstreamgen` is a **Databricks-native library** for generating realistic synthetic streaming data at scale. It's designed to test and validate data pipelines, streaming applications, and Delta Live Tables without needing production data.

**Perfect for:**
- 🧪 Testing streaming pipelines (Kinesis, Kafka, Event Hubs)
- 📊 Validating Delta Live Tables and medallion architectures
- 🏋️ Load testing and performance benchmarking
- 🎓 Training and demos with realistic data
- 🔒 Development without access to production data

---

## ✨ Key Features

- **🚀 High Throughput**: Generate millions of events per second using Spark
- **🎭 Multiple Event Types**: Support for 10K+ different event schemas
- **🔄 Unified Streaming**: All event types in a single streaming query
- **📤 Multiple Sinks**: Kinesis, Kafka, Event Hubs, Delta
- **⚙️ Configuration-Driven**: Define everything in YAML
- **🎨 Use-Case Agnostic**: Works for gaming, e-commerce, IoT, finance, etc.
- **📦 Flexible Serialization**: JSON, Avro, Binary, Protobuf

---

## 🏗️ Architecture

```
YAML Config → dbldatagen Generation → Union Strategy → Custom Sink Writers
```

**How it works:**
1. Define event types and fields in YAML
2. Library creates dbldatagen specs for each event type
3. Generates separate Spark DataFrames per type
4. Pre-serializes and unions into single stream
5. Writes to your sink (Kinesis, Kafka, etc.)

**Scales to:**
- 10,000+ event types
- 1M+ events/second
- Auto-calculated sharding (Kinesis)

---

## 📦 Installation

### For Databricks

**Option 1: Install from wheel**
```python
%pip install /Volumes/catalog/schema/volume/dblstreamgen-0.2.0-py3-none-any.whl
```

**Option 2: Install from source**
```bash
git clone https://github.com/yourorg/dblstreamgen.git
cd dblstreamgen
python -m build
# Upload wheel to Unity Catalog volume
```

---

## 🚀 Quick Start

### Step 1: Define Your Config

**`config.yaml`**:
```yaml
# Common fields across all events
common_fields:
  user_id:
    type: "int"
    range: [1, 1000000]

# Event type definitions  
event_types:
  - event_type_id: "user.session.start"
    weight: 0.50  # 50% of total throughput
    fields:
      session_id:
        type: "uuid"
      device_type:
        type: "string"
        values: ["iOS", "Android", "Web"]
        weights: [0.4, 0.4, 0.2]
  
  - event_type_id: "user.purchase"
    weight: 0.30  # 30% of total throughput
    fields:
      transaction_id:
        type: "uuid"
      amount:
        type: "float"
        range: [0.99, 999.99]
  
  - event_type_id: "user.session.end"
    weight: 0.20  # 20% of total throughput
    fields:
      session_duration_sec:
        type: "int"
        range: [60, 7200]

# Generation settings
generation_mode: "streaming"

streaming_config:
  total_rows_per_second: 1000  # Total rate across all types

serialization_format: "json"

# Sink configuration
sink_config:
  type: "kinesis"
  stream_name: "my-events-stream"
  region: "us-east-1"
  partition_key_field: "user_id"
  auto_shard_calculation: true
  aws_access_key_id: "{{secrets/my-scope/aws-key}}"
  aws_secret_access_key: "{{secrets/my-scope/aws-secret}}"
```

### Step 2: Generate and Stream

**Databricks Notebook**:
```python
from pyspark.sql import SparkSession
import dblstreamgen

# Get Spark session
spark = SparkSession.getActiveSession()

# Load configuration
config = dblstreamgen.load_config("/Volumes/catalog/schema/volume/config.yaml")

# Create orchestrator
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)

# Generate unified stream
unified_stream = orchestrator.create_unified_stream()

# Write to Kinesis
query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "my-events-stream") \
    .option("region", "us-east-1") \
    .option("partition_key_field", "user_id") \
    .option("auto_shard_calculation", "true") \
    .option("aws_access_key_id", dbutils.secrets.get("my-scope", "aws-key")) \
    .option("aws_secret_access_key", dbutils.secrets.get("my-scope", "aws-secret")) \
    .option("checkpointLocation", "/tmp/checkpoints/kinesis") \
    .start()

print("✅ Streaming to Kinesis...")
query.awaitTermination()
```

**That's it!** You're now generating 1000 events/second across 3 event types.

---

## 📚 Examples

### Example 1: E-commerce Events

```yaml
common_fields:
  customer_id:
    type: "int"
    range: [100000, 999999]
  
  store_id:
    type: "string"
    values: ["store_nyc", "store_sf", "store_la"]
    weights: [0.5, 0.3, 0.2]

event_types:
  - event_type_id: "cart.add"
    weight: 0.40
    fields:
      product_id:
        type: "int"
        range: [1000, 9999]
      quantity:
        type: "int"
        range: [1, 5]
  
  - event_type_id: "checkout.complete"
    weight: 0.10
    fields:
      order_id:
        type: "uuid"
      total_amount:
        type: "float"
        range: [10.0, 500.0]
```

### Example 2: IoT Sensor Data

```yaml
common_fields:
  device_id:
    type: "string"
    values: ["sensor_001", "sensor_002", "sensor_003"]
  
  location:
    type: "string"
    values: ["warehouse_a", "warehouse_b", "warehouse_c"]

event_types:
  - event_type_id: "sensor.temperature"
    weight: 0.50
    fields:
      temperature_celsius:
        type: "float"
        range: [-10.0, 50.0]
  
  - event_type_id: "sensor.humidity"
    weight: 0.30
    fields:
      humidity_percent:
        type: "float"
        range: [0.0, 100.0]
  
  - event_type_id: "sensor.alert"
    weight: 0.20
    fields:
      alert_type:
        type: "string"
        values: ["high_temp", "low_temp", "offline"]
```

### Example 3: Write to Delta Table

```yaml
sink_config:
  type: "delta"
  path: "/mnt/delta/events"
  mode: "append"
```

```python
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)
unified_stream = orchestrator.create_unified_stream()

query = unified_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/delta") \
    .start("/mnt/delta/events")
```

### Example 4: Batch Generation

```yaml
generation_mode: "batch"

batch_config:
  total_rows: 1000000
  partitions: 8
```

```python
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)
batch_df = orchestrator.create_unified_stream()  # Returns batch DataFrame

# Write to Delta
batch_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/batch_events")

print(f"✅ Generated {batch_df.count():,} events")
```

---

## 🎨 Supported Field Types

| Type | Description | Example |
|------|-------------|---------|
| `uuid` | UUID v4 | `session_id`, `transaction_id` |
| `int` | Integer with range | `user_id: [1, 1000000]` |
| `float` | Float with range | `amount: [0.99, 999.99]` |
| `string` | String with values/weights | `device: ["iOS", "Android"]` |
| `timestamp` | Current timestamp | `event_timestamp` |

---

## 🔌 Supported Sinks

| Sink | Status | Description |
|------|--------|-------------|
| **AWS Kinesis** | ✅ Ready | Custom PySpark DataSource with parallel batching |
| **Delta Lake** | ✅ Ready | Native Spark connector |
| **Apache Kafka** | 🚧 Coming Soon | Native Spark connector wrapper |
| **Azure Event Hubs** | 🚧 Coming Soon | Custom PySpark DataSource |

---

## ⚙️ Configuration Reference

### Common Fields
Define fields that appear in **all** event types:
```yaml
common_fields:
  user_id:
    type: "int"
    range: [1, 1000000]
  timestamp:
    type: "timestamp"
```

### Event Types
Define different event schemas with weights:
```yaml
event_types:
  - event_type_id: "event.name"
    weight: 0.50  # Proportion of total throughput (must sum to 1.0)
    fields:
      field_name:
        type: "uuid"  # or "int", "float", "string", "timestamp"
```

### Generation Modes

**Streaming**:
```yaml
generation_mode: "streaming"
streaming_config:
  total_rows_per_second: 1000  # Distributed by weights
```

**Batch**:
```yaml
generation_mode: "batch"
batch_config:
  total_rows: 1000000
  partitions: 8
```

### Serialization
```yaml
serialization_format: "json"  # or "avro", "binary", "protobuf"
```

### Sink Configuration

**Kinesis**:
```yaml
sink_config:
  type: "kinesis"
  stream_name: "my-stream"
  region: "us-east-1"
  partition_key_field: "user_id"  # Which field to use for partitioning
  auto_shard_calculation: true    # Auto-calculate shard count
  # shard_count: 5                # Or specify manually
```

**Delta**:
```yaml
sink_config:
  type: "delta"
  path: "/mnt/delta/events"
  mode: "append"  # or "overwrite"
```

---

## 🎓 How It Works

### 1. Weight-Based Rate Distribution
```yaml
event_types:
  - event_type_id: "type_a"
    weight: 0.5  # Gets 500 events/sec
  - event_type_id: "type_b"
    weight: 0.3  # Gets 300 events/sec
  - event_type_id: "type_c"
    weight: 0.2  # Gets 200 events/sec

streaming_config:
  total_rows_per_second: 1000  # Total
```

### 2. Union Strategy for Multiple Event Types
```
Event Type A DataFrame (schema: a, b, c) → Serialize → unified schema
Event Type B DataFrame (schema: d, e, f) → Serialize → unified schema
Event Type C DataFrame (schema: g, h, i) → Serialize → unified schema
                                              ↓
                          Union into single stream
                                              ↓
                     Schema: event_type_id, timestamp,
                             partition_key, serialized_payload
```

**Benefits:**
- No schema conflicts
- Scales to 10K+ event types
- Single streaming query

### 3. Kinesis Auto-Sharding
```python
required_shards = ceil(total_events_per_sec / 1000)
```

For 5000 events/sec:
- Required shards: 5
- Each shard: 1000 events/sec, 1 MiB/sec

---

## 📊 Performance

| Scenario | Throughput | Resource Usage |
|----------|------------|----------------|
| 1 event type | 50K events/sec | 1 executor, 2 cores |
| 10 event types | 100K events/sec | 2 executors, 4 cores |
| 100 event types | 500K events/sec | 4 executors, 8 cores |
| 1000 event types | 1M+ events/sec | 8 executors, 16 cores |

*Based on Databricks cluster with i3.xlarge instances*

---

## 🔧 Development

### Building from Source

```bash
# Clone repository
git clone https://github.com/yourorg/dblstreamgen.git
cd dblstreamgen

# Install dependencies
pip install -r requirements.txt

# Build wheel
python -m build

# Upload to Unity Catalog volume
# (see sample/UNITY_CATALOG_SETUP.md)
```

### Running Tests
```bash
pytest tests/
```

---

## 📖 Documentation

- [Technical Specification](docs/agent_context/TECHNICAL_SPECIFICATION.md)
- [Databricks Setup Guide](sample/DATABRICKS_SETUP.md)
- [Unity Catalog Setup](sample/UNITY_CATALOG_SETUP.md)
- [Quick Start Guide](QUICKSTART.md)

---

## 🤝 Contributing

Contributions welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Roadmap

**v2.0 (Current)**:
- ✅ dbldatagen integration
- ✅ Union streaming strategy
- ✅ Kinesis PySpark DataSource
- 🚧 Delta/Kafka sink wrappers

**v2.1**:
- Event Hubs sink
- Advanced field types
- Unit tests

**v2.2**:
- Rate variance patterns
- Schema auto-generation (LLM)
- Performance optimizations

---

## 📄 License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

---

## 💬 Support

- **Issues**: [GitHub Issues](https://github.com/yourorg/dblstreamgen/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourorg/dblstreamgen/discussions)
- **Email**: support@yourorg.com

---

## 🙏 Acknowledgments

- [dbldatagen](https://github.com/databrickslabs/dbldatagen) - Data generation engine
- [Databricks](https://databricks.com) - Platform and runtime
- Community contributors

---

**Built with ❤️ for the Databricks community**