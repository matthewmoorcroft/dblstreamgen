# dblstreamgen - Databricks Sample Setup

This folder contains example configurations and notebooks for running `dblstreamgen` v0.1.0 in Databricks.

---

## 📁 Folder Structure

```
sample/
├── README.md                      # This file
├── UNITY_CATALOG_SETUP.md         # Detailed Unity Catalog setup
├── configs/
│   └── config.yaml                # Example configuration
└── notebooks/
    └── 01_simple_example.py       # Complete end-to-end example
```

---

## 🚀 Quick Start in Databricks

### Step 1: Create Unity Catalog Volumes

```sql
-- Create volume for libraries
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.libraries
COMMENT 'Shared Python wheels and libraries';

-- Create volume for configs
CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.configs
COMMENT 'Configuration files for dblstreamgen';
```

### Step 2: Upload the Wheel

**Build the wheel** (if not already built):
```bash
cd /path/to/dblstreamgen
python3 -m build
```

**Upload to Unity Catalog**:
```bash
# Using Databricks CLI
databricks fs cp dist/dblstreamgen-0.1.0-py3-none-any.whl \
  dbfs:/Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Or upload via UI: Catalog → Volumes → libraries → Upload Files
```

### Step 3: Upload Configuration

**Upload config file**:
```bash
databricks fs cp sample/configs/config.yaml \
  dbfs:/Volumes/<catalog>/<schema>/configs/config.yaml
```

**Edit the config** to match your use case (see Configuration section below).

### Step 4: Configure AWS Credentials (for Kinesis)

**Create Databricks secrets**:
```bash
# Create secret scope
databricks secrets create-scope dblstreamgen

# Add AWS credentials
databricks secrets put --scope dblstreamgen --key aws-access-key-id
databricks secrets put --scope dblstreamgen --key aws-secret-access-key
```

### Step 5: Run the Example Notebook

1. Import `notebooks/01_simple_example.py` into Databricks
2. Update paths to your Unity Catalog volumes
3. Run all cells

---

## 📝 Configuration

The `config.yaml` file defines your data generation schema. See [QUICKSTART.md](../QUICKSTART.md) for full documentation.

**Example - E-commerce Events**:
```yaml
common_fields:
  customer_id:
    type: "int"
    range: [100000, 999999]

event_types:
  - event_type_id: "product_view"
    weight: 0.6  # 60% of events
    fields:
      product_id:
        type: "int"
        range: [1, 10000]
  
  - event_type_id: "purchase"
    weight: 0.4  # 40% of events
    fields:
      amount:
        type: "float"
        range: [10.0, 1000.0]

generation_mode: "streaming"
streaming_config:
  total_rows_per_second: 1000

serialization_format: "json"

sink_config:
  type: "kinesis"
  stream_name: "my-events-stream"
  region: "us-east-1"
  partition_key_field: "customer_id"
```

---

## 📓 Notebooks

### `01_simple_example.py`
Complete end-to-end example showing:
1. Install dblstreamgen wheel
2. Load configuration
3. Create StreamOrchestrator
4. Generate unified streaming DataFrame
5. Write to Kinesis using custom PySpark DataSource

**Runtime**: Databricks Runtime 15.4 LTS or above

---

## 🔐 Security Best Practices

**Never commit credentials!**

Always use Databricks secrets for sensitive information:

```python
# In your notebook
aws_key = dbutils.secrets.get(scope="dblstreamgen", key="aws-access-key-id")
aws_secret = dbutils.secrets.get(scope="dblstreamgen", key="aws-secret-access-key")

# Pass to stream writer
query = stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "my-stream") \
    .option("region", "us-east-1") \
    .option("aws_access_key_id", aws_key) \
    .option("aws_secret_access_key", aws_secret) \
    .start()
```

---

## 🎯 Common Use Cases

### E-commerce Platform
```yaml
common_fields:
  customer_id: {type: "int", range: [100000, 999999]}
  session_id: {type: "uuid"}

event_types:
  - event_type_id: "product_view"
    weight: 0.5
    fields:
      product_id: {type: "int", range: [1, 10000]}
      
  - event_type_id: "add_to_cart"
    weight: 0.3
    fields:
      product_id: {type: "int", range: [1, 10000]}
      quantity: {type: "int", range: [1, 5]}
      
  - event_type_id: "purchase"
    weight: 0.2
    fields:
      order_id: {type: "uuid"}
      total_amount: {type: "float", range: [10.0, 1000.0]}
```

### IoT Sensors
```yaml
common_fields:
  device_id: {type: "int", range: [1, 1000]}
  timestamp: {type: "timestamp"}

event_types:
  - event_type_id: "temperature_reading"
    weight: 0.6
    fields:
      temperature_celsius: {type: "float", range: [-20.0, 50.0]}
      
  - event_type_id: "battery_status"
    weight: 0.4
    fields:
      battery_percent: {type: "int", range: [0, 100]}
```

### Gaming Platform
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
      action_type: {type: "string", values: ["move", "shoot", "jump"]}
      
  - event_type_id: "game_end"
    weight: 0.2
    fields:
      final_score: {type: "int", range: [0, 10000]}
```

---

## 🛠️ Troubleshooting

### ModuleNotFoundError: No module named 'dblstreamgen'
```python
# Install the wheel
%pip install /Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Restart Python
dbutils.library.restartPython()
```

### ConfigurationError: Total weights must sum to 1.0
Check your event type weights:
```yaml
event_types:
  - event_type_id: "type_a"
    weight: 0.6  # 60%
  - event_type_id: "type_b"
    weight: 0.4  # 40%
# Total: 1.0 ✅
```

### Kinesis: Stream not found
Create the stream first:
```bash
aws kinesis create-stream \
  --stream-name my-events-stream \
  --shard-count 2
```

### AWS Credentials Error
Verify secrets are configured:
```python
# Test secret access
dbutils.secrets.get(scope="dblstreamgen", key="aws-access-key-id")
```

---

## 📚 Additional Resources

- **Full Documentation**: [README.md](../README.md)
- **Quick Start Guide**: [QUICKSTART.md](../QUICKSTART.md)
- **Technical Specification**: [docs/agent_context/TECHNICAL_SPECIFICATION.md](../docs/agent_context/TECHNICAL_SPECIFICATION.md)
- **Unity Catalog Setup**: [UNITY_CATALOG_SETUP.md](./UNITY_CATALOG_SETUP.md)
- **GitHub Issues**: https://github.com/matthewmoorcroft/dblstreamgen/issues

---

## 📋 Notes

- This `sample/` folder is in `.gitignore` - customize freely
- Update configuration to match your use case
- Library requires Databricks Runtime 15.4 LTS or above
- Uses dbldatagen for data generation (no standalone Python support)
- Supports Kinesis, Kafka, Event Hubs, Delta (Kafka/Event Hubs coming in v0.3.0)

---

**Version**: v0.1.0  
**Updated**: October 2024