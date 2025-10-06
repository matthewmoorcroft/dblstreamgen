# Databricks notebook source
# MAGIC %md
# MAGIC # dblstreamgen - Installation & Quick Test
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Installs the dblstreamgen wheel
# MAGIC 2. Tests imports and basic functionality
# MAGIC 3. Generates a few test events (no publishing)
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Upload `dblstreamgen-0.1.0-py3-none-any.whl` to DBFS or Workspace
# MAGIC - Upload config files to DBFS or Workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install the Wheel

# COMMAND ----------

# DBTITLE 1,Install dblstreamgen
# Option A: Install from Unity Catalog Volume (RECOMMENDED)
%pip install /Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# Option B: Install from Workspace Repos (for development)
# %pip install /Workspace/Repos/<user>/dblstreamgen/dist/dblstreamgen-0.1.0-py3-none-any.whl

# Option C: Install from DBFS (legacy - not recommended)
# %pip install /dbfs/libraries/dblstreamgen-0.1.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Installation

# COMMAND ----------

# DBTITLE 1,Test Imports
import dblstreamgen

print(f"✓ dblstreamgen version: {dblstreamgen.__version__}")
print(f"✓ Available exports: {', '.join(dblstreamgen.__all__)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Configuration

# COMMAND ----------

# DBTITLE 1,Load Config (Generation Only)
# Load just the generation config (no Kinesis needed for this test)

# Option A: From Unity Catalog Volume (RECOMMENDED)
config = dblstreamgen.load_config(
    '/Volumes/<catalog>/<schema>/config/config_generation.yaml'
)

# Option B: From Workspace Repos (for development)
# config = dblstreamgen.load_config(
#     '/Workspace/Repos/<user>/dblstreamgen/sample/configs/config_generation.yaml'
# )

# Option C: From DBFS (legacy - not recommended)
# config = dblstreamgen.load_config(
#     '/dbfs/configs/dblstreamgen/config_generation.yaml'
# )

print("✓ Configuration loaded")
print(f"  Event types: {len(config['event_types'])}")
print(f"  Base rate: {config.get('test_execution.base_throughput_per_sec')} events/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Generate Test Events (No Publishing)

# COMMAND ----------

# DBTITLE 1,Generate 10 Sample Events
# Create generator
generator = dblstreamgen.StreamGenerator(config=config)

print("Generating 10 sample events...\n")

# Generate and display events
events = []
for i, event in enumerate(generator.generate()):
    events.append(event)
    
    print(f"Event {i+1}:")
    print(f"  Type: {event['event_type_id']}")
    print(f"  Timestamp: {event['event_timestamp']}")
    print(f"  Player ID: {event['player_id']}")
    print(f"  Payload size: {len(event['payload'])} bytes")
    print()
    
    if i >= 9:
        generator.stop()
        break

print("✓ Successfully generated 10 events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Event Distribution

# COMMAND ----------

# DBTITLE 1,Generate 1000 Events and Check Distribution
generator = dblstreamgen.StreamGenerator(config=config)

event_counts = {}
total_events = 1000

print(f"Generating {total_events} events to verify distribution...\n")

for i, event in enumerate(generator.generate()):
    event_type = event['event_type_id']
    event_counts[event_type] = event_counts.get(event_type, 0) + 1
    
    if i >= total_events - 1:
        generator.stop()
        break

print("Event Distribution:")
print("-" * 60)
for event_type, count in sorted(event_counts.items()):
    percentage = (count / total_events) * 100
    print(f"  {event_type:40s} {count:5d} ({percentage:5.1f}%)")
print("-" * 60)

# Compare with expected weights
print("\nExpected weights:")
for et in config['event_types']:
    expected = et['weight'] * 100
    print(f"  {et['event_type_id']:40s} ({expected:5.1f}%)")

print("\n✓ Distribution test complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Installation Complete!
# MAGIC 
# MAGIC The library is working correctly. Next steps:
# MAGIC 
# MAGIC 1. **Configure Kinesis**: Edit `config_source_kinesis.yaml` with your AWS credentials
# MAGIC 2. **Run StreamGenerator notebook**: See `02_stream_generator.py` for full example
# MAGIC 3. **Customize schemas**: Edit `config_generation.yaml` to add your event types
# MAGIC 
# MAGIC **Security Note**: Use Databricks secrets for AWS credentials:
# MAGIC ```python
# MAGIC dbutils.secrets.get(scope="dblstreamgen", key="aws-access-key")
# MAGIC ```

