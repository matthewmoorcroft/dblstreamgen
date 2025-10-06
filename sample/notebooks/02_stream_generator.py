# Databricks notebook source
# MAGIC %md
# MAGIC # dblstreamgen - StreamGenerator with Kinesis
# MAGIC 
# MAGIC This notebook demonstrates generating synthetic events and publishing to AWS Kinesis.
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - dblstreamgen wheel installed (run `01_install_and_test.py` first)
# MAGIC - AWS Kinesis stream created
# MAGIC - Databricks secrets configured with AWS credentials
# MAGIC - Config files uploaded and updated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# DBTITLE 1,Set Parameters
# Test duration (seconds)
DURATION_SECONDS = 300  # 5 minutes

# Batch size for publishing
BATCH_SIZE = 100

# Config paths - UPDATE THESE with your Unity Catalog paths!
CATALOG = "your_catalog"      # ← CHANGE THIS
SCHEMA = "your_schema"        # ← CHANGE THIS

GENERATION_CONFIG = f'/Volumes/{CATALOG}/{SCHEMA}/config/config_generation.yaml'
SOURCE_CONFIG = f'/Volumes/{CATALOG}/{SCHEMA}/config/config_source_kinesis.yaml'

print(f"Duration: {DURATION_SECONDS} seconds ({DURATION_SECONDS/60:.1f} minutes)")
print(f"Batch size: {BATCH_SIZE} events")
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# DBTITLE 1,Import Library
import dblstreamgen
import time
from datetime import datetime

print(f"✓ dblstreamgen v{dblstreamgen.__version__}")

# COMMAND ----------

# DBTITLE 1,Load Configuration
config = dblstreamgen.load_config(GENERATION_CONFIG, SOURCE_CONFIG)

print("✓ Configuration loaded")
print(f"  Event types: {len(config['event_types'])}")
print(f"  Target rate: {config.get('test_execution.base_throughput_per_sec')} events/sec")
print(f"  Kinesis stream: {config['kinesis_config']['stream_name']}")
print(f"  AWS region: {config['kinesis_config']['region']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Generator & Publisher

# COMMAND ----------

# DBTITLE 1,Initialize Generator
generator = dblstreamgen.StreamGenerator(config=config)
print("✓ StreamGenerator created")

# COMMAND ----------

# DBTITLE 1,Initialize Publisher
# Note: If using Databricks secrets, credentials will be loaded automatically
publisher = dblstreamgen.KinesisPublisher(config=config['kinesis_config'])
print("✓ KinesisPublisher created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate & Publish Events

# COMMAND ----------

# DBTITLE 1,Run Generation Loop
print("=" * 70)
print(f"Starting generation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

start_time = time.time()
total_events = 0
total_failed = 0
batch_buffer = []
last_print_time = start_time

try:
    for event in generator.generate():
        batch_buffer.append(event)
        
        # Publish in batches
        if len(batch_buffer) >= BATCH_SIZE:
            result = publisher.publish_batch(batch_buffer)
            total_events += result.records_sent
            total_failed += result.failed_records
            batch_buffer = []
            
            # Print progress every 10 seconds
            current_time = time.time()
            if current_time - last_print_time >= 10:
                elapsed = current_time - start_time
                rate = total_events / elapsed if elapsed > 0 else 0
                
                print(f"  Progress: {total_events:,} sent, {total_failed} failed ({rate:.0f} events/sec)")
                last_print_time = current_time
            
            # Warn on failures
            if result.failed_records > 0:
                print(f"  ⚠ Warning: {result.failed_records} records failed in this batch")
        
        # Stop after duration
        if time.time() - start_time >= DURATION_SECONDS:
            generator.stop()
            break

except KeyboardInterrupt:
    print("\n⚠ Interrupted by user")
    generator.stop()

except Exception as e:
    print(f"\n✗ Error during generation: {e}")
    generator.stop()
    raise

# Publish remaining events
if batch_buffer:
    result = publisher.publish_batch(batch_buffer)
    total_events += result.records_sent
    total_failed += result.failed_records

# Cleanup
publisher.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# DBTITLE 1,Display Summary
elapsed = time.time() - start_time
avg_rate = total_events / elapsed if elapsed > 0 else 0

print("=" * 70)
print("Test Complete!")
print("=" * 70)
print(f"Total events sent:    {total_events:,}")
print(f"Total failed:         {total_failed:,}")
print(f"Success rate:         {(total_events/(total_events+total_failed)*100) if (total_events+total_failed) > 0 else 0:.1f}%")
print(f"Duration:             {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
print(f"Average throughput:   {avg_rate:.0f} events/sec")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Events in Kinesis (Optional)

# COMMAND ----------

# DBTITLE 1,Check Kinesis Stream
# Use boto3 directly to verify events were written
import boto3

kinesis = boto3.client(
    'kinesis',
    region_name=config['kinesis_config']['region']
)

# Get stream description
stream_name = config['kinesis_config']['stream_name']
response = kinesis.describe_stream(StreamName=stream_name)

print(f"Stream: {stream_name}")
print(f"Status: {response['StreamDescription']['StreamStatus']}")
print(f"Shards: {len(response['StreamDescription']['Shards'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC 1. **Adjust rate**: Edit `config_generation.yaml` → `test_execution.base_throughput_per_sec`
# MAGIC 2. **Add event types**: Edit `config_generation.yaml` → `event_types`
# MAGIC 3. **Run longer**: Change `DURATION_SECONDS` parameter above
# MAGIC 4. **Monitor pipeline**: Check your downstream DLT/Delta tables
# MAGIC 5. **Try BatchGenerator**: See `03_batch_generator.py` (coming soon)

