# Databricks notebook source
# MAGIC %md
# MAGIC # dblstreamgen v0.1.0 - Simple Streaming Example
# MAGIC
# MAGIC This notebook demonstrates basic usage of dblstreamgen v0.1.0 to generate
# MAGIC synthetic streaming data and write to AWS Kinesis.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks Runtime 15.4 LTS or above
# MAGIC - AWS credentials configured in Databricks secrets
# MAGIC - Kinesis stream created in AWS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install dblstreamgen

# COMMAND ----------

# Install from Unity Catalog volume
%pip install /Volumes/catalog/schema/volume/dblstreamgen-0.1.0-py3-none-any.whl

# COMMAND ----------

# Restart Python to load the library
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import and Setup

# COMMAND ----------

from pyspark.sql import SparkSession
import dblstreamgen

# Get active Spark session
spark = SparkSession.getActiveSession()

print(f"✅ dblstreamgen version: {dblstreamgen.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load Configuration

# COMMAND ----------

# Load configuration from Unity Catalog volume
config = dblstreamgen.load_config(
    "/Volumes/catalog/schema/volume/config_v0.1.0.yaml"
)

# Validate configuration loaded correctly
print(f"✅ Configuration loaded")
print(f"   Generation mode: {config.data['generation_mode']}")
print(f"   Event types: {len(config.data['event_types'])}")
print(f"   Total rate: {config.data['streaming_config']['total_rows_per_second']} events/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Stream Orchestrator

# COMMAND ----------

# Create orchestrator
orchestrator = dblstreamgen.StreamOrchestrator(spark, config)

# Calculate rates for each event type
rates = orchestrator.calculate_rates()
print("✅ Event type rates:")
for event_id, rate in rates.items():
    print(f"   {event_id}: {rate:.0f} events/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Generate Unified Stream

# COMMAND ----------

# Generate unified stream (all event types)
unified_stream = orchestrator.create_unified_stream()

print("✅ Unified stream created")
print(f"   Is streaming: {unified_stream.isStreaming}")
print("\nSchema:")
unified_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Preview Stream (Console Output)
# MAGIC
# MAGIC Let's preview the stream output to console first to verify it's working.

# COMMAND ----------

# Write to console for preview (will run for 30 seconds)
preview_query = unified_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("numRows", 10) \
    .option("truncate", False) \
    .start()

# Let it run for 30 seconds
import time
time.sleep(30)

# Stop preview
preview_query.stop()
print("✅ Preview complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Register Kinesis Data Source

# COMMAND ----------

# Register the custom Kinesis data source
spark.dataSource.register(dblstreamgen.KinesisDataSource)

print("✅ Kinesis data source registered as 'dblstreamgen_kinesis'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Write to Kinesis
# MAGIC
# MAGIC Now write the unified stream to AWS Kinesis.

# COMMAND ----------

# Get AWS credentials from Databricks secrets
aws_key = dbutils.secrets.get(scope="my-scope", key="aws-access-key")
aws_secret = dbutils.secrets.get(scope="my-scope", key="aws-secret-key")

# Write to Kinesis
kinesis_query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", "web-events-stream") \
    .option("region", "us-east-1") \
    .option("partition_key_field", "partition_key") \
    .option("auto_shard_calculation", "true") \
    .option("aws_access_key_id", aws_key) \
    .option("aws_secret_access_key", aws_secret) \
    .option("checkpointLocation", "/tmp/dblstreamgen/checkpoints/kinesis") \
    .start()

print("✅ Streaming to Kinesis started!")
print(f"   Query ID: {kinesis_query.id}")
print(f"   Status: {kinesis_query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Monitor Stream

# COMMAND ----------

# Check stream status
print(f"Stream status: {kinesis_query.status}")
print(f"Recent progress:")
print(kinesis_query.recentProgress[-1] if kinesis_query.recentProgress else "No progress yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Stop Stream (When Done)

# COMMAND ----------

# Stop the streaming query when done
kinesis_query.stop()
print("✅ Streaming stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've successfully:
# MAGIC 1. ✅ Installed dblstreamgen v0.1.0
# MAGIC 2. ✅ Loaded configuration
# MAGIC 3. ✅ Created stream orchestrator
# MAGIC 4. ✅ Generated unified stream from multiple event types
# MAGIC 5. ✅ Written to AWS Kinesis
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Verify events in Kinesis stream
# MAGIC - Adjust rates in config as needed
# MAGIC - Add more event types
# MAGIC - Try batch generation mode
