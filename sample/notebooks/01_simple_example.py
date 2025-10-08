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
    "/Volumes/catalog/schema/volume/config.yaml"
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
# MAGIC Write the unified stream to AWS Kinesis. Choose the authentication method that fits your setup:
# MAGIC
# MAGIC | Method | Best For | Auto-Refresh | Cluster Type |
# MAGIC |--------|----------|--------------|--------------|
# MAGIC | Service Credential | Unity Catalog + Single-user | ✅ Yes | Single-user only |
# MAGIC | Instance Profile | Production, Non-Unity Catalog | ✅ Yes | Any |
# MAGIC | Direct Credentials | Legacy/Testing | ❌ No | Any |

# COMMAND ----------

# Get Kinesis configuration from config
sink_config = config.data['sink_config']
stream_name = sink_config['stream_name']
region = sink_config['region']
partition_key_field = sink_config.get('partition_key_field', 'partition_key')

print(f"Writing to Kinesis stream: {stream_name} in {region}")

# Determine authentication method from config
service_credential_name = sink_config.get('service_credential_name')
use_direct_credentials = sink_config.get('aws_access_key_id')

if service_credential_name:
    # Option 1: Unity Catalog service credential (single-user clusters only)
    print(f"✅ Using service credential: {service_credential_name} (auto-refresh enabled)")
    
    kinesis_query = unified_stream.writeStream \
        .format("dblstreamgen_kinesis") \
        .option("stream_name", stream_name) \
        .option("region", region) \
        .option("partition_key_field", partition_key_field) \
        .option("service_credential_name", service_credential_name) \
        .option("checkpointLocation", "/tmp/dblstreamgen/checkpoints/kinesis") \
        .trigger(processingTime='1 second') \
        .start()

elif use_direct_credentials:
    # Option 2: Direct credentials from Databricks secrets
    print("✅ Using direct AWS credentials from secrets")
    aws_key = dbutils.secrets.get(scope="my-scope", key="aws-access-key")
    aws_secret = dbutils.secrets.get(scope="my-scope", key="aws-secret-key")
    
    kinesis_query = unified_stream.writeStream \
        .format("dblstreamgen_kinesis") \
        .option("stream_name", stream_name) \
        .option("region", region) \
        .option("partition_key_field", partition_key_field) \
        .option("aws_access_key_id", aws_key) \
        .option("aws_secret_access_key", aws_secret) \
        .option("checkpointLocation", "/tmp/dblstreamgen/checkpoints/kinesis") \
        .trigger(processingTime='1 second') \
        .start()

else:
    # Option 3: Cluster instance profile (automatic)
    print("✅ Using cluster instance profile (recommended for production)")
    
    kinesis_query = unified_stream.writeStream \
        .format("dblstreamgen_kinesis") \
        .option("stream_name", stream_name) \
        .option("region", region) \
        .option("partition_key_field", partition_key_field) \
        .option("checkpointLocation", "/tmp/dblstreamgen/checkpoints/kinesis") \
        .trigger(processingTime='1 second') \
        .start()

print(f"✅ Streaming started! Query ID: {kinesis_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Monitor Stream Status

# COMMAND ----------

# Check query status
print(f"Query ID: {kinesis_query.id}")
print(f"Status: {kinesis_query.status}")
print(f"Recent Progress:")
print(kinesis_query.lastProgress)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Stop Stream
# MAGIC
# MAGIC When you're done testing, stop the stream.

# COMMAND ----------

# Stop the streaming query
kinesis_query.stop()
print("✅ Stream stopped")

# COMMAND ----------

