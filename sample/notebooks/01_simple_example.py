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
# MAGIC ## Step 6: Register Kinesis Data Source

# COMMAND ----------

# Register the custom Kinesis data source
spark.dataSource.register(dblstreamgen.KinesisDataSource)

print("✅ Kinesis data source registered as 'dblstreamgen_kinesis'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write to Kinesis
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
# MAGIC ## Step 8: Read from Kinesis and Analyze Distribution
# MAGIC
# MAGIC Read back the data from Kinesis to validate the distribution matches our configuration.

# COMMAND ----------

# Read from Kinesis
kinesis_df = (spark.readStream
    .format("kinesis")
    .option("streamName", stream_name)
    .option("region", region)
    .option("initialPosition", "TRIM_HORIZON")
    .load()
)

# Parse the data
from pyspark.sql.functions import from_json, get_json_object, count, sum as spark_sum, col

parsed_df = (kinesis_df
    .selectExpr("CAST(data AS STRING) as json_data")
    .withColumn("event_type_id", get_json_object(col("json_data"), "$.event_type_id"))
    .withColumn("event_timestamp", get_json_object(col("json_data"), "$.event_timestamp"))
    .withColumn("partition_key", get_json_object(col("json_data"), "$.partition_key"))
    .withColumn("payload", get_json_object(col("json_data"), "$.serialized_payload"))
)

print("✅ Kinesis read stream configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Distribution Analysis
# MAGIC
# MAGIC Analyze the event type distribution to verify it matches the configured weights.

# COMMAND ----------

# Aggregate by event_type_id to get counts
distribution_df = (parsed_df
    .groupBy("event_type_id")
    .agg(count("*").alias("count"))
    .orderBy("event_type_id")
)

# Write to memory table for analysis
distribution_query = (distribution_df.writeStream
    .format("memory")
    .queryName("event_distribution")
    .outputMode("complete")
    .start()
)

print("✅ Distribution analysis started")
print("   Analyzing event distribution...")

# COMMAND ----------

# Let it run for 30 seconds to collect data
import time
time.sleep(30)

# Query the distribution
distribution_results = spark.sql("""
    SELECT 
        event_type_id,
        count,
        ROUND(count * 100.0 / SUM(count) OVER (), 2) as percentage
    FROM event_distribution
    ORDER BY count DESC
    LIMIT 20
""")

print("✅ Event Distribution (Top 20):")
distribution_results.show(20, False)

# Get total counts
total_counts = spark.sql("SELECT SUM(count) as total FROM event_distribution").collect()[0]['total']
print(f"\n✅ Total events processed: {total_counts:,}")

# Calculate expected vs actual distribution
expected_weight = 1.0 / len(config.data['event_types'])
print(f"✅ Expected percentage per event type: {expected_weight * 100:.4f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Stop Streams
# MAGIC
# MAGIC When done analyzing, stop all streams.

# COMMAND ----------

# Stop all queries
kinesis_query.stop()
distribution_query.stop()
print("✅ All streams stopped")

# COMMAND ----------

