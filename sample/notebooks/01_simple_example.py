# Databricks notebook source
# MAGIC %md
# MAGIC # dblstreamgen v0.1.0 - Simple Streaming Example
# MAGIC
# MAGIC This notebook demonstrates basic usage of dblstreamgen v0.1.0 to generate
# MAGIC synthetic streaming data and write to AWS Kinesis using flat JSON schema.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks Runtime 15.4 LTS or above
# MAGIC - AWS credentials configured via Unity Catalog service credential
# MAGIC - Kinesis stream created in AWS
# MAGIC
# MAGIC ## Schema Structure
# MAGIC - Data is written as flat JSON to Kinesis Data field
# MAGIC - All event fields (event_name, event_key, event_timestamp, etc.) are at top level
# MAGIC - Compatible with customer bronze ingest pipeline (payload:field_name parsing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install dblstreamgen

# COMMAND ----------

# Install from Unity Catalog volume
%pip install --force-reinstall /Volumes/users/matthew_moorcroft/files/dblstreamgen-0.1.0-py3-none-any.whl

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
    "/Workspace/Users/matthew.moorcroft@databricks.com/sample/configs/stress_test_1500_events.yaml"
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
# serialize=True (default) creates (partition_key, data) format for Kinesis/Kafka
# Use serialize=False for Delta/Parquet/JSON to get wide schema with typed columns
unified_stream = orchestrator.create_unified_stream()

print("✅ Unified stream created")
print(f"   Is streaming: {unified_stream.isStreaming}")
print("   Format: Serialized (partition_key, data) for Kinesis")
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
# MAGIC Write the unified stream to AWS Kinesis.

# COMMAND ----------

checkpoint_location = "/tmp/dblstreamgen/checkpoints/kinesis"
dbutils.fs.rm(checkpoint_location, True)

sink_config = config.data['sink_config']
stream_name = sink_config['stream_name']
region = sink_config['region']
partition_key_field = sink_config.get('partition_key_field', 'event_key')
service_credential = sink_config.get('service_credential')

print(f"Writing to Kinesis: {stream_name} ({region})")
print(f"Using service credential: {service_credential}")

kinesis_query = unified_stream.writeStream \
    .format("dblstreamgen_kinesis") \
    .option("stream_name", stream_name) \
    .option("region", region) \
    .option("partition_key_field", partition_key_field) \
    .option("service_credential", service_credential) \
    .option("checkpointLocation", checkpoint_location) \
    .trigger(processingTime='1 second') \
    .start()

print(f"✅ Streaming started! Query ID: {kinesis_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Read from Kinesis and Parse Data

# COMMAND ----------

from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, 
    to_timestamp, unix_timestamp
)

print(f"Reading from Kinesis: {stream_name}")

kinesis_df = (spark.readStream
    .format("kinesis")
    .option("streamName", stream_name)
    .option("region", region)
    .option("initialPosition", "latest")
    .option("serviceCredential", service_credential)
    .load()
)

# Parse the flat JSON structure from the data field
# The data field now contains ALL fields in flat JSON (event_name, event_key, event_timestamp, etc.)
parsed_kinesis_df = (
    kinesis_df
    .withColumn("payload", col("data").cast("string"))
    .withColumn("event_name", get_json_object(col("payload"), "$.event_name"))
    .withColumn("event_key", get_json_object(col("payload"), "$.event_key"))
    .withColumn("event_timestamp_str", get_json_object(col("payload"), "$.event_timestamp"))
    .withColumn("event_id", get_json_object(col("payload"), "$.event_id"))
    .withColumn("event_timestamp", to_timestamp(col("event_timestamp_str")))
    .withColumn("read_timestamp", current_timestamp())
    .withColumn("e2e_latency_seconds", 
        unix_timestamp(col("read_timestamp")) - unix_timestamp(col("event_timestamp")))
    .withColumn("kinesis_latency_seconds",
        unix_timestamp(col("approximateArrivalTimestamp")) - unix_timestamp(col("event_timestamp")))
    .select(
        "event_name", "event_key", "event_id", "event_timestamp", "read_timestamp", 
        "e2e_latency_seconds", "kinesis_latency_seconds", 
        "payload", "approximateArrivalTimestamp"
    )
)

print("✅ Kinesis read stream configured with flat JSON parsing and E2E latency calculation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Event Distribution Analysis
# MAGIC
# MAGIC Display event type distribution with latency metrics in 10-second windows.

# COMMAND ----------

from pyspark.sql.functions import (
    count, avg, min as spark_min, max as spark_max, stddev, window
)

event_distribution_df = (
    parsed_kinesis_df
    .withWatermark("event_timestamp", "30 seconds")
    .groupBy(
        window(col("event_timestamp"), "10 seconds"),
        "event_name"
    )
    .agg(
        count("*").alias("event_count"),
        avg("e2e_latency_seconds").alias("avg_e2e_latency"),
        spark_min("e2e_latency_seconds").alias("min_e2e_latency"),
        spark_max("e2e_latency_seconds").alias("max_e2e_latency"),
        stddev("e2e_latency_seconds").alias("stddev_e2e_latency"),
        avg("kinesis_latency_seconds").alias("avg_kinesis_latency")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_name",
        "event_count",
        "avg_e2e_latency",
        "min_e2e_latency",
        "max_e2e_latency",
        "stddev_e2e_latency",
        "avg_kinesis_latency"
    )
    .orderBy("window_start", "event_name")
)

print("📊 Event distribution analysis ready (flat JSON schema)")

display(event_distribution_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Alternative - Write to Delta Lake
# MAGIC
# MAGIC For analytics use cases, write to Delta instead of Kinesis using wide schema format.

# COMMAND ----------

# Uncomment to try Delta Lake output:

# # Generate wide schema (NOT serialized) for Delta
# wide_stream = orchestrator.create_unified_stream(serialize=False)
# 
# print("✅ Wide schema stream created for Delta")
# print("\nSchema:")
# wide_stream.printSchema()
# 
# # Write to Delta table
# delta_checkpoint = "/tmp/dblstreamgen/checkpoints/delta"
# dbutils.fs.rm(delta_checkpoint, True)
# 
# delta_query = wide_stream.writeStream \
#     .format("delta") \
#     .outputMode("append") \
#     .option("checkpointLocation", delta_checkpoint) \
#     .partitionBy("event_name") \
#     .option("mergeSchema", "true") \
#     .option("optimizeWrite", "true") \
#     .table("users.matthew_moorcroft.streaming_events")
# 
# print(f"✅ Streaming to Delta! Query ID: {delta_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tip: Faker Integration (v0.3.0)
# MAGIC
# MAGIC Generate realistic data using Python Faker instead of fixed value lists.
# MAGIC Requires `pip install faker` (pre-installed on Databricks Runtime 13.3+).
# MAGIC
# MAGIC ```yaml
# MAGIC # In your YAML config, use faker on any string field:
# MAGIC common_fields:
# MAGIC   customer_name:
# MAGIC     type: string
# MAGIC     faker: "name"          # generates realistic full names
# MAGIC   contact_email:
# MAGIC     type: string
# MAGIC     faker: "ascii_company_email"
# MAGIC   tagline:
# MAGIC     type: string
# MAGIC     faker: "sentence"
# MAGIC     faker_args:
# MAGIC       nb_words: 8
# MAGIC ```
# MAGIC
# MAGIC See `sample/configs/faker_config.yaml` for a complete example.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Stop Streams
# MAGIC
# MAGIC When done, stop the write stream. The display query will stop automatically when you stop the cell.

# COMMAND ----------

# kinesis_query.stop()
# # delta_query.stop()  # If using Delta
# print("✅ Stream stopped")