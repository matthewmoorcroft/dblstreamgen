# Databricks notebook source
# MAGIC %md
# MAGIC # dblstreamgen - Simple Streaming Example
# MAGIC
# MAGIC Generates synthetic web-analytics events and writes them to AWS Kinesis
# MAGIC using the v0.4 `Scenario` API.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Databricks Runtime 15.4 LTS or above
# MAGIC - AWS credentials configured (Unity Catalog service credential recommended)
# MAGIC - Kinesis stream created in AWS
# MAGIC
# MAGIC ## Output schema
# MAGIC The config has `serialization.format: json`, so the stream produces
# MAGIC two columns: `partition_key` (string) and `data` (JSON string).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install dblstreamgen

# COMMAND ----------

# Install from a Unity Catalog volume (adjust path to your volume)
%pip install --force-reinstall /Volumes/<catalog>/<schema>/libraries/dblstreamgen-0.4.0-py3-none-any.whl

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import and verify

# COMMAND ----------

import dblstreamgen
from dblstreamgen import Config, Scenario, KinesisDataSource
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
print(f"dblstreamgen version: {dblstreamgen.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load configuration

# COMMAND ----------

# Load and validate the YAML config (raises ConfigurationError on invalid input)
config = Config.from_yaml(
    "/Workspace/Users/<user>/sample/configs/simple_config.yaml"
)

print(f"Config loaded: {config.source_name}")
print(f"Generation mode: {config.generation_mode}")
print(f"Event types: {len(config.event_types)}")
print(f"Scenario: {config.scenario}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Preview the execution plan (no data generated yet)

# COMMAND ----------

scenario = Scenario(spark, config)

# Dry-run: compiles the Spark query plan and prints it -- no data generated
scenario.dry_run()

# Human-readable breakdown of hidden columns and dedup groups
print(scenario.explain())

# Execution plan (streaming only)
for plan in scenario.plan():
    print(plan)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Register the Kinesis data source

# COMMAND ----------

spark.dataSource.register(KinesisDataSource)
print("Registered 'dblstreamgen_kinesis' data source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Define the sink factory
# MAGIC
# MAGIC `Scenario.run()` accepts a `sink_factory: Callable[[DataFrame, str], StreamingQuery]`.
# MAGIC The factory receives the DataFrame and a checkpoint path and returns a running query.

# COMMAND ----------

STREAM_NAME = "web-events-stream"
REGION = "us-east-1"
SERVICE_CREDENTIAL = "my-kinesis-credential"  # Unity Catalog service credential name


def kinesis_sink(df, checkpoint_path):
    """Write a serialized (partition_key, data) DataFrame to Kinesis."""
    return (
        df.writeStream
        .format("dblstreamgen_kinesis")
        .option("stream_name", STREAM_NAME)
        .option("region", REGION)
        .option("service_credential", SERVICE_CREDENTIAL)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 second")
        .start()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Run the scenario
# MAGIC
# MAGIC `run()` blocks for `scenario.duration_seconds` (300 s in the config), then stops
# MAGIC all queries automatically. A spike is fired at t=60s if configured.

# COMMAND ----------

CHECKPOINT_BASE = "/tmp/dblstreamgen/checkpoints/simple_example"
dbutils.fs.rm(CHECKPOINT_BASE, True)

result = scenario.run(kinesis_sink, CHECKPOINT_BASE)
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Build a DataFrame without running
# MAGIC
# MAGIC Use `Scenario.build()` when you want the DataFrame itself -- for inspection,
# MAGIC writing to Delta, or custom sink logic outside the runner.

# COMMAND ----------

# Build returns a streaming DataFrame (config has serialization, so output is
# narrow: partition_key + data columns).
df = scenario.build()

print(f"Is streaming: {df.isStreaming}")
df.printSchema()

# Write to Delta for analytics (wide schema -- pass serialize=False to skip serialization)
wide_df = scenario.build(serialize=False)

delta_checkpoint = "/tmp/dblstreamgen/checkpoints/delta"
dbutils.fs.rm(delta_checkpoint, True)

delta_query = (
    wide_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", delta_checkpoint)
    .option("mergeSchema", "true")
    .table("<catalog>.<schema>.streaming_events")
)

print(f"Delta query started: {delta_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Read back from Kinesis and parse events

# COMMAND ----------

from pyspark.sql.functions import (
    col, get_json_object, current_timestamp, to_timestamp, unix_timestamp, window,
    count, avg, min as spark_min, max as spark_max,
)

kinesis_df = (
    spark.readStream
    .format("kinesis")
    .option("streamName", STREAM_NAME)
    .option("region", REGION)
    .option("initialPosition", "latest")
    .option("serviceCredential", SERVICE_CREDENTIAL)
    .load()
)

parsed_df = (
    kinesis_df
    .withColumn("payload", col("data").cast("string"))
    .withColumn("event_name",      get_json_object(col("payload"), "$.event_name"))
    .withColumn("event_key",       get_json_object(col("payload"), "$.event_key"))
    .withColumn("event_timestamp", to_timestamp(get_json_object(col("payload"), "$.event_timestamp")))
    .withColumn("event_id",        get_json_object(col("payload"), "$.event_id"))
    .withColumn("read_timestamp",  current_timestamp())
    .withColumn("e2e_latency_seconds",
        unix_timestamp(col("read_timestamp")) - unix_timestamp(col("event_timestamp")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Event distribution in 10-second windows

# COMMAND ----------

distribution_df = (
    parsed_df
    .withWatermark("event_timestamp", "30 seconds")
    .groupBy(window(col("event_timestamp"), "10 seconds"), "event_name")
    .agg(
        count("*").alias("event_count"),
        avg("e2e_latency_seconds").alias("avg_e2e_latency_s"),
        spark_min("e2e_latency_seconds").alias("min_e2e_latency_s"),
        spark_max("e2e_latency_seconds").alias("max_e2e_latency_s"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_name", "event_count",
        "avg_e2e_latency_s", "min_e2e_latency_s", "max_e2e_latency_s",
    )
    .orderBy("window_start", "event_name")
)

display(distribution_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Stop all streams

# COMMAND ----------

# delta_query.stop()
# print("Streams stopped")
