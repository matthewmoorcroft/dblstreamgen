"""Wide schema -> narrow format serialization.

Config-driven with runtime override.  Not sink-specific.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, to_json

logger = logging.getLogger(__name__)


def serialize_to_json(df: DataFrame, partition_key_field: str) -> DataFrame:
    """Serialize wide DataFrame to (partition_key, data) with JSON payload.

    Conditional null fields are excluded from the JSON via ``ignoreNullFields``.
    """
    payload_cols = [c for c in df.columns if c != partition_key_field]
    return df.select(
        col(partition_key_field).alias("partition_key"),
        to_json(struct(*payload_cols), {"ignoreNullFields": "true"}).alias("data"),
    )


def serialize_to_avro(df: DataFrame, partition_key_field: str) -> DataFrame:
    """Serialize wide DataFrame to (partition_key, data) with Avro payload.

    Requires ``spark-avro`` (bundled in Databricks Runtime).
    """
    try:
        from pyspark.sql.avro.functions import to_avro
    except ImportError:
        raise ImportError(
            "Avro serialization requires spark-avro.  "
            "On Databricks Runtime it is pre-installed.  "
            "Locally: pip install pyspark[sql] or add spark-avro to your classpath."
        )
    payload_cols = [c for c in df.columns if c != partition_key_field]
    return df.select(
        col(partition_key_field).alias("partition_key"),
        to_avro(struct(*payload_cols)).alias("data"),
    )


def serialize_dataframe(df: DataFrame, serialization_config: dict) -> DataFrame:
    """Config-driven dispatch to the correct serialization format."""
    fmt = serialization_config.get("format", "json")
    pk_field = serialization_config["partition_key_field"]

    if fmt == "json":
        return serialize_to_json(df, pk_field)
    elif fmt == "avro":
        return serialize_to_avro(df, pk_field)
    else:
        raise ValueError(f"Unknown serialization format: {fmt!r}")
