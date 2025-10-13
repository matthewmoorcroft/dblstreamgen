"""Serialization utilities for DataFrame payloads."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct, col
from pyspark.sql.types import StringType
from typing import List


def get_payload_columns(df: DataFrame, exclude_cols: List[str]) -> List[str]:
    """
    Get DataFrame columns excluding specified columns.
    
    Typically used to get payload columns by excluding metadata columns.
    
    Args:
        df: Input DataFrame
        exclude_cols: List of column names to exclude
        
    Returns:
        List of column names to include in payload
        
    Example:
        >>> payload_cols = get_payload_columns(
        ...     df, 
        ...     ['event_type_id', 'event_timestamp', 'user_id']
        ... )
        >>> print(payload_cols)
        ['session_id', 'device_type', 'action']
    """
    return [c for c in df.columns if c not in exclude_cols]


def serialize_to_json(df: DataFrame, payload_cols: List[str], alias: str = 'serialized_payload') -> DataFrame:
    """
    Serialize specified columns to JSON string.
    
    Creates a new column containing JSON-serialized payload from the specified columns.
    Uses Spark's native to_json function for efficiency.
    
    Args:
        df: Input DataFrame
        payload_cols: List of column names to serialize
        alias: Name for the serialized column (default: 'serialized_payload')
        
    Returns:
        DataFrame with added serialized_payload column
        
    Example:
        >>> df_serialized = serialize_to_json(
        ...     df,
        ...     ['session_id', 'device_type', 'action']
        ... )
        >>> df_serialized.select('serialized_payload').show(1, False)
        +---------------------------------------------------+
        |serialized_payload                                 |
        +---------------------------------------------------+
        |{"session_id":"abc","device_type":"iOS","action":"click"}|
        +---------------------------------------------------+
    """
    if not payload_cols:
        # No payload columns, return empty JSON object
        return df.withColumn(alias, to_json(struct()))
    
    # Create struct from payload columns and serialize to JSON
    json_col = to_json(struct(*[col(c) for c in payload_cols]))
    
    return df.withColumn(alias, json_col.cast(StringType()))
