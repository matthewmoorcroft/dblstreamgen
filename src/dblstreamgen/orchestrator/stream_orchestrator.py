"""Stream orchestrator for managing multiple event type streams."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict, List

from dblstreamgen.config import Config
from dblstreamgen.builder.spec_builder import DataGeneratorBuilder
from dblstreamgen.orchestrator.serialization import serialize_to_json, get_payload_columns


class StreamOrchestrator:
    """
    Orchestrates multiple event type streams into a unified stream.
    
    Responsibilities:
    - Calculate rates for each event type based on weights
    - Generate separate DataFrames for each event type
    - Serialize payloads to resolve schema conflicts
    - Union all event types into single unified stream
    
    Example:
        >>> orchestrator = StreamOrchestrator(spark, config)
        >>> unified_stream = orchestrator.create_unified_stream()
        >>> unified_stream.isStreaming
        True
    """
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Validated configuration
        """
        self.spark = spark
        self.config = config
        self.builder = DataGeneratorBuilder(spark, config)
    
    def calculate_rates(self) -> Dict[str, float]:
        """
        Calculate rows per second for each event type based on weights.
        
        Only applicable for streaming mode.
        
        Returns:
            Dictionary mapping event_type_id to calculated rate
            
        Example:
            >>> rates = orchestrator.calculate_rates()
            >>> rates
            {'user.click': 500.0, 'user.purchase': 300.0, 'user.view': 200.0}
        """
        if self.config.data['generation_mode'] != 'streaming':
            return {}
        
        total_rate = self.config.data['streaming_config']['total_rows_per_second']
        
        rates = {}
        for event_type in self.config.data['event_types']:
            event_id = event_type['event_type_id']
            weight = event_type['weight']
            rates[event_id] = total_rate * weight
        
        return rates
    
    def generate_event_streams(self) -> Dict[str, DataFrame]:
        """
        Generate separate DataFrame for each event type.
        
        Returns:
            Dictionary mapping event_type_id to DataFrame
            
        Example:
            >>> streams = orchestrator.generate_event_streams()
            >>> len(streams)
            3
        """
        return self.builder.build_all_dataframes()
    
    def serialize_stream(self, df: DataFrame) -> DataFrame:
        """
        Transform DataFrame to unified schema with serialized payload.
        
        Unified schema:
        - event_type_id (string)
        - event_timestamp (timestamp)
        - partition_key (from configurable field)
        - serialized_payload (string, JSON)
        
        Args:
            df: Input DataFrame with event-specific schema
            
        Returns:
            DataFrame with unified schema
            
        Example:
            >>> serialized_df = orchestrator.serialize_stream(df)
            >>> serialized_df.columns
            ['event_type_id', 'event_timestamp', 'partition_key', 'serialized_payload']
        """
        # Get partition key field from config
        partition_key_field = self.config.data['sink_config'].get('partition_key_field', 'event_type_id')
        
        # Verify partition key field exists
        if partition_key_field not in df.columns:
            raise ValueError(
                f"Partition key field '{partition_key_field}' not found in DataFrame. "
                f"Available columns: {df.columns}"
            )
        
        # Define columns to exclude from payload
        exclude_cols = ['event_type_id', 'event_timestamp', partition_key_field]
        
        # Get payload columns
        payload_cols = get_payload_columns(df, exclude_cols)
        
        # Serialize payload
        df_with_payload = serialize_to_json(df, payload_cols, 'serialized_payload')
        
        # Select unified schema
        return df_with_payload.select(
            col('event_type_id'),
            col('event_timestamp'),
            col(partition_key_field).alias('partition_key'),
            col('serialized_payload')
        )
    
    def create_unified_stream(self) -> DataFrame:
        """
        Create unified stream from all event types.
        
        Process:
        1. Generate DataFrames for each event type
        2. Serialize each to unified schema
        3. Union all into single stream
        
        Returns:
            Single DataFrame containing all event types
            
        Example:
            >>> unified = orchestrator.create_unified_stream()
            >>> unified.printSchema()
            root
             |-- event_type_id: string
             |-- event_timestamp: timestamp
             |-- partition_key: string
             |-- serialized_payload: string
        """
        # Generate streams for each event type
        streams = self.generate_event_streams()
        
        if not streams:
            raise ValueError("No event types defined in configuration")
        
        # Serialize each stream to unified schema
        serialized_streams = []
        for event_id, df in streams.items():
            try:
                serialized_df = self.serialize_stream(df)
                serialized_streams.append(serialized_df)
            except Exception as e:
                raise RuntimeError(f"Error serializing stream for '{event_id}': {e}")
        
        # Union all streams
        unified = serialized_streams[0]
        for stream in serialized_streams[1:]:
            unified = unified.union(stream)
        
        return unified
