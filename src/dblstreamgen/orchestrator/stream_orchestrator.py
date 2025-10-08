"""Stream orchestrator for managing multiple event type streams."""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Dict

from dblstreamgen.config import Config
from dblstreamgen.builder.spec_builder import DataGeneratorBuilder
from dblstreamgen.orchestrator.serialization import serialize_to_json, get_payload_columns

logger = logging.getLogger(__name__)


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
    
    def limit_stream_rate(self, df: DataFrame, event_type_id: str, 
                          target_rows_per_second: float) -> DataFrame:
        """
        Apply rate limiting to prevent backlog accumulation during spin-up and delays.
        
        Uses 2x hard limit on batch size to handle spin-up periods while maintaining
        manageable batch sizes during normal operation.
        
        Args:
            df: Input streaming DataFrame
            event_type_id: Event type identifier
            target_rows_per_second: Target rate for this event type
            
        Returns:
            Rate-limited DataFrame
        """
        max_rows_per_batch = int(target_rows_per_second * 2)
        limited_df = df.limit(max_rows_per_batch)
        
        logger.info(f"Event '{event_type_id}': rate limit {target_rows_per_second:,.0f} rows/sec "
                   f"(max {max_rows_per_batch:,} per batch)")
        
        return limited_df
    
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
        Create unified stream from all event types with rate limiting.
        
        Generates separate DataFrames for each event type, applies rate limiting
        to prevent backlog accumulation, serializes to unified schema, and unions
        all streams together.
        
        Returns:
            Single DataFrame with unified schema containing all event types
        """
        streams = self.generate_event_streams()
        
        if not streams:
            raise ValueError("No event types defined in configuration")
        
        rates = self.calculate_rates()
        serialized_streams = []
        
        for event_id, df in streams.items():
            try:
                # Rate limiting disabled - was causing empty batches
                # TODO: Re-implement with streaming-compatible approach
                # if self.config.data['generation_mode'] == 'streaming':
                #     target_rate = rates.get(event_id, 0)
                #     limited_df = self.limit_stream_rate(df, event_id, target_rate)
                # else:
                #     limited_df = df
                
                limited_df = df
                
                serialized_df = self.serialize_stream(limited_df)
                serialized_streams.append(serialized_df)
            except Exception as e:
                raise RuntimeError(f"Error processing stream for '{event_id}': {e}")
        
        unified = serialized_streams[0]
        for stream in serialized_streams[1:]:
            unified = unified.union(stream)
        
        if rates:
            total_rate = sum(rates.values())
            logger.info(f"Unified stream created: {len(serialized_streams)} event types, "
                       f"target {total_rate:,.0f} rows/sec (rate limiting disabled)")
        else:
            logger.info(f"Unified stream created: {len(serialized_streams)} event types")
        
        return unified
