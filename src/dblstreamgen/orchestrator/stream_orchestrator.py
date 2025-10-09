"""Stream orchestrator for managing multiple event type streams."""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, hash, current_timestamp, struct, to_json, expr, rand
from typing import Dict, Any, List, Tuple
import dbldatagen as dg

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
        Create unified stream with standardized output schema.
        
        "Unified" refers to the output schema, not the implementation:
        - All event types share the same 4-column output format
        - Event-specific data is serialized to JSON in the payload column
        
        Implementation uses wide schema approach (NOT unions):
        1. Creates single wide DataFrame with all possible fields
        2. Populates fields conditionally based on event_type_id
        3. Serializes to unified output: (event_type_id, event_timestamp, partition_key, payload)
        
        This avoids deep union trees and scales efficiently to thousands of event types.
        
        Returns:
            DataFrame with unified output schema:
            - event_type_id (string): Event type identifier
            - event_timestamp (timestamp): Event timestamp
            - partition_key (string): Partition key for routing
            - serialized_payload (string): JSON payload with event-specific fields
        
        Example:
            >>> unified_stream = orchestrator.create_unified_stream()
            >>> unified_stream.printSchema()
            root
             |-- event_type_id: string
             |-- event_timestamp: timestamp
             |-- partition_key: string
             |-- serialized_payload: string
        """
        # 1. Create base stream with event_type_id distribution
        df = self._create_base_stream_with_types()
        
        # 2. Add common fields (present in all events)
        df = self._add_common_fields(df)
        
        # 3. Build field registry (unique fields across all event types)
        field_registry = self._build_field_registry()
        
        # 4. Add all event-specific fields conditionally
        df = self._add_conditional_fields(df, field_registry)
        
        # 5. Serialize to JSON with unified schema
        df = self._serialize_wide_schema(df)
        
        rates = self.calculate_rates()
        if rates:
            total_rate = sum(rates.values())
            logger.info(f"Wide schema stream created: {len(self.config.data['event_types'])} event types, "
                       f"{len(field_registry)} unique fields, target {total_rate:,.0f} rows/sec")
        else:
            logger.info(f"Wide schema stream created: {len(self.config.data['event_types'])} event types, "
                       f"{len(field_registry)} unique fields")
        
        return df
    
    def _create_base_stream_with_types(self) -> DataFrame:
        """
        Create base stream and assign event_type_id using hash-based stratified sampling.
        
        Uses dbldatagen to create the base rate stream (same as old approach).
        
        Returns:
            DataFrame with event_type_id column
        """
        if self.config.data['generation_mode'] == 'streaming':
            total_rate = self.config.data['streaming_config']['total_rows_per_second']
            partitions = self.builder._calculate_partitions_from_rate(total_rate)
            
            # Create base spec using dbldatagen (same as old approach)
            spec = (dg.DataGenerator(sparkSession=self.spark, name="base_stream", rows=total_rate)
                .withColumn("_id", "long", minValue=0, maxValue=1000000000, random=True))
            
            # Build as streaming DataFrame with 2-minute ramp-up
            df = spec.build(
                withStreaming=True,
                options={
                    'rowsPerSecond': int(total_rate),
                    'rampUpTimeSeconds': 0  # Gradually ramp up over 2 minutes
                }
            )
        else:
            # Batch mode
            total_rows = self.config.data['batch_config']['total_rows']
            partitions = self.config.data['batch_config'].get('partitions', 8)
            
            # Use dbldatagen for batch too
            spec = (dg.DataGenerator(sparkSession=self.spark, name="base_batch", rows=total_rows, partitions=partitions)
                .withColumn("_id", "long", minValue=0, maxValue=1000000000, random=True))
            
            df = spec.build()
        
        # Add event_type_id using cumulative weight distribution
        df = self._add_event_type_distribution(df)
        
        return df
    
    def _add_event_type_distribution(self, df: DataFrame) -> DataFrame:
        """
        Add event_type_id based on cumulative weight distribution.
        
        Uses hash-based stratified sampling to distribute event types according
        to their weights. Hash provides uniform distribution and determinism.
        """
        cumulative = 0.0
        type_expr = None
        
        for event_type in self.config.data['event_types']:
            event_id = event_type['event_type_id']
            weight = event_type['weight']
            
            cumulative += weight
            threshold = int(cumulative * 10000)  # Scale to 0-10000 for precision
            
            if type_expr is None:
                type_expr = when((hash(col("_id")) % 10000) < threshold, lit(event_id))
            else:
                type_expr = type_expr.when((hash(col("_id")) % 10000) < threshold, lit(event_id))
        
        return df.withColumn("event_type_id", type_expr)
    
    def _add_common_fields(self, df: DataFrame) -> DataFrame:
        """Add common fields that appear in all events."""
        common_fields = self.config.data.get('common_fields', {})
        
        for field_name, field_spec in common_fields.items():
            df = df.withColumn(field_name, self._generate_field_expression(field_spec))
        
        return df
    
    def _build_field_registry(self) -> Dict[str, Dict[str, Any]]:
        """
        Build registry of all unique fields across event types.
        
        Returns:
            Dict mapping field_name to field_spec
        """
        registry = {}
        
        for event_type in self.config.data['event_types']:
            for field_name, field_spec in event_type.get('fields', {}).items():
                if field_name not in registry:
                    # Store field spec and which event types use it
                    registry[field_name] = {
                        'spec': field_spec,
                        'event_types': [event_type['event_type_id']]
                    }
                else:
                    registry[field_name]['event_types'].append(event_type['event_type_id'])
        
        return registry
    
    def _add_conditional_fields(self, df: DataFrame, field_registry: Dict[str, Dict[str, Any]]) -> DataFrame:
        """
        Add all event-specific fields with conditional population.
        
        Each field is only populated when event_type_id matches one of the
        event types that uses this field.
        """
        for field_name, field_info in field_registry.items():
            field_spec = field_info['spec']
            event_types_using_field = field_info['event_types']
            
            # Build conditional expression
            field_expr = None
            for event_id in event_types_using_field:
                if field_expr is None:
                    field_expr = when(
                        col("event_type_id") == event_id,
                        self._generate_field_expression(field_spec)
                    )
                else:
                    field_expr = field_expr.when(
                        col("event_type_id") == event_id,
                        self._generate_field_expression(field_spec)
                    )
            
            # Default to null for event types that don't use this field
            field_expr = field_expr.otherwise(lit(None))
            
            df = df.withColumn(field_name, field_expr)
        
        return df
    
    def _generate_field_expression(self, field_spec: Dict[str, Any]):
        """
        Generate Spark SQL expression for a field based on its type.
        
        Returns:
            Spark Column expression
        """
        field_type = field_spec.get('type')
        
        if field_type == 'uuid':
            return expr("uuid()")
        
        elif field_type == 'int':
            min_val = field_spec.get('range', [0, 100])[0]
            max_val = field_spec.get('range', [0, 100])[1]
            return (rand() * (max_val - min_val) + min_val).cast('int')
        
        elif field_type == 'float':
            min_val = field_spec.get('range', [0.0, 100.0])[0]
            max_val = field_spec.get('range', [0.0, 100.0])[1]
            return (rand() * (max_val - min_val) + min_val).cast('float')
        
        elif field_type == 'string':
            values = field_spec.get('values', ['value'])
            weights = field_spec.get('weights')
            
            # Build weighted case statement
            if weights:
                cumulative = 0.0
                string_expr = None
                
                for i, (value, weight) in enumerate(zip(values, weights)):
                    cumulative += weight
                    threshold = cumulative
                    
                    if string_expr is None:
                        string_expr = when(rand() < threshold, lit(value))
                    else:
                        string_expr = string_expr.when(rand() < threshold, lit(value))
                
                return string_expr.otherwise(lit(values[-1]))
            else:
                # Uniform distribution
                threshold_step = 1.0 / len(values)
                string_expr = None
                
                for i, value in enumerate(values):
                    threshold = (i + 1) * threshold_step
                    
                    if string_expr is None:
                        string_expr = when(rand() < threshold, lit(value))
                    else:
                        string_expr = string_expr.when(rand() < threshold, lit(value))
                
                return string_expr.otherwise(lit(values[-1]))
        
        elif field_type == 'timestamp':
            return current_timestamp()
        
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
    
    def _serialize_wide_schema(self, df: DataFrame) -> DataFrame:
        """
        Serialize wide schema to unified output format.
        
        Returns DataFrame with:
        - event_type_id
        - event_timestamp (or first timestamp field found)
        - partition_key
        - serialized_payload (JSON with null fields excluded)
        """
        # Get partition key field from config
        partition_key_field = self.config.data['sink_config'].get('partition_key_field', 'event_type_id')
        
        # Find timestamp field (event_timestamp or first timestamp in common_fields)
        timestamp_field = None
        if 'event_timestamp' in df.columns:
            timestamp_field = 'event_timestamp'
        else:
            # Look for first timestamp in common fields
            for field_name, field_spec in self.config.data.get('common_fields', {}).items():
                if field_spec.get('type') == 'timestamp' and field_name in df.columns:
                    timestamp_field = field_name
                    break
        
        if not timestamp_field:
            # Add timestamp if not present
            df = df.withColumn('event_timestamp', current_timestamp())
            timestamp_field = 'event_timestamp'
        
        # Verify partition key exists
        if partition_key_field not in df.columns:
            raise ValueError(
                f"Partition key field '{partition_key_field}' not found in DataFrame. "
                f"Available columns: {df.columns}"
            )
        
        # Get metadata columns to exclude from payload
        metadata_cols = ['event_type_id', timestamp_field, partition_key_field, '_id']
        
        # All other columns go into payload
        payload_cols = [c for c in df.columns if c not in metadata_cols]
        
        # Serialize to JSON with null filtering
        df_serialized = df.withColumn(
            "serialized_payload",
            to_json(struct(*[col(c) for c in payload_cols]), {"ignoreNullFields": "true"})
        )
        
        # Return unified schema
        return df_serialized.select(
            col("event_type_id"),
            col(timestamp_field).alias("event_timestamp"),
            col(partition_key_field).alias("partition_key"),
            col("serialized_payload")
        )
