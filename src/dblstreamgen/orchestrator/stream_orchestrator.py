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
    
    Uses wide schema approach to efficiently handle large numbers of event types.
    All fields are generated conditionally based on event_type_id.
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
        """Calculate rows per second for each event type based on weights (streaming mode only)."""
        if self.config.data['generation_mode'] != 'streaming':
            return {}
        
        total_rate = self.config.data['streaming_config']['total_rows_per_second']
        
        rates = {}
        for event_type in self.config.data['event_types']:
            event_id = event_type['event_type_id']
            weight = event_type['weight']
            rates[event_id] = total_rate * weight
        
        return rates
    
    def create_unified_stream(self) -> DataFrame:
        """
        Create unified stream with wide schema approach.
        
        Returns DataFrame with:
        - event_type_id: Event type identifier
        - event_timestamp: Event timestamp
        - partition_key: Partition key for routing
        - serialized_payload: JSON with event-specific fields (nulls excluded)
        """
        df = self._create_base_stream_with_types()
        df = self._add_common_fields(df)
        field_registry = self._build_field_registry()
        df = self._add_conditional_fields(df, field_registry)
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
        """Create base stream with event_type_id distribution."""
        if self.config.data['generation_mode'] == 'streaming':
            total_rate = self.config.data['streaming_config']['total_rows_per_second']
            partitions = self.builder._calculate_partitions_from_rate(total_rate)
            
            spec = (dg.DataGenerator(sparkSession=self.spark, name="base_stream", rows=total_rate)
                .withColumn("_id", "long", minValue=0, maxValue=1000000000, random=True))
            
            df = spec.build(withStreaming=True, 
                          options={'rowsPerSecond': int(total_rate), 'rampUpTimeSeconds': 0})
        else:
            total_rows = self.config.data['batch_config']['total_rows']
            partitions = self.config.data['batch_config'].get('partitions', 8)
            spec = (dg.DataGenerator(sparkSession=self.spark, name="base_batch", rows=total_rows, partitions=partitions)
                .withColumn("_id", "long", minValue=0, maxValue=1000000000, random=True))
            df = spec.build()
        
        return self._add_event_type_distribution(df)
    
    def _add_event_type_distribution(self, df: DataFrame) -> DataFrame:
        """Add event_type_id based on cumulative weight distribution using hash-based sampling."""
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
        """Build registry of all unique fields across event types."""
        registry = {}
        for event_type in self.config.data['event_types']:
            for field_name, field_spec in event_type.get('fields', {}).items():
                if field_name not in registry:
                    registry[field_name] = {'spec': field_spec, 'event_types': [event_type['event_type_id']]}
                else:
                    registry[field_name]['event_types'].append(event_type['event_type_id'])
        return registry
    
    def _add_conditional_fields(self, df: DataFrame, field_registry: Dict[str, Dict[str, Any]]) -> DataFrame:
        """Add event-specific fields with conditional population based on event_type_id."""
        for field_name, field_info in field_registry.items():
            field_spec = field_info['spec']
            event_types_using_field = field_info['event_types']
            
            field_expr = None
            for event_id in event_types_using_field:
                if field_expr is None:
                    field_expr = when(col("event_type_id") == event_id, self._generate_field_expression(field_spec))
                else:
                    field_expr = field_expr.when(col("event_type_id") == event_id, self._generate_field_expression(field_spec))
            
            df = df.withColumn(field_name, field_expr.otherwise(lit(None)))
        return df
    
    def _generate_field_expression(self, field_spec: Dict[str, Any]):
        """Generate Spark SQL expression for a field based on its type."""
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
            
            if weights:
                cumulative, string_expr = 0.0, None
                for value, weight in zip(values, weights):
                    cumulative += weight
                    string_expr = when(rand() < cumulative, lit(value)) if string_expr is None else string_expr.when(rand() < cumulative, lit(value))
                return string_expr.otherwise(lit(values[-1]))
            else:
                threshold_step, string_expr = 1.0 / len(values), None
                for i, value in enumerate(values):
                    threshold = (i + 1) * threshold_step
                    string_expr = when(rand() < threshold, lit(value)) if string_expr is None else string_expr.when(rand() < threshold, lit(value))
                return string_expr.otherwise(lit(values[-1]))
        
        elif field_type == 'timestamp':
            return current_timestamp()
        
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
    
    def _serialize_wide_schema(self, df: DataFrame) -> DataFrame:
        """Serialize wide schema to unified output format with JSON payload."""
        partition_key_field = self.config.data['sink_config'].get('partition_key_field', 'event_type_id')
        
        timestamp_field = 'event_timestamp' if 'event_timestamp' in df.columns else None
        if not timestamp_field:
            for field_name, field_spec in self.config.data.get('common_fields', {}).items():
                if field_spec.get('type') == 'timestamp' and field_name in df.columns:
                    timestamp_field = field_name
                    break
        if not timestamp_field:
            df = df.withColumn('event_timestamp', current_timestamp())
            timestamp_field = 'event_timestamp'
        
        if partition_key_field not in df.columns:
            raise ValueError(f"Partition key field '{partition_key_field}' not found in DataFrame")
        
        metadata_cols = ['event_type_id', timestamp_field, partition_key_field, '_id']
        payload_cols = [c for c in df.columns if c not in metadata_cols]
        
        df_serialized = df.withColumn("serialized_payload",
            to_json(struct(*[col(c) for c in payload_cols]), {"ignoreNullFields": "true"}))
        
        return df_serialized.select(col("event_type_id"), col(timestamp_field).alias("event_timestamp"),
                                   col(partition_key_field).alias("partition_key"), col("serialized_payload"))
