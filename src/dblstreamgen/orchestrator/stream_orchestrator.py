"""Stream orchestrator for managing multiple event type streams."""

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, hash, current_timestamp, struct, to_json
from typing import Dict, Any
import dbldatagen as dg

from dblstreamgen.config import Config

logger = logging.getLogger(__name__)


class StreamOrchestrator:
    """Orchestrates multiple event type streams into a unified stream using wide schema approach."""
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Validated configuration
        """
        self.spark = spark
        self.config = config
    
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
    
    def create_unified_stream(self, serialize: bool = True) -> DataFrame:
        """
        Create unified stream with all fields generated via dbldatagen.
        
        Args:
            serialize: If True, returns (partition_key, data) format for Kinesis/Kafka.
                      If False, returns wide schema with typed columns for Delta/Parquet/JSON/CSV.
                      Default: True (backward compatible)
        
        Returns:
            DataFrame - Format depends on serialize parameter:
                - serialize=True: Two columns (partition_key, data) with JSON payload
                - serialize=False: Wide schema with all typed columns
        
        Examples:
            >>> # For Kinesis/Kafka (message-based sinks)
            >>> stream = orchestrator.create_unified_stream(serialize=True)
            >>> stream.printSchema()
            root
             |-- partition_key: string
             |-- data: string (JSON)
            
            >>> # For Delta/Parquet/JSON/CSV (file/table-based sinks)
            >>> stream = orchestrator.create_unified_stream(serialize=False)
            >>> stream.printSchema()
            root
             |-- event_name: string
             |-- event_id: string
             |-- event_timestamp: timestamp
             |-- user_id: string
             |-- ... (all fields as typed columns)
        """
        spec = self._build_complete_spec()
        df = self._build_dataframe_from_spec(spec)
        
        # Only serialize if requested (for Kinesis/Kafka)
        if serialize:
            df = self._serialize_wide_schema(df)
        
        # Log creation with format type
        rates = self.calculate_rates()
        field_registry = self._build_field_registry()
        format_desc = "serialized (partition_key, data)" if serialize else "wide schema"
        
        if rates:
            total_rate = sum(rates.values())
            logger.info(f"Stream created ({format_desc}): {len(self.config.data['event_types'])} event types, "
                       f"{len(field_registry)} unique fields, target {total_rate:,.0f} rows/sec")
        else:
            logger.info(f"Stream created ({format_desc}): {len(self.config.data['event_types'])} event types, "
                       f"{len(field_registry)} unique fields")
        
        return df
    
    def _get_event_type_field_name(self) -> str:
        """
        Get the event type field name from configuration.
        
        Returns:
            'event_name' if defined in common_fields, otherwise 'event_type_id'
        """
        return 'event_name' if 'event_name' in self.config.data.get('common_fields', {}) else 'event_type_id'
    
    def _build_complete_spec(self) -> dg.DataGenerator:
        """Build complete dbldatagen spec with all fields."""
        spec = self._create_base_spec()
        spec = self._add_event_type_id_to_spec(spec)
        spec = self._add_common_fields_to_spec(spec)
        field_registry = self._build_field_registry()
        spec = self._add_conditional_fields_to_spec(spec, field_registry)
        return spec
    
    def _create_base_spec(self) -> dg.DataGenerator:
        """Initialize dbldatagen spec with internal _id column for uniqueness."""
        if self.config.data['generation_mode'] == 'streaming':
            total_rate = self.config.data['streaming_config']['total_rows_per_second']
            spec = dg.DataGenerator(sparkSession=self.spark, name="base_stream", rows=total_rate)
        else:
            total_rows = self.config.data['batch_config']['total_rows']
            partitions = self.config.data['batch_config'].get('partitions', 8)
            spec = dg.DataGenerator(sparkSession=self.spark, name="base_batch", 
                                   rows=total_rows, partitions=partitions)
        
        return spec.withColumn("_id", "long", minValue=0, maxValue=1000000000, random=True)
    
    def _add_event_type_id_to_spec(self, spec) -> dg.DataGenerator:
        """Add event type column with weighted distribution using dbldatagen."""
        event_ids = [et['event_type_id'] for et in self.config.data['event_types']]
        weights = [et['weight'] for et in self.config.data['event_types']]
        event_type_field = self._get_event_type_field_name()
        
        return spec.withColumn(event_type_field, "string", values=event_ids, weights=weights, random=True)
    
    def _build_dataframe_from_spec(self, spec) -> DataFrame:
        """Build DataFrame from complete spec."""
        if self.config.data['generation_mode'] == 'streaming':
            total_rate = self.config.data['streaming_config']['total_rows_per_second']
            return spec.build(withStreaming=True, 
                            options={'rowsPerSecond': int(total_rate), 'rampUpTimeSeconds': 0})
        else:
            return spec.build()
    
    def _add_common_fields_to_spec(self, spec) -> dg.DataGenerator:
        """Add common fields using dbldatagen expr."""
        common_fields = self.config.data.get('common_fields', {})
        event_type_field = self._get_event_type_field_name()
        
        for field_name, field_spec in common_fields.items():
            # Skip event type field (already added by _add_event_type_id_to_spec)
            if field_name == event_type_field:
                continue
            sql_expr = self._generate_sql_expression(field_spec)
            field_type = self._get_spark_type(field_spec)
            spec = spec.withColumn(field_name, field_type, expr=sql_expr)
        return spec
    
    def _add_conditional_fields_to_spec(self, spec, field_registry: Dict) -> dg.DataGenerator:
        """Add conditional fields using SQL CASE with IN clause."""
        event_type_field = self._get_event_type_field_name()
        common_field_names = set(self.config.data.get('common_fields', {}).keys())
        
        for field_name, field_info in field_registry.items():
            # Skip fields that are already added as common fields
            if field_name in common_field_names:
                logger.debug(f"Skipping conditional field '{field_name}' (already in common_fields)")
                continue
            
            field_spec = field_info['spec']
            event_types = field_info['event_types']
            
            field_sql = self._generate_sql_expression(field_spec)
            
            if len(event_types) == 1:
                sql_expr = f"CASE WHEN {event_type_field} = '{event_types[0]}' THEN {field_sql} ELSE NULL END"
            else:
                event_list = "', '".join(event_types)
                sql_expr = f"CASE WHEN {event_type_field} IN ('{event_list}') THEN {field_sql} ELSE NULL END"
            
            field_type = self._get_spark_type(field_spec)
            spec = spec.withColumn(field_name, field_type, expr=sql_expr, baseColumn=event_type_field)
        
        return spec
    
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
    
    def _generate_sql_expression(self, field_spec: Dict[str, Any]) -> str:
        """
        Generate SQL expression for field generation.
        
        Supports: uuid(), CAST(rand()...) for int/float, CASE/WHEN for strings, current_timestamp()
        
        Args:
            field_spec: Field specification with type and parameters
            
        Returns:
            SQL expression string
        """
        field_type = field_spec.get('type')
        
        if field_type == 'uuid':
            return "uuid()"
        
        elif field_type == 'int':
            min_val = field_spec.get('range', [0, 100])[0]
            max_val = field_spec.get('range', [0, 100])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS INT)"
        
        elif field_type == 'float':
            min_val = field_spec.get('range', [0.0, 100.0])[0]
            max_val = field_spec.get('range', [0.0, 100.0])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS FLOAT)"
        
        elif field_type == 'string':
            values = field_spec.get('values', ['value'])
            weights = field_spec.get('weights')
            
            if weights:
                cumulative = 0.0
                sql_expr = "CASE "
                for value, weight in zip(values, weights):
                    cumulative += weight
                    sql_expr += f"WHEN rand() < {cumulative} THEN '{value}' "
                sql_expr += f"ELSE '{values[-1]}' END"
                return sql_expr
            else:
                threshold = 1.0 / len(values)
                sql_expr = "CASE "
                for i, value in enumerate(values):
                    sql_expr += f"WHEN rand() < {(i+1) * threshold} THEN '{value}' "
                sql_expr += f"ELSE '{values[-1]}' END"
                return sql_expr
        
        elif field_type == 'timestamp':
            return "current_timestamp()"
        
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
    
    def _get_spark_type(self, field_spec: Dict[str, Any]) -> str:
        """Map field type to Spark SQL type string."""
        type_map = {
            'uuid': 'string',
            'int': 'int', 
            'float': 'float',
            'string': 'string',
            'timestamp': 'timestamp'
        }
        return type_map.get(field_spec.get('type'), 'string')
    
    def _serialize_wide_schema(self, df: DataFrame) -> DataFrame:
        """
        Serialize wide schema to flat JSON for Kinesis.
        
        Creates output with:
        - partition_key: Top-level column for Kinesis routing
        - data: Flat JSON with ALL fields (goes into Kinesis Data field)
        """
        # Default partition key is the event type field name
        default_partition_key = self._get_event_type_field_name()
        partition_key_field = self.config.data['sink_config'].get('partition_key_field', default_partition_key)
        
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
        
        # Create flat JSON with all fields except internal _id
        payload_cols = [c for c in df.columns if c != '_id']
        
        df_serialized = df.withColumn("data",
            to_json(struct(*[col(c) for c in payload_cols]), {"ignoreNullFields": "true"}))
        
        # Return partition_key for routing and data for Kinesis Data field
        return df_serialized.select(
            col(partition_key_field).alias("partition_key"),
            col("data")
        )
