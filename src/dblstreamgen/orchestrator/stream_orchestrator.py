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
        """Add common fields using dbldatagen expr, supporting nested types."""
        common_fields = self.config.data.get('common_fields', {})
        event_type_field = self._get_event_type_field_name()
        
        for field_name, field_spec in common_fields.items():
            # Skip event type field (already added by _add_event_type_id_to_spec)
            if field_name == event_type_field:
                continue
            
            field_type_name = field_spec.get('type')
            
            # Handle nested types in common fields
            if field_type_name == 'array':
                spec, array_expr, generated_cols = self._process_array_field(
                    spec, field_name, field_spec, event_type_field, []
                )
                field_type = self._get_spark_type(field_spec)
                spec = spec.withColumn(field_name, field_type, expr=array_expr, baseColumn=generated_cols)
            
            elif field_type_name == 'struct':
                spec, struct_expr, generated_cols = self._process_struct_field(
                    spec, field_name, field_spec, event_type_field, []
                )
                field_type = self._get_spark_type(field_spec)
                spec = spec.withColumn(field_name, field_type, expr=struct_expr, baseColumn=generated_cols)
            
            elif field_type_name == 'map':
                spec, map_expr, generated_cols = self._process_map_field(
                    spec, field_name, field_spec, event_type_field, []
                )
                field_type = self._get_spark_type(field_spec)
                spec = spec.withColumn(field_name, field_type, expr=map_expr)
            
            else:
                # Simple field types
                sql_expr = self._generate_sql_expression(field_spec)
                field_type = self._get_spark_type(field_spec)
                spec = spec.withColumn(field_name, field_type, expr=sql_expr)
        
        return spec
    
    def _add_conditional_fields_to_spec(self, spec, field_registry: Dict) -> dg.DataGenerator:
        """Add conditional fields using SQL CASE with IN clause, supporting nested types."""
        event_type_field = self._get_event_type_field_name()
        common_field_names = set(self.config.data.get('common_fields', {}).keys())
        
        for field_name, field_info in field_registry.items():
            # Skip fields that are already added as common fields
            if field_name in common_field_names:
                logger.debug(f"Skipping conditional field '{field_name}' (already in common_fields)")
                continue
            
            field_spec = field_info['spec']
            event_types = field_info['event_types']
            field_type_name = field_spec.get('type')
            
            # Handle nested types (array, struct, map)
            if field_type_name == 'array':
                spec, array_expr, generated_cols = self._process_array_field(
                    spec, field_name, field_spec, event_type_field, event_types
                )
                # Add final array column with conditional
                if len(event_types) == 1:
                    sql_expr = f"CASE WHEN {event_type_field} = '{event_types[0]}' THEN {array_expr} ELSE NULL END"
                else:
                    event_list = "', '".join(event_types)
                    sql_expr = f"CASE WHEN {event_type_field} IN ('{event_list}') THEN {array_expr} ELSE NULL END"
                
                field_type = self._get_spark_type(field_spec)
                base_cols = [event_type_field] + generated_cols
                spec = spec.withColumn(field_name, field_type, expr=sql_expr, baseColumn=base_cols)
            
            elif field_type_name == 'struct':
                spec, struct_expr, generated_cols = self._process_struct_field(
                    spec, field_name, field_spec, event_type_field, event_types
                )
                # Add final struct column with conditional
                if len(event_types) == 1:
                    sql_expr = f"CASE WHEN {event_type_field} = '{event_types[0]}' THEN {struct_expr} ELSE NULL END"
                else:
                    event_list = "', '".join(event_types)
                    sql_expr = f"CASE WHEN {event_type_field} IN ('{event_list}') THEN {struct_expr} ELSE NULL END"
                
                field_type = self._get_spark_type(field_spec)
                base_cols = [event_type_field] + generated_cols
                spec = spec.withColumn(field_name, field_type, expr=sql_expr, baseColumn=base_cols)
            
            elif field_type_name == 'map':
                spec, map_expr, generated_cols = self._process_map_field(
                    spec, field_name, field_spec, event_type_field, event_types
                )
                # Add final map column with conditional
                if len(event_types) == 1:
                    sql_expr = f"CASE WHEN {event_type_field} = '{event_types[0]}' THEN {map_expr} ELSE NULL END"
                else:
                    event_list = "', '".join(event_types)
                    sql_expr = f"CASE WHEN {event_type_field} IN ('{event_list}') THEN {map_expr} ELSE NULL END"
                
                field_type = self._get_spark_type(field_spec)
                spec = spec.withColumn(field_name, field_type, expr=sql_expr, baseColumn=event_type_field)
            
            else:
                # Simple field types
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
            begin = field_spec.get('begin')
            end = field_spec.get('end')
            
            if begin and end:
                # Generate timestamp within range
                # Convert to unix timestamp, generate random value, convert back
                return f"from_unixtime(unix_timestamp('{begin}') + rand() * (unix_timestamp('{end}') - unix_timestamp('{begin}')))"
            else:
                # Default: current timestamp
                return "current_timestamp()"
        
        elif field_type == 'boolean':
            # Support values with weights for probability
            values = field_spec.get('values', [True, False])
            weights = field_spec.get('weights')
            
            if weights:
                cumulative = 0.0
                sql_expr = "CASE "
                for value, weight in zip(values, weights):
                    cumulative += weight
                    bool_str = 'true' if value else 'false'
                    sql_expr += f"WHEN rand() < {cumulative} THEN {bool_str} "
                bool_str = 'true' if values[-1] else 'false'
                sql_expr += f"ELSE {bool_str} END"
                return sql_expr
            else:
                # Equal probability
                return "CASE WHEN rand() < 0.5 THEN true ELSE false END"
        
        elif field_type == 'long':
            min_val = field_spec.get('range', [0, 9223372036854775807])[0]
            max_val = field_spec.get('range', [0, 9223372036854775807])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS BIGINT)"
        
        elif field_type == 'double':
            min_val = field_spec.get('range', [0.0, 100.0])[0]
            max_val = field_spec.get('range', [0.0, 100.0])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS DOUBLE)"
        
        elif field_type == 'date':
            # Support begin/end like timestamp
            begin = field_spec.get('begin', '2020-01-01')
            end = field_spec.get('end', '2024-12-31')
            # Generate date within range using unix timestamp
            return f"to_date(from_unixtime(unix_timestamp('{begin}') + rand() * (unix_timestamp('{end}') - unix_timestamp('{begin}'))))"
        
        elif field_type == 'decimal':
            precision = field_spec.get('precision', 10)
            scale = field_spec.get('scale', 2)
            min_val = field_spec.get('range', [0, 99999999.99])[0]
            max_val = field_spec.get('range', [0, 99999999.99])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS DECIMAL({precision},{scale}))"
        
        elif field_type == 'byte':
            min_val = field_spec.get('range', [0, 127])[0]
            max_val = field_spec.get('range', [0, 127])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS TINYINT)"
        
        elif field_type == 'short':
            min_val = field_spec.get('range', [0, 32767])[0]
            max_val = field_spec.get('range', [0, 32767])[1]
            return f"CAST(rand() * ({max_val} - {min_val}) + {min_val} AS SMALLINT)"
        
        elif field_type == 'binary':
            # Binary type - generate as hex string then convert
            # Use uuid as default binary data
            return "unhex(replace(uuid(), '-', ''))"
        
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
    
    def _process_array_field(self, spec, field_name: str, field_spec: Dict[str, Any], event_type_field: str, event_types: list) -> tuple:
        """
        Process array field by generating multiple columns and combining them.
        
        Returns:
            tuple: (spec with array columns added, array_field_names)
        """
        item_type = field_spec.get('item_type', 'string')
        num_features = field_spec.get('num_features', [1, 5])
        values = field_spec.get('values', [])
        
        # Determine min and max array size
        if isinstance(num_features, list):
            min_size, max_size = num_features[0], num_features[1]
        else:
            min_size = max_size = num_features
        
        # Generate individual element columns
        element_columns = []
        for i in range(max_size):
            element_col_name = f"{field_name}_elem_{i}"
            element_columns.append(element_col_name)
            
            # Create field spec for array element
            element_spec = {
                'type': item_type,
                'values': values if values else None,
                'range': field_spec.get('range'),
                'weights': field_spec.get('weights')
            }
            
            # Generate SQL for element
            element_sql = self._generate_sql_expression(element_spec)
            element_spark_type = self._get_spark_type(element_spec)
            
            # Add conditional generation based on event type
            if event_types:
                event_list = "', '".join(event_types)
                conditional_sql = f"CASE WHEN {event_type_field} IN ('{event_list}') THEN {element_sql} ELSE NULL END"
            else:
                conditional_sql = element_sql
            
            spec = spec.withColumn(element_col_name, element_spark_type, 
                                 expr=conditional_sql, 
                                 baseColumn=event_type_field,
                                 omit=True)
        
        # Create array column combining elements with variable length
        array_elements = ", ".join(element_columns)
        
        if min_size == max_size:
            # Fixed size array
            array_expr = f"array({array_elements})"
        else:
            # Variable size array using slice
            size_expr = f"(abs(hash({event_type_field})) % {max_size - min_size + 1}) + {min_size}"
            array_expr = f"slice(array({array_elements}), 1, {size_expr})"
        
        return spec, array_expr, element_columns
    
    def _process_struct_field(self, spec, field_name: str, field_spec: Dict[str, Any], event_type_field: str, event_types: list) -> tuple:
        """
        Process struct field by recursively generating nested fields.
        
        Returns:
            tuple: (spec with struct fields added, struct_expr)
        """
        struct_fields = field_spec.get('fields', {})
        struct_parts = []
        generated_columns = []
        
        for sub_field_name, sub_field_spec in struct_fields.items():
            sub_col_name = f"{field_name}_{sub_field_name}"
            generated_columns.append(sub_col_name)
            sub_field_type = sub_field_spec.get('type')
            
            # Recursively handle nested types
            if sub_field_type == 'struct':
                spec, sub_struct_expr, sub_generated = self._process_struct_field(
                    spec, sub_col_name, sub_field_spec, event_type_field, event_types
                )
                struct_parts.append(f"'{sub_field_name}', {sub_struct_expr}")
                generated_columns.extend(sub_generated)
            elif sub_field_type == 'array':
                spec, sub_array_expr, sub_generated = self._process_array_field(
                    spec, sub_col_name, sub_field_spec, event_type_field, event_types
                )
                struct_parts.append(f"'{sub_field_name}', {sub_array_expr}")
                generated_columns.extend(sub_generated)
            else:
                # Simple field
                sub_sql = self._generate_sql_expression(sub_field_spec)
                sub_spark_type = self._get_spark_type(sub_field_spec)
                
                # Add conditional generation
                if event_types:
                    event_list = "', '".join(event_types)
                    conditional_sql = f"CASE WHEN {event_type_field} IN ('{event_list}') THEN {sub_sql} ELSE NULL END"
                else:
                    conditional_sql = sub_sql
                
                spec = spec.withColumn(sub_col_name, sub_spark_type,
                                     expr=conditional_sql,
                                     baseColumn=event_type_field,
                                     omit=True)
                struct_parts.append(f"'{sub_field_name}', {sub_col_name}")
        
        # Build named_struct expression
        struct_expr = f"named_struct({', '.join(struct_parts)})"
        
        return spec, struct_expr, generated_columns
    
    def _process_map_field(self, spec, field_name: str, field_spec: Dict[str, Any], event_type_field: str, event_types: list) -> tuple:
        """
        Process map field by generating discrete map values.
        
        Returns:
            tuple: (spec with map column added, map_expr)
        """
        values = field_spec.get('values', [])
        weights = field_spec.get('weights')
        
        if not values:
            # Empty map by default
            key_type = field_spec.get('key_type', 'string')
            value_type = field_spec.get('value_type', 'string')
            return spec, f"map()", []
        
        # Generate map from discrete values
        # values should be list of dicts: [{"key1": "val1"}, {"key2": "val2"}]
        if weights:
            cumulative = 0.0
            sql_expr = "CASE "
            for value_dict, weight in zip(values, weights):
                cumulative += weight
                # Convert dict to map SQL
                map_pairs = [f"'{k}', '{v}'" for k, v in value_dict.items()]
                sql_expr += f"WHEN rand() < {cumulative} THEN map({', '.join(map_pairs)}) "
            # Last value
            map_pairs = [f"'{k}', '{v}'" for k, v in values[-1].items()]
            sql_expr += f"ELSE map({', '.join(map_pairs)}) END"
        else:
            # Equal probability for each map
            threshold = 1.0 / len(values)
            sql_expr = "CASE "
            for i, value_dict in enumerate(values):
                map_pairs = [f"'{k}', '{v}'" for k, v in value_dict.items()]
                sql_expr += f"WHEN rand() < {(i+1) * threshold} THEN map({', '.join(map_pairs)}) "
            map_pairs = [f"'{k}', '{v}'" for k, v in values[-1].items()]
            sql_expr += f"ELSE map({', '.join(map_pairs)}) END"
        
        return spec, sql_expr, []
    
    def _get_spark_type(self, field_spec: Dict[str, Any]) -> str:
        """Map field type to Spark SQL type string."""
        field_type = field_spec.get('type')
        
        # Handle decimal with precision/scale
        if field_type == 'decimal':
            precision = field_spec.get('precision', 10)
            scale = field_spec.get('scale', 2)
            return f'decimal({precision},{scale})'
        
        # Handle array type
        if field_type == 'array':
            item_type = field_spec.get('item_type', 'string')
            item_spec = {'type': item_type}
            item_spark_type = self._get_spark_type(item_spec)
            return f'array<{item_spark_type}>'
        
        # Handle map type
        if field_type == 'map':
            key_type = field_spec.get('key_type', 'string')
            value_type = field_spec.get('value_type', 'string')
            key_spec = {'type': key_type}
            value_spec = {'type': value_type}
            key_spark_type = self._get_spark_type(key_spec)
            value_spark_type = self._get_spark_type(value_spec)
            return f'map<{key_spark_type},{value_spark_type}>'
        
        # Handle struct type - need to build recursively
        if field_type == 'struct':
            fields = field_spec.get('fields', {})
            field_defs = []
            for sub_name, sub_spec in fields.items():
                sub_type = self._get_spark_type(sub_spec)
                field_defs.append(f'{sub_name}:{sub_type}')
            return f"struct<{','.join(field_defs)}>"
        
        type_map = {
            'uuid': 'string',
            'int': 'int', 
            'float': 'float',
            'string': 'string',
            'timestamp': 'timestamp',
            'boolean': 'boolean',
            'long': 'bigint',
            'double': 'double',
            'date': 'date',
            'byte': 'tinyint',
            'short': 'smallint',
            'binary': 'binary'
        }
        return type_map.get(field_type, 'string')
    
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
