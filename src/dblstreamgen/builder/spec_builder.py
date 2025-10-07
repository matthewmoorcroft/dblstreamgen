"""DataGenerator spec builder for dbldatagen."""

import dbldatagen as dg
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, TimestampType
from typing import Dict, Any

from dblstreamgen.config import Config
from dblstreamgen.builder.field_mappers import FieldMapper


class DataGeneratorBuilder:
    """
    Builds dbldatagen DataGenerator specifications from YAML configuration.
    
    Creates separate DataGenerator specs for each event type, with appropriate
    rates based on weight distribution.
    
    Example:
        >>> builder = DataGeneratorBuilder(spark, config)
        >>> df = builder.build_dataframe(event_type, 500)  # 500 events/sec
    """
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize builder.
        
        Args:
            spark: Active SparkSession
            config: Validated configuration
        """
        self.spark = spark
        self.config = config
        self.field_mapper = FieldMapper()
    
    def build_spec_for_event_type(self, 
                                    event_type: Dict[str, Any],
                                    rows_per_second: float) -> dg.DataGenerator:
        """
        Create a dbldatagen DataGenerator spec for a single event type.
        
        Args:
            event_type: Event type definition from config
            rows_per_second: Target rate for this event type
            
        Returns:
            Configured dbldatagen DataGenerator spec
            
        Example:
            >>> spec = builder.build_spec_for_event_type(
            ...     {'event_type_id': 'user.click', 'weight': 0.5, 'fields': {...}},
            ...     500.0
            ... )
        """
        event_type_id = event_type['event_type_id']
        
        # Determine rows and partitions based on mode
        generation_mode = self.config.data['generation_mode']
        
        if generation_mode == 'streaming':
            # For streaming, rows parameter is ignored
            rows = None
            partitions = 4  # Default for streaming
        else:  # batch
            # Calculate rows for this event type based on weight
            total_rows = self.config.data['batch_config']['total_rows']
            weight = event_type['weight']
            rows = int(total_rows * weight)
            partitions = self.config.data['batch_config'].get('partitions', 8)
        
        # Create base DataGenerator
        spec = dg.DataGenerator(
            self.spark,
            name=f"datagen_{event_type_id.replace('.', '_')}",
            rows=rows,
            partitions=partitions
        )
        
        # Add event_type_id column (constant value for this event type)
        spec = spec.withColumn(
            "event_type_id",
            StringType(),
            values=[event_type_id]
        )
        
        # Add event_timestamp column
        spec = spec.withColumn(
            "event_timestamp",
            TimestampType(),
            expr="now()"
        )
        
        # Add common fields (if defined)
        for field_name, field_spec in self.config.data.get('common_fields', {}).items():
            spec = self.field_mapper.map_field(spec, field_name, field_spec)
        
        # Add event-specific fields
        for field_name, field_spec in event_type.get('fields', {}).items():
            spec = self.field_mapper.map_field(spec, field_name, field_spec)
        
        return spec
    
    def build_dataframe(self, 
                         event_type: Dict[str, Any],
                         rows_per_second: float) -> DataFrame:
        """
        Build a Spark DataFrame for an event type.
        
        Args:
            event_type: Event type definition from config
            rows_per_second: Target rate for this event type (streaming only)
            
        Returns:
            Spark DataFrame (streaming or batch)
            
        Example:
            >>> df = builder.build_dataframe(event_type, 500.0)
            >>> df.isStreaming
            True
        """
        # Build the spec
        spec = self.build_spec_for_event_type(event_type, rows_per_second)
        
        # Build DataFrame based on mode
        generation_mode = self.config.data['generation_mode']
        
        if generation_mode == 'streaming':
            # Build streaming DataFrame
            return spec.build(
                withStreaming=True,
                options={'rowsPerSecond': int(rows_per_second)}
            )
        else:
            # Build batch DataFrame
            return spec.build()
    
    def build_all_dataframes(self) -> Dict[str, DataFrame]:
        """
        Build DataFrames for all event types with calculated rates.
        
        Returns:
            Dictionary mapping event_type_id to DataFrame
            
        Example:
            >>> dfs = builder.build_all_dataframes()
            >>> len(dfs)
            3
        """
        # Calculate rates for each event type
        if self.config.data['generation_mode'] == 'streaming':
            total_rate = self.config.data['streaming_config']['total_rows_per_second']
        else:
            # For batch, rate doesn't matter (rows are pre-calculated)
            total_rate = 0
        
        dataframes = {}
        
        for event_type in self.config.data['event_types']:
            event_id = event_type['event_type_id']
            weight = event_type['weight']
            rate = total_rate * weight
            
            df = self.build_dataframe(event_type, rate)
            dataframes[event_id] = df
        
        return dataframes
