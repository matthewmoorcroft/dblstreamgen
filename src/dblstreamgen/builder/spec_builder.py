"""DataGenerator spec builder for dbldatagen."""

import logging
import dbldatagen as dg
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, TimestampType
from typing import Dict, Any

from dblstreamgen.config import Config
from dblstreamgen.builder.field_mappers import FieldMapper

logger = logging.getLogger(__name__)


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
    
    def _calculate_partitions_from_rate(self, total_rows_per_second: int) -> int:
        """
        Calculate partitions targeting ~800 events/sec per partition (min 4, multiple of 4).
        
        Args:
            total_rows_per_second: Total throughput
            
        Returns:
            int: Partition count
        """
        rate_based = total_rows_per_second // 800
        bounded = max(4, rate_based)
        partitions = self._round_to_multiple_of_4(bounded)
        
        logger.debug(f"Calculated {partitions} partitions for {total_rows_per_second:,} events/sec")
        
        return partitions
    
    def _round_to_multiple_of_4(self, value: int) -> int:
        """
        Round value to nearest multiple of 4.
        
        Args:
            value: Input value
            
        Returns:
            int: Value rounded to multiple of 4
        """
        return max(4, (value + 2) // 4 * 4)  # Round to nearest, minimum 4
    
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
            # For streaming, rows = rows per micro-batch
            # This is a reasonable default; actual rate controlled by rowsPerSecond
            rows = 1000
            
            # Calculate partitions - prioritize event-specific, then global, then auto
            streaming_config = self.config.data.get('streaming_config', {})
            
            if 'partitions' in event_type:
                partitions = self._round_to_multiple_of_4(event_type['partitions'])
                logger.info(f"Event '{event_type_id}': using event-specific {partitions} partitions")
            elif 'partitions' in streaming_config:
                partitions = self._round_to_multiple_of_4(streaming_config['partitions'])
                logger.info(f"Event '{event_type_id}': using global {partitions} partitions")
            else:
                partitions = self._calculate_partitions_from_rate(int(rows_per_second))
                logger.info(f"Event '{event_type_id}': auto-calculated {partitions} partitions for {rows_per_second:,.0f} events/sec")
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
            logger.info(f"Event '{event_type['event_type_id']}': streaming at {rows_per_second:,.0f} events/sec")
            return spec.build(
                withStreaming=True,
                options={'rowsPerSecond': int(rows_per_second)}
            )
        else:
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
