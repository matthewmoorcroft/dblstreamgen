"""DataGenerator spec builder for dbldatagen."""

import logging
from pyspark.sql import SparkSession

from dblstreamgen.config import Config

logger = logging.getLogger(__name__)


class DataGeneratorBuilder:
    """Utility class for partition calculations and streaming configuration."""
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize builder.
        
        Args:
            spark: Active SparkSession
            config: Validated configuration
        """
        self.spark = spark
        self.config = config
    
    def _calculate_partitions_from_rate(self, total_rows_per_second: int) -> int:
        """
        Calculate partitions targeting ~800 events/sec per partition (min 4, multiple of 4).
        
        Args:
            total_rows_per_second: Total throughput
            
        Returns:
            Partition count rounded to nearest multiple of 4
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
            Value rounded to multiple of 4 (minimum 4)
        """
        return max(4, (value + 2) // 4 * 4)
