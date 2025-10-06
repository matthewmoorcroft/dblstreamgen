"""
dblstreamgen - Databricks stream data generation for harness testing

A library for generating synthetic streaming data to test and validate
Databricks data pipelines. Supports both continuous stream generation and
high-throughput batch generation with publishers for Kinesis and Kafka.

Quick Start:
    >>> import dblstreamgen
    >>> 
    >>> # Load configuration
    >>> config = dblstreamgen.load_config('gen.yaml', 'kinesis.yaml')
    >>> 
    >>> # Create generator and publisher
    >>> generator = dblstreamgen.StreamGenerator(config)
    >>> publisher = dblstreamgen.KinesisPublisher(config['kinesis_config'])
    >>> 
    >>> # Generate and publish events
    >>> for event in generator.generate():
    >>>     publisher.publish_single(event)
"""

__version__ = "0.1.0"

# Core configuration
from dblstreamgen.config import load_config, Config

# Generators
from dblstreamgen.generators.stream import StreamGenerator

# Publishers
from dblstreamgen.publishers.kinesis import KinesisPublisher, PublishResult

__all__ = [
    # Version
    "__version__",
    
    # Configuration
    "load_config",
    "Config",
    
    # Generators
    "StreamGenerator",
    
    # Publishers
    "KinesisPublisher",
    "PublishResult",
]


