"""dblstreamgen - Generate synthetic streaming data at scale for Databricks."""

__version__ = "0.1.0"

from dblstreamgen.config import load_config, Config, ConfigurationError
from dblstreamgen.builder import FieldMapper, DataGeneratorBuilder
from dblstreamgen.orchestrator import StreamOrchestrator
from dblstreamgen.sinks import KinesisDataSource

__all__ = [
    '__version__',
    'load_config',
    'Config',
    'ConfigurationError',
    'FieldMapper',
    'DataGeneratorBuilder',
    'StreamOrchestrator',
    'KinesisDataSource',
]