"""dblstreamgen - Generate synthetic streaming data at scale for Databricks."""

__version__ = "0.4.0"

from dblstreamgen.config import Config, ConfigurationError, load_config, load_config_from_dict

# Spark-heavy imports are guarded so that config.py and pure-Python modules
# can be used (and tested) without pyspark / dbldatagen installed.
try:
    from dblstreamgen.scenario import Scenario, ScenarioResult, SinkPlan
except ImportError:
    Scenario = None  # type: ignore[assignment,misc]
    ScenarioResult = None  # type: ignore[assignment,misc]
    SinkPlan = None  # type: ignore[assignment,misc]

try:
    from dblstreamgen.serialization import serialize_to_avro, serialize_to_json
except ImportError:
    serialize_to_json = None  # type: ignore[assignment,misc]
    serialize_to_avro = None  # type: ignore[assignment,misc]

try:
    from dblstreamgen.sinks import KinesisDataSource
except ImportError:
    KinesisDataSource = None  # type: ignore[assignment,misc]

# Deprecation shim: StreamOrchestrator is kept importable for one minor version.
import warnings as _warnings

try:
    from dblstreamgen.orchestrator import StreamOrchestrator as _SO

    class StreamOrchestrator(_SO):  # type: ignore[no-redef]
        def __init_subclass__(cls, **kw: object) -> None:
            super().__init_subclass__(**kw)

        def __init__(self, *args: object, **kwargs: object) -> None:
            _warnings.warn(
                "StreamOrchestrator is deprecated and will be removed in v0.5.  "
                "Use Scenario(spark, Config.from_yaml(...)).build() instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]
except ImportError:
    StreamOrchestrator = None  # type: ignore[assignment,misc]

__all__ = [
    "__version__",
    "Config",
    "ConfigurationError",
    "load_config",
    "load_config_from_dict",
    "Scenario",
    "ScenarioResult",
    "SinkPlan",
    "serialize_to_json",
    "serialize_to_avro",
    "KinesisDataSource",
    "StreamOrchestrator",
]
