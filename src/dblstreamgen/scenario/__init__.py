"""Scenario package -- public facade, builder, runner."""

try:
    from dblstreamgen.scenario.scenario import Scenario
    from dblstreamgen.scenario.types import ScenarioResult, SinkPlan
except ImportError:
    Scenario = None  # type: ignore[assignment,misc]
    ScenarioResult = None  # type: ignore[assignment,misc]
    SinkPlan = None  # type: ignore[assignment,misc]

__all__ = ["Scenario", "ScenarioResult", "SinkPlan"]
