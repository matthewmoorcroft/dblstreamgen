"""Shared dataclasses for the scenario package."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SinkPlan:
    name: str
    start_seconds: int
    stop_seconds: int
    rows_per_second: int
    event_types: list[str]
    weights: dict
    hidden_column_count: int
    dedup_ratio: float

    def __repr__(self) -> str:
        return (
            f"SinkPlan({self.name}: t={self.start_seconds}s-{self.stop_seconds}s, "
            f"rate={self.rows_per_second}/s, types={len(self.event_types)}, "
            f"hidden_cols={self.hidden_column_count} ({self.dedup_ratio:.0%} dedup))"
        )


@dataclass
class ScenarioResult:
    duration_seconds: float
    total_rows_written: Optional[int]
    queries_started: int
    queries_stopped: int
    errors: list = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def __repr__(self) -> str:
        status = "OK" if self.success else f"FAILED ({len(self.errors)} errors)"
        rows = f"{self.total_rows_written:,}" if self.total_rows_written else "unknown"
        return (
            f"ScenarioResult({status}: {self.duration_seconds:.1f}s, "
            f"~{rows} rows, {self.queries_started} queries)"
        )
