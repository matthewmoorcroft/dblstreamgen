"""Spec deduplication for conditional field hidden columns.

Groups event types that share identical generation specs for the same field
name so they can share a single hidden column.  Zero Spark imports.
"""

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dblstreamgen.config import ConfigurationError

logger = logging.getLogger(__name__)

_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
_DATE_FORMAT = "%Y-%m-%d"


@dataclass(frozen=True)
class HiddenColumn:
    """One hidden column to be added with ``omit=True``."""

    name: str
    field_spec: dict
    event_type_ids: tuple


@dataclass(frozen=True)
class DeduplicationResult:
    """Output of ``SpecDeduplicator.deduplicate`` for one output field."""

    field_name: str
    hidden_columns: tuple
    routing_expr: str
    base_columns: tuple
    dedup_ratio: float


class SpecDeduplicator:
    """Groups conditional-field specs by canonical signature.

    For each output field name shared by N event types, produces M <= N
    hidden columns (one per unique spec) and a CASE WHEN routing expression.
    """

    def __init__(self) -> None:
        self._registry: dict = {}

    def deduplicate(
        self,
        field_name: str,
        event_specs: dict[str, dict],
        event_type_field: str = "__dsg_event_type_id",
    ) -> DeduplicationResult:
        """Deduplicate specs for *field_name* across event types.

        Parameters
        ----------
        field_name:
            The user-visible output column name (e.g. ``"price"``).
        event_specs:
            ``{event_type_id: field_spec}`` for every event type that defines
            this field.
        event_type_field:
            Internal discriminator column name.

        Returns
        -------
        DeduplicationResult with hidden columns and a CASE WHEN routing expression.
        """
        groups: dict[str, list[str]] = {}
        sig_to_spec: dict[str, dict] = {}

        for event_type_id, spec in event_specs.items():
            sig = self.compute_signature(spec)
            groups.setdefault(sig, []).append(event_type_id)
            if sig not in sig_to_spec:
                sig_to_spec[sig] = spec

        hidden_cols: list = []
        case_branches: list = []
        base_cols: list = [event_type_field]

        for sig, event_type_ids in groups.items():
            hidden_name = self._hidden_column_name(field_name, sig)
            if hidden_name in self._registry:
                raise ConfigurationError(
                    f"Hidden column name collision: '{hidden_name}'.  "
                    f"Two distinct signatures produced the same 8-char hash.  "
                    f"This is extremely unlikely -- please report a bug."
                )
            self._registry[hidden_name] = sig

            hidden_cols.append(
                HiddenColumn(
                    name=hidden_name,
                    field_spec=sig_to_spec[sig],
                    event_type_ids=tuple(sorted(event_type_ids)),
                )
            )
            base_cols.append(hidden_name)

            if len(event_type_ids) == 1:
                cond = f"{event_type_field} = '{event_type_ids[0]}'"
            else:
                id_list = "', '".join(sorted(event_type_ids))
                cond = f"{event_type_field} IN ('{id_list}')"
            case_branches.append(f"WHEN {cond} THEN {hidden_name}")

        routing_expr = "CASE " + " ".join(case_branches) + " ELSE NULL END"

        total_defs = len(event_specs)
        unique_specs = len(groups)
        dedup_ratio = 1.0 - (unique_specs / total_defs) if total_defs > 0 else 0.0

        return DeduplicationResult(
            field_name=field_name,
            hidden_columns=tuple(hidden_cols),
            routing_expr=routing_expr,
            base_columns=tuple(base_cols),
            dedup_ratio=dedup_ratio,
        )

    # ------------------------------------------------------------------
    # Signature computation
    # ------------------------------------------------------------------

    @staticmethod
    def compute_signature(field_spec: dict) -> str:
        """Canonical string from generation-affecting parameters.

        Numerics canonicalized to ``float()``, weights normalized to
        sum-1.0 ratios at 6 decimal places, timestamps parsed to ISO 8601.
        """
        parts: dict = {}
        parts["type"] = field_spec.get("type", "string")

        if "range" in field_spec:
            r = field_spec["range"]
            parts["range"] = f"{float(r[0])}:{float(r[1])}"

        if "values" in field_spec:
            parts["values"] = ",".join(str(v) for v in field_spec["values"])

        if "weights" in field_spec:
            total = sum(field_spec["weights"])
            if total > 0:
                normalized = [w / total for w in field_spec["weights"]]
            else:
                normalized = field_spec["weights"]
            parts["weights"] = ",".join(f"{w:.6f}" for w in normalized)

        if "begin" in field_spec:
            parts["begin"] = _normalize_temporal(field_spec["begin"], field_spec.get("type"))

        if "end" in field_spec:
            parts["end"] = _normalize_temporal(field_spec["end"], field_spec.get("type"))

        if "interval" in field_spec:
            parts["interval"] = str(field_spec["interval"])

        if "random" in field_spec:
            parts["random"] = str(bool(field_spec["random"]))

        if "mode" in field_spec:
            parts["mode"] = str(field_spec["mode"])

        if "jitter_seconds" in field_spec:
            parts["jitter_seconds"] = str(float(field_spec["jitter_seconds"]))

        if "step" in field_spec:
            parts["step"] = f"{float(field_spec['step']):.10g}"

        if "percent_nulls" in field_spec:
            parts["pn"] = f"{float(field_spec['percent_nulls']):.6f}"

        if "faker" in field_spec:
            parts["faker"] = str(field_spec["faker"])
            if "faker_args" in field_spec:
                args = field_spec["faker_args"]
                parts["faker_args"] = ",".join(f"{k}={v}" for k, v in sorted(args.items()))

        if "precision" in field_spec:
            parts["precision"] = str(int(field_spec["precision"]))
        if "scale" in field_spec:
            parts["scale"] = str(int(field_spec["scale"]))

        if "expr" in field_spec:
            parts["expr"] = str(field_spec["expr"])

        return "|".join(f"{k}={v}" for k, v in sorted(parts.items()))

    # ------------------------------------------------------------------
    # Hidden column naming
    # ------------------------------------------------------------------

    @staticmethod
    def _hidden_column_name(field_name: str, signature: str) -> str:
        sig_hash = hashlib.md5(signature.encode("utf-8")).hexdigest()[:8]
        return f"__base_{field_name}_{sig_hash}"


def _normalize_temporal(value: str, field_type: Optional[str] = None) -> str:
    """Parse temporal value to a canonical ISO 8601 string."""
    value = str(value).strip()
    fmt = _DATE_FORMAT if field_type == "date" else _TIMESTAMP_FORMAT
    try:
        dt = datetime.strptime(value, fmt)
        return dt.isoformat()
    except ValueError:
        return value
