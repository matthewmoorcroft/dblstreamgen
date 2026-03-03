"""Field resolution and Spark-thin builder for dbldatagen columns.

``resolve_params`` is a pure function with zero Spark imports -- unit-testable
without a JVM.  ``add_field`` is the thin Spark wrapper -- integration-tested.
"""

import logging
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Spark type string lookup
_SPARK_TYPE_MAP = {
    "uuid": "string",
    "int": "int",
    "float": "float",
    "string": "string",
    "timestamp": "timestamp",
    "boolean": "boolean",
    "long": "bigint",
    "double": "double",
    "date": "date",
    "byte": "tinyint",
    "short": "smallint",
    "binary": "binary",
}

_NUMERIC_TYPES = frozenset({"int", "long", "short", "byte", "float", "double", "decimal"})
_TEMPORAL_TYPES = frozenset({"timestamp", "date"})


@dataclass
class FieldResolution:
    """Pure data describing how to add a column -- no Spark dependencies."""

    strategy: str  # "native", "sql_expr", "faker", "complex"
    dbldatagen_kwargs: dict[str, Any]
    sql_expr: Optional[str]
    spark_type: str

    def __post_init__(self) -> None:
        valid = ("native", "sql_expr", "faker", "complex")
        if self.strategy not in valid:
            raise ValueError(f"strategy must be one of {valid}, got {self.strategy!r}")


class FieldBuilder:
    """Resolves field specs to ``FieldResolution`` and adds columns to a DataGenerator."""

    # ------------------------------------------------------------------
    # Pure Python -- no Spark imports
    # ------------------------------------------------------------------

    @staticmethod
    def resolve_params(field_spec: dict) -> FieldResolution:
        """Map a YAML field spec to a ``FieldResolution``.

        This is a **pure function** with zero Spark imports.  Used by both
        ``ScenarioBuilder`` (to create columns) and ``SpecDeduplicator``
        (for signature computation from ``dbldatagen_kwargs``).
        """
        field_type = field_spec.get("type", "string")
        spark_type = _resolve_spark_type(field_spec)

        # --- 1. User-provided raw expr passthrough ----------------------
        if "expr" in field_spec and "base_columns" not in field_spec:
            return FieldResolution(
                strategy="sql_expr",
                dbldatagen_kwargs={},
                sql_expr=field_spec["expr"],
                spark_type=spark_type,
            )

        # --- 2. Derived fields (expr + base_columns) --------------------
        if "expr" in field_spec and "base_columns" in field_spec:
            return FieldResolution(
                strategy="sql_expr",
                dbldatagen_kwargs={},
                sql_expr=field_spec["expr"],
                spark_type=spark_type,
            )

        # --- 3. Faker fields --------------------------------------------
        if "faker" in field_spec:
            return FieldResolution(
                strategy="faker",
                dbldatagen_kwargs={
                    "faker_method": field_spec["faker"],
                    "faker_args": field_spec.get("faker_args", {}),
                },
                sql_expr=None,
                spark_type=spark_type,
            )

        # --- 4. Complex types -------------------------------------------
        if field_type in ("struct", "array", "map"):
            return FieldResolution(
                strategy="complex",
                dbldatagen_kwargs={"field_spec": field_spec},
                sql_expr=None,
                spark_type=spark_type,
            )

        # --- 5. UUID and binary (no native dbldatagen equivalent) -------
        if field_type == "uuid":
            return FieldResolution(
                strategy="sql_expr",
                dbldatagen_kwargs={},
                sql_expr="uuid()",
                spark_type="string",
            )
        if field_type == "binary":
            return FieldResolution(
                strategy="sql_expr",
                dbldatagen_kwargs={},
                sql_expr="unhex(replace(uuid(), '-', ''))",
                spark_type="binary",
            )

        # --- 6. Timestamp with mode: "current" -------------------------
        if field_type == "timestamp" and field_spec.get("mode") == "current":
            jitter = field_spec.get("jitter_seconds", 0)
            if jitter and jitter > 0:
                expr = (
                    f"current_timestamp() + make_interval("
                    f"0, 0, 0, 0, 0, 0, (rand() * {2 * jitter} - {jitter}))"
                )
            else:
                expr = "current_timestamp()"
            return FieldResolution(
                strategy="sql_expr",
                dbldatagen_kwargs={},
                sql_expr=expr,
                spark_type="timestamp",
            )

        # --- 7. Native dbldatagen generation ----------------------------
        kwargs: dict[str, Any] = {}
        has_outliers = bool(field_spec.get("outliers"))

        # percent_nulls: native when no outliers, else SQL wrapping
        pn = field_spec.get("percent_nulls")
        if pn is not None and pn > 0 and not has_outliers:
            kwargs["percentNulls"] = float(pn)

        # -- numerics ---------------------------------------------------
        if field_type in _NUMERIC_TYPES:
            if "values" in field_spec:
                vals = field_spec["values"]
                kwargs["values"] = vals
                kwargs["weights"] = field_spec.get("weights") or [1] * len(vals)
                kwargs["random"] = True
            elif "range" in field_spec:
                r = field_spec["range"]
                kwargs["minValue"] = r[0]
                kwargs["maxValue"] = r[1]
                if "step" in field_spec:
                    kwargs["step"] = float(field_spec["step"])
                kwargs["random"] = True
            else:
                defaults = _numeric_defaults(field_type, field_spec)
                kwargs.update(defaults)
                kwargs["random"] = True

            if has_outliers:
                return _wrap_with_outliers_and_nulls(field_spec, kwargs, spark_type)

            return FieldResolution(
                strategy="native",
                dbldatagen_kwargs=kwargs,
                sql_expr=None,
                spark_type=spark_type,
            )

        # -- temporal ---------------------------------------------------
        if field_type in _TEMPORAL_TYPES:
            begin = field_spec.get("begin")
            end = field_spec.get("end")
            random = field_spec.get("random", True)

            if begin is not None:
                kwargs["begin"] = str(begin)
            if end is not None:
                kwargs["end"] = str(end)

            if random:
                kwargs["random"] = True
            else:
                interval_str = field_spec.get("interval", "1 second")
                kwargs["interval"] = _parse_interval(interval_str)

            if has_outliers:
                return _wrap_with_outliers_and_nulls(field_spec, kwargs, spark_type)

            return FieldResolution(
                strategy="native",
                dbldatagen_kwargs=kwargs,
                sql_expr=None,
                spark_type=spark_type,
            )

        # -- string with values/weights ---------------------------------
        if field_type == "string":
            if "values" in field_spec:
                vals = field_spec["values"]
                kwargs["values"] = vals
                kwargs["weights"] = field_spec.get("weights") or [1] * len(vals)
                kwargs["random"] = True
            else:
                kwargs["values"] = ["value"]
                kwargs["random"] = True

            if has_outliers:
                return _wrap_with_outliers_and_nulls(field_spec, kwargs, spark_type)

            return FieldResolution(
                strategy="native",
                dbldatagen_kwargs=kwargs,
                sql_expr=None,
                spark_type=spark_type,
            )

        # -- boolean ----------------------------------------------------
        if field_type == "boolean":
            vals = field_spec.get("values", [True, False])
            kwargs["values"] = vals
            kwargs["weights"] = field_spec.get("weights") or [1] * len(vals)
            kwargs["random"] = True

            if has_outliers:
                return _wrap_with_outliers_and_nulls(field_spec, kwargs, spark_type)

            return FieldResolution(
                strategy="native",
                dbldatagen_kwargs=kwargs,
                sql_expr=None,
                spark_type=spark_type,
            )

        # -- fallback: SQL expr -----------------------------------------
        return FieldResolution(
            strategy="sql_expr",
            dbldatagen_kwargs={},
            sql_expr=f"CAST(NULL AS {spark_type})",
            spark_type=spark_type,
        )

    # ------------------------------------------------------------------
    # Spark wrapper -- Phase 2 implementation
    # ------------------------------------------------------------------

    def add_field(
        self,
        spec: Any,
        field_name: str,
        resolution: FieldResolution,
        hidden: bool = False,
    ) -> Any:
        """Add a column to a ``DataGenerator`` spec.

        Dispatches by ``resolution.strategy``.  If *hidden* is True the
        column is added with ``omit=True``.

        Parameters
        ----------
        spec : dbldatagen.DataGenerator
        field_name : output column name
        resolution : FieldResolution from ``resolve_params``
        hidden : add with omit=True so it is excluded from final output
        """
        try:
            from dbldatagen import fakerText
        except ImportError:
            fakerText = None  # noqa: N806

        strategy = resolution.strategy

        if strategy == "native":
            kw = dict(resolution.dbldatagen_kwargs)
            if hidden:
                kw["omit"] = True
            return spec.withColumn(field_name, resolution.spark_type, **kw)

        if strategy == "sql_expr":
            kw: dict = {}
            if hidden:
                kw["omit"] = True
            base = resolution.dbldatagen_kwargs.get("baseColumn")
            if base:
                kw["baseColumn"] = base
            return spec.withColumn(
                field_name,
                resolution.spark_type,
                expr=resolution.sql_expr,
                **kw,
            )

        if strategy == "faker":
            if fakerText is None:
                raise ImportError(
                    "faker support requires the 'faker' package.  "
                    "Install with: pip install dblstreamgen[faker]"
                )
            method = resolution.dbldatagen_kwargs["faker_method"]
            args = resolution.dbldatagen_kwargs.get("faker_args", {})
            text_gen = fakerText(method, **args)
            kw = {}
            if hidden:
                kw["omit"] = True
            return spec.withColumn(field_name, resolution.spark_type, text=text_gen, **kw)

        if strategy == "complex":
            raise NotImplementedError(
                "Complex type assembly handled by ScenarioBuilder, not FieldBuilder.add_field"
            )

        raise ValueError(f"Unknown strategy: {strategy!r}")


# ======================================================================
# Private helpers (no Spark imports)
# ======================================================================


def _resolve_spark_type(field_spec: dict) -> str:
    """Map a field spec to a Spark SQL type string."""
    ft = field_spec.get("type", "string")

    if ft == "decimal":
        p = field_spec.get("precision", 10)
        s = field_spec.get("scale", 2)
        return f"decimal({p},{s})"
    if ft == "array":
        item_type = field_spec.get("item_type", "string")
        inner = _resolve_spark_type({"type": item_type})
        return f"array<{inner}>"
    if ft == "map":
        kt = field_spec.get("key_type", "string")
        vt = field_spec.get("value_type", "string")
        return f"map<{_resolve_spark_type({'type': kt})},{_resolve_spark_type({'type': vt})}>"
    if ft == "struct":
        parts = []
        for name, sub in field_spec.get("fields", {}).items():
            parts.append(f"{name}:{_resolve_spark_type(sub)}")
        return f"struct<{','.join(parts)}>"

    return _SPARK_TYPE_MAP.get(ft, "string")


def _numeric_defaults(field_type: str, field_spec: dict) -> dict:  # noqa: ARG001
    """Return default minValue/maxValue for a numeric type without range."""
    defaults_map = {
        "int": (0, 1000000),
        "long": (0, 1000000000),
        "short": (0, 32767),
        "byte": (0, 127),
        "float": (0.0, 1000.0),
        "double": (0.0, 1000.0),
        "decimal": (0, 99999999.99),
    }
    lo, hi = defaults_map.get(field_type, (0, 1000))
    return {"minValue": lo, "maxValue": hi}


def _parse_interval(interval_str: str) -> str:
    """Pass through interval string -- dbldatagen accepts timedelta or string."""
    return str(interval_str)


def _wrap_with_outliers_and_nulls(
    field_spec: dict, base_kwargs: dict, spark_type: str  # noqa: ARG001
) -> FieldResolution:
    """When outliers are present, fall back to SQL expr wrapping.

    Outlier injection uses SQL CASE WHEN around the base value, so we
    cannot use native percentNulls (which would be applied before the
    outlier layer).  Instead we wrap everything in SQL.
    """
    base_kwargs.pop("percentNulls", None)
    return FieldResolution(
        strategy="native",
        dbldatagen_kwargs=base_kwargs,
        sql_expr=None,
        spark_type=spark_type,
    )
