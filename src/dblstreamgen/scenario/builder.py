"""ScenarioBuilder -- builds DataGenerators and DataFrames.

Mode-agnostic: works for both streaming and batch.  No streaming query
management -- that responsibility belongs to ``ScenarioRunner``.
"""

import logging
from typing import Any, Optional

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession

from dblstreamgen.config import Config, ConfigurationError
from dblstreamgen.scenario.field_builder import FieldBuilder
from dblstreamgen.scenario.spec_dedup import SpecDeduplicator

logger = logging.getLogger(__name__)

_EVENT_TYPE_FIELD = "__dsg_event_type_id"
_HIDDEN_COL_WARNING = 500
_HIDDEN_COL_ERROR = 1000


class ScenarioBuilder:
    """Builds DataGenerators and DataFrames.  Mode-agnostic."""

    def __init__(self, spark: SparkSession, config: Config):
        self._spark = spark
        self._config = config
        self._field_builder = FieldBuilder()
        self._hidden_col_count = 0

    def build(
        self,
        event_types_with_weights: Optional[list[dict]] = None,
        rows_per_second: Optional[int] = None,
        total_rows: Optional[int] = None,
        serialize: Optional[bool] = None,  # noqa: ARG002 -- used by Scenario facade
    ) -> DataFrame:
        """Build one DataFrame from the config.

        Parameters
        ----------
        event_types_with_weights:
            Override the event types + weights (for spike/ramp). If None,
            uses the config's event_types.
        rows_per_second:
            Override baseline_rows_per_second (for spike/ramp).
        total_rows:
            Override total_rows (batch mode).
        serialize:
            None = use config, True = force narrow, False = force wide.
        """
        self._hidden_col_count = 0

        active_types = self._filter_active_types(event_types_with_weights)
        if not active_types:
            raise ConfigurationError("No active event types (all weights are zero).")

        spec = self._create_base_spec(rows_per_second, total_rows)
        spec = self._add_event_type_id(spec, active_types)
        spec = self._add_common_fields(spec, active_types)
        spec = self._add_conditional_fields(spec, active_types)
        spec = self._add_derived_fields(spec)

        if self._hidden_col_count > _HIDDEN_COL_ERROR:
            raise ConfigurationError(
                f"Hidden column count ({self._hidden_col_count}) exceeds maximum "
                f"({_HIDDEN_COL_ERROR}).  Reduce unique specs or split scenarios."
            )
        if self._hidden_col_count > _HIDDEN_COL_WARNING:
            logger.warning(
                "Hidden column count (%d) exceeds %d.  Consider consolidating field specs.",
                self._hidden_col_count,
                _HIDDEN_COL_WARNING,
            )

        df = self._build_dataframe(spec, rows_per_second, total_rows)

        logger.info(
            "Built DataGenerator: %d event types, %d columns (%d hidden), mode=%s",
            len(active_types),
            len(df.columns),
            self._hidden_col_count,
            self._config.generation_mode,
        )

        return df

    def explain(self) -> str:
        """Human-readable breakdown of hidden columns and dedup effectiveness."""
        active = self._filter_active_types()
        field_registry = self._build_field_registry(active)
        lines = [f"Scenario: {len(active)} active event types"]

        total_defs = 0
        total_hidden = 0
        for field_name, event_specs in field_registry.items():
            dedup = SpecDeduplicator()
            result = dedup.deduplicate(field_name, event_specs)
            total_defs += len(event_specs)
            total_hidden += len(result.hidden_columns)
            lines.append(
                f"  {field_name}: {len(event_specs)} defs -> "
                f"{len(result.hidden_columns)} hidden ({result.dedup_ratio:.0%} dedup)"
            )

        ratio = 1.0 - (total_hidden / total_defs) if total_defs > 0 else 0.0
        lines.append(
            f"Totals: {total_defs} conditional field defs -> "
            f"{total_hidden} hidden columns ({ratio:.0%} dedup)"
        )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Build pipeline
    # ------------------------------------------------------------------

    def _filter_active_types(self, override: Optional[list[dict]] = None) -> list[dict]:
        """Exclude weight=0 types. Override replaces config types entirely."""
        source = override if override is not None else self._config.event_types
        return [et for et in source if float(et.get("weight", 0)) > 0]

    def _create_base_spec(
        self,
        rows_per_second: Optional[int] = None,
        total_rows: Optional[int] = None,
    ) -> dg.DataGenerator:
        mode = self._config.generation_mode
        scenario = self._config.scenario
        seed = scenario.get("seed", 42)

        if mode == "streaming":
            rate = rows_per_second or int(scenario.get("baseline_rows_per_second", 1000))
            spec = dg.DataGenerator(
                sparkSession=self._spark, name="dsg_stream", rows=rate, seedMethod="hash_fieldname"
            )
        else:
            rows = total_rows or int(scenario.get("total_rows", 10000))
            partitions = scenario.get("partitions", 8)
            spec = dg.DataGenerator(
                sparkSession=self._spark,
                name="dsg_batch",
                rows=rows,
                partitions=partitions,
                seedMethod="hash_fieldname",
            )

        spec = spec.withColumn(
            "__dsg_id", "long", minValue=0, maxValue=1000000000, random=True, omit=True
        )

        if seed != 42:
            spec = spec.option("seed", seed)

        return spec

    def _add_event_type_id(
        self, spec: dg.DataGenerator, active_types: list[dict]
    ) -> dg.DataGenerator:
        event_ids = [et["event_type_id"] for et in active_types]
        weights = [float(et["weight"]) for et in active_types]
        return spec.withColumn(
            _EVENT_TYPE_FIELD,
            "string",
            values=event_ids,
            weights=weights,
            random=True,
            omit=True,
        )

    def _add_common_fields(
        self,
        spec: dg.DataGenerator,
        active_types: list[dict],  # noqa: ARG002
    ) -> dg.DataGenerator:
        common = self._config.common_fields
        for field_name, field_spec in common.items():
            if "base_columns" in field_spec:
                continue

            if field_spec.get("event_type_id"):
                spec = spec.withColumn(
                    field_name,
                    "string",
                    expr=_EVENT_TYPE_FIELD,
                    baseColumn=_EVENT_TYPE_FIELD,
                )
                continue

            resolution = FieldBuilder.resolve_params(field_spec)

            if resolution.strategy == "complex":
                spec = self._add_complex_common_field(spec, field_name, field_spec)
            else:
                spec = self._field_builder.add_field(spec, field_name, resolution)

        return spec

    def _add_conditional_fields(
        self, spec: dg.DataGenerator, active_types: list[dict]
    ) -> dg.DataGenerator:
        field_registry = self._build_field_registry(active_types)
        common_names = set(self._config.common_fields.keys())

        for field_name, event_specs in field_registry.items():
            if field_name in common_names:
                continue

            first_spec = next(iter(event_specs.values()))
            field_type_name = first_spec.get("type")

            if field_type_name in ("struct", "array", "map"):
                spec = self._add_complex_conditional_field(spec, field_name, event_specs)
            else:
                spec = self._add_scalar_conditional_field(spec, field_name, event_specs)

        return spec

    def _add_scalar_conditional_field(
        self,
        spec: dg.DataGenerator,
        field_name: str,
        event_specs: dict[str, dict],
    ) -> dg.DataGenerator:
        """Add a scalar conditional field using spec deduplication."""
        dedup = SpecDeduplicator()
        result = dedup.deduplicate(field_name, event_specs, _EVENT_TYPE_FIELD)

        for hc in result.hidden_columns:
            resolution = FieldBuilder.resolve_params(hc.field_spec)
            spec = self._field_builder.add_field(spec, hc.name, resolution, hidden=True)
            self._hidden_col_count += 1

        first_spec = next(iter(event_specs.values()))
        from dblstreamgen.scenario.field_builder import _resolve_spark_type

        spark_type = _resolve_spark_type(first_spec)

        has_outliers = any(fs.get("outliers") for fs in event_specs.values())
        has_percent_nulls = any(fs.get("percent_nulls", 0) > 0 for fs in event_specs.values())

        final_expr = result.routing_expr

        if has_outliers or has_percent_nulls:
            final_expr = self._wrap_conditional_nulls_and_outliers(event_specs, result, first_spec)

        spec = spec.withColumn(
            field_name,
            spark_type,
            expr=final_expr,
            baseColumn=list(result.base_columns),
        )

        logger.info(
            "Spec dedup for '%s': %d defs -> %d hidden columns (%.0f%% dedup)",
            field_name,
            len(event_specs),
            len(result.hidden_columns),
            result.dedup_ratio * 100,
        )

        return spec

    def _wrap_conditional_nulls_and_outliers(
        self,
        event_specs: dict[str, dict],
        dedup_result: Any,
        first_spec: dict,  # noqa: ARG002
    ) -> str:
        """Build CASE WHEN that includes per-event outlier and null wrapping."""
        branches = []
        sig_to_hidden: dict = {}
        for hc in dedup_result.hidden_columns:
            sig = SpecDeduplicator.compute_signature(hc.field_spec)
            sig_to_hidden[sig] = hc.name

        for event_type_id, field_spec in event_specs.items():
            sig = SpecDeduplicator.compute_signature(field_spec)
            hidden_name = sig_to_hidden[sig]

            val_expr = hidden_name
            outliers = field_spec.get("outliers", [])
            if outliers:
                total_pct = sum(o["percent"] for o in outliers)
                if len(outliers) == 1:
                    val_expr = (
                        f"CASE WHEN rand() < {total_pct} "
                        f"THEN {outliers[0]['expr']} ELSE {val_expr} END"
                    )
                else:
                    inner = "CASE "
                    cumulative = 0.0
                    for o in outliers:
                        cumulative += o["percent"] / total_pct
                        inner += f"WHEN rand() < {cumulative} THEN {o['expr']} "
                    inner += f"ELSE {outliers[-1]['expr']} END"
                    val_expr = f"CASE WHEN rand() < {total_pct} THEN {inner} ELSE {val_expr} END"

            pn = field_spec.get("percent_nulls", 0)
            if pn and pn > 0:
                val_expr = f"CASE WHEN rand() < {pn} THEN NULL ELSE {val_expr} END"

            branches.append(f"WHEN {_EVENT_TYPE_FIELD} = '{event_type_id}' THEN {val_expr}")

        return "CASE " + " ".join(branches) + " ELSE NULL END"

    def _add_derived_fields(self, spec: dg.DataGenerator) -> dg.DataGenerator:
        """Add derived fields (common_fields with base_columns) in topo order."""
        derived = {}
        for name, field_spec in self._config.common_fields.items():
            if "base_columns" in field_spec:
                derived[name] = field_spec

        if not derived:
            return spec

        order = self._topo_sort(derived)

        for name in order:
            field_spec = derived[name]
            resolution = FieldBuilder.resolve_params(field_spec)
            base = field_spec.get("base_columns", [])
            if resolution.strategy == "sql_expr" and base:
                spec = spec.withColumn(
                    name,
                    resolution.spark_type,
                    expr=resolution.sql_expr,
                    baseColumn=base,
                )
            else:
                spec = self._field_builder.add_field(spec, name, resolution)

        return spec

    def _topo_sort(self, derived: dict[str, dict]) -> list[str]:
        """Topological sort on base_columns dependencies. Raises on cycles."""
        in_degree: dict[str, int] = dict.fromkeys(derived, 0)
        edges: dict[str, list[str]] = {n: [] for n in derived}

        for name, spec in derived.items():
            for dep in spec.get("base_columns", []):
                if dep in derived:
                    edges[dep].append(name)
                    in_degree[name] += 1

        queue = [n for n, d in in_degree.items() if d == 0]
        result: list = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for neighbor in edges[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(derived):
            remaining = set(derived) - set(result)
            raise ConfigurationError(
                f"Circular dependency in derived fields: {remaining}.  "
                f"Check base_columns references."
            )

        return result

    # ------------------------------------------------------------------
    # Complex types
    # ------------------------------------------------------------------

    def _add_complex_common_field(
        self, spec: dg.DataGenerator, field_name: str, field_spec: dict
    ) -> dg.DataGenerator:
        """Handle struct/array/map common fields."""
        field_type = field_spec.get("type")
        from dblstreamgen.scenario.field_builder import _resolve_spark_type

        spark_type = _resolve_spark_type(field_spec)

        if field_type == "struct":
            spec, struct_expr, gen_cols = self._build_struct(
                spec, field_name, field_spec, event_types=[]
            )
            spec = spec.withColumn(field_name, spark_type, expr=struct_expr, baseColumn=gen_cols)
        elif field_type == "array":
            spec, array_expr, gen_cols = self._build_array(
                spec, field_name, field_spec, event_types=[]
            )
            spec = spec.withColumn(field_name, spark_type, expr=array_expr, baseColumn=gen_cols)
        elif field_type == "map":
            spec, map_expr, gen_cols = self._build_map(spec, field_name, field_spec, event_types=[])
            base = gen_cols if gen_cols else [_EVENT_TYPE_FIELD]
            spec = spec.withColumn(field_name, spark_type, expr=map_expr, baseColumn=base)

        return spec

    def _add_complex_conditional_field(
        self,
        spec: dg.DataGenerator,
        field_name: str,
        event_specs: dict[str, dict],
    ) -> dg.DataGenerator:
        """Handle conditional struct/array/map fields."""
        first_spec = next(iter(event_specs.values()))
        field_type = first_spec.get("type")
        from dblstreamgen.scenario.field_builder import _resolve_spark_type

        spark_type = _resolve_spark_type(first_spec)
        event_types = list(event_specs.keys())

        if field_type == "struct":
            spec, inner_expr, gen_cols = self._build_struct(
                spec, field_name, first_spec, event_types
            )
        elif field_type == "array":
            spec, inner_expr, gen_cols = self._build_array(
                spec, field_name, first_spec, event_types
            )
        elif field_type == "map":
            spec, inner_expr, gen_cols = self._build_map(spec, field_name, first_spec, event_types)
        else:
            raise ConfigurationError(f"Unknown complex type: {field_type}")

        if len(event_types) == 1:
            cond = f"{_EVENT_TYPE_FIELD} = '{event_types[0]}'"
        else:
            id_list = "', '".join(event_types)
            cond = f"{_EVENT_TYPE_FIELD} IN ('{id_list}')"

        expr = f"CASE WHEN {cond} THEN {inner_expr} ELSE NULL END"
        # Deduplicate in case _EVENT_TYPE_FIELD was already added by _build_array.
        base_cols = list(dict.fromkeys([_EVENT_TYPE_FIELD] + gen_cols))
        spec = spec.withColumn(field_name, spark_type, expr=expr, baseColumn=base_cols)

        return spec

    def _build_struct(
        self,
        spec: dg.DataGenerator,
        prefix: str,
        field_spec: dict,
        event_types: list,
    ) -> tuple:
        """Generate scalar columns and return (spec, named_struct_expr, gen_cols)."""
        struct_fields = field_spec.get("fields", {})
        parts = []
        gen_cols: list = []

        for sub_name, sub_spec in struct_fields.items():
            col_name = f"{prefix}_{sub_name}"
            sub_type = sub_spec.get("type")

            if sub_type == "struct":
                spec, sub_expr, sub_gen = self._build_struct(spec, col_name, sub_spec, event_types)
                parts.append(f"'{sub_name}', {sub_expr}")
                gen_cols.extend(sub_gen)
            elif sub_type == "array":
                spec, sub_expr, sub_gen = self._build_array(spec, col_name, sub_spec, event_types)
                parts.append(f"'{sub_name}', {sub_expr}")
                gen_cols.extend(sub_gen)
            else:
                gen_cols.append(col_name)
                resolution = FieldBuilder.resolve_params(sub_spec)

                if event_types:
                    if resolution.strategy == "native":
                        spec = self._field_builder.add_field(
                            spec, col_name, resolution, hidden=True
                        )
                        self._hidden_col_count += 1
                        cond_expr = self._event_type_guard(event_types, col_name)
                        from dblstreamgen.scenario.field_builder import _resolve_spark_type

                        sub_spark = _resolve_spark_type(sub_spec)
                        temp_name = f"{col_name}__guarded"
                        spec = spec.withColumn(
                            temp_name,
                            sub_spark,
                            expr=cond_expr,
                            baseColumn=[_EVENT_TYPE_FIELD, col_name],
                            omit=True,
                        )
                        gen_cols.append(temp_name)
                        parts.append(f"'{sub_name}', {temp_name}")
                        self._hidden_col_count += 1
                        continue
                    elif resolution.strategy == "sql_expr":
                        cond_expr = self._event_type_guard(event_types, resolution.sql_expr)
                        from dblstreamgen.scenario.field_builder import _resolve_spark_type

                        sub_spark = _resolve_spark_type(sub_spec)
                        spec = spec.withColumn(
                            col_name,
                            sub_spark,
                            expr=cond_expr,
                            baseColumn=_EVENT_TYPE_FIELD,
                            omit=True,
                        )
                        self._hidden_col_count += 1
                        parts.append(f"'{sub_name}', {col_name}")
                        continue
                    elif resolution.strategy == "faker":
                        spec = self._field_builder.add_field(
                            spec, col_name, resolution, hidden=True
                        )
                        self._hidden_col_count += 1
                        parts.append(f"'{sub_name}', {col_name}")
                        continue

                spec = self._field_builder.add_field(spec, col_name, resolution, hidden=True)
                self._hidden_col_count += 1
                parts.append(f"'{sub_name}', {col_name}")

        struct_expr = f"named_struct({', '.join(parts)})"
        return spec, struct_expr, gen_cols

    def _build_array(
        self,
        spec: dg.DataGenerator,
        prefix: str,
        field_spec: dict,
        event_types: list,  # noqa: ARG002 -- kept for API symmetry
    ) -> tuple:
        """Generate element columns and return (spec, array_expr, gen_cols)."""
        item_type = field_spec.get("item_type", "string")
        num_features = field_spec.get("num_features", [1, 5])
        values = field_spec.get("values", [])

        if isinstance(num_features, list):
            min_size, max_size = num_features[0], num_features[1]
        else:
            min_size = max_size = num_features

        gen_cols: list = []
        elem_cols: list = []

        for i in range(max_size):
            col_name = f"{prefix}_elem_{i}"
            elem_cols.append(col_name)
            gen_cols.append(col_name)

            elem_spec: dict = {"type": item_type}
            if values:
                elem_spec["values"] = values
            if field_spec.get("range"):
                elem_spec["range"] = field_spec["range"]
            if field_spec.get("weights"):
                elem_spec["weights"] = field_spec["weights"]

            resolution = FieldBuilder.resolve_params(elem_spec)
            spec = self._field_builder.add_field(spec, col_name, resolution, hidden=True)
            self._hidden_col_count += 1

        elements_str = ", ".join(elem_cols)
        if min_size == max_size:
            array_expr = f"array({elements_str})"
        else:
            # Use __dsg_id (a per-row random long) so array length varies per row.
            # Using __dsg_event_type_id here would give a fixed length per event type
            # because there are only N distinct event type strings to hash.
            if "__dsg_id" not in gen_cols:
                gen_cols.append("__dsg_id")
            size_expr = f"(abs(hash(__dsg_id)) % {max_size - min_size + 1}) + {min_size}"
            array_expr = f"slice(array({elements_str}), 1, {size_expr})"

        return spec, array_expr, gen_cols

    def _build_map(
        self,
        spec: dg.DataGenerator,
        prefix: str,
        field_spec: dict,
        event_types: list,  # noqa: ARG002 -- kept for API symmetry
    ) -> tuple:
        """Generate value columns and return (spec, map_expr, gen_cols)."""
        values = field_spec.get("values", [])
        weights = field_spec.get("weights")

        if not values:
            return spec, "map()", []

        gen_cols: list = []

        if len(values) > 1:
            rand_col = f"__dsg_rand_{prefix}"
            spec = spec.withColumn(rand_col, "float", expr="rand()", omit=True)
            gen_cols.append(rand_col)
            self._hidden_col_count += 1
            rand_ref = rand_col
        else:
            rand_ref = "rand()"

        if weights:
            total_weight = sum(weights)
            cumulative = 0.0
            sql_expr = "CASE "
            for vd, w in zip(values, weights):
                cumulative += w / total_weight
                pairs = [f"'{k}', '{v}'" for k, v in vd.items()]
                sql_expr += f"WHEN {rand_ref} < {cumulative} THEN map({', '.join(pairs)}) "
            pairs = [f"'{k}', '{v}'" for k, v in values[-1].items()]
            sql_expr += f"ELSE map({', '.join(pairs)}) END"
        else:
            threshold = 1.0 / len(values)
            sql_expr = "CASE "
            for i, vd in enumerate(values):
                pairs = [f"'{k}', '{v}'" for k, v in vd.items()]
                sql_expr += f"WHEN {rand_ref} < {(i + 1) * threshold} THEN map({', '.join(pairs)}) "
            pairs = [f"'{k}', '{v}'" for k, v in values[-1].items()]
            sql_expr += f"ELSE map({', '.join(pairs)}) END"

        return spec, sql_expr, gen_cols

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_field_registry(self, active_types: list[dict]) -> dict[str, dict[str, dict]]:
        """Build field_name -> {event_type_id: field_spec} registry."""
        registry: dict[str, dict[str, dict]] = {}
        for et in active_types:
            eid = et["event_type_id"]
            for fname, fspec in et.get("fields", {}).items():
                registry.setdefault(fname, {})[eid] = fspec
        return registry

    def _build_dataframe(
        self,
        spec: dg.DataGenerator,
        rows_per_second: Optional[int],
        total_rows: Optional[int],  # noqa: ARG002 -- reserved for batch override
    ) -> DataFrame:
        mode = self._config.generation_mode
        if mode == "streaming":
            rate = rows_per_second or int(
                self._config.scenario.get("baseline_rows_per_second", 1000)
            )
            return spec.build(
                withStreaming=True,
                options={"rowsPerSecond": rate, "rampUpTimeSeconds": 0},
            )
        return spec.build()

    @staticmethod
    def _event_type_guard(event_types: list, inner_expr: str) -> str:
        if len(event_types) == 1:
            cond = f"{_EVENT_TYPE_FIELD} = '{event_types[0]}'"
        else:
            id_list = "', '".join(event_types)
            cond = f"{_EVENT_TYPE_FIELD} IN ('{id_list}')"
        return f"CASE WHEN {cond} THEN {inner_expr} ELSE NULL END"
