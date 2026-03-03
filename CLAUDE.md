# CLAUDE.md — dblstreamgen

Databricks streaming synthetic-data generator using `dbldatagen`. Targets Databricks Runtime (PySpark 3.3+). Must run correctly on a distributed cluster — never suggest local-only patterns.

---

## Build & Test Commands

```bash
# Install in editable mode with all dev dependencies
pip install -e ".[dev]"

# Run all unit tests (no Spark required)
pytest tests/unit/

# Run integration tests (requires local Spark via conftest.py)
pytest tests/integration/ -m spark

# Run specific test file
pytest tests/unit/test_config.py -v

# Full test suite with coverage
pytest --cov=src/dblstreamgen --cov-report=term-missing tests/

# Lint (ruff)
ruff check src/ tests/
ruff check --fix src/ tests/   # auto-fix

# Format (ruff)
ruff format src/ tests/
ruff format --check src/ tests/   # check only

# Type check (mypy)
mypy src/

# Run all checks in one command
./scripts/check.sh
```

**Ruff config**: 100-char line length, `target-version = "py39"`. Ruff rules: E, W, F, I, B, C4, UP, ARG, SIM. `E501` ignored (formatter handles it).

**Mypy config**: `disallow_untyped_defs = true`, `strict_optional = true`. `pyspark.*`, `dbldatagen.*`, `confluent_kafka.*`, `fastavro.*` have `ignore_missing_imports = true`.

**Pytest markers**: `slow`, `spark`, `integration`. Use `-m "not slow"` to skip slow tests.

---

## Architecture Backbone

### Core Philosophy: Single DataGenerator, CASE WHEN Wide Schema

The single `DataGenerator` with CASE WHEN wide schema is **intentional and benchmarked**. Do not split into per-event-type DataGenerators — benchmark data shows one `rate` source with projections is Spark's strength. The bottleneck is rate-source count, not CASE WHEN complexity (proven up to 500 event types, 1500+ columns).

### Class Hierarchy

```
Scenario (public facade)
  ├── ScenarioBuilder   → builds DataGenerator + DataFrame
  │     ├── FieldBuilder.resolve_params()  ← pure Python, no Spark imports
  │     ├── SpecDeduplicator              ← pure Python, no Spark imports
  │     └── FieldBuilder.add_field()      ← Spark wrapper
  └── ScenarioRunner    → streaming lifecycle (start/stop/sleep state machine)

Config (pure Python)  ← detected schema version v0.3 (legacy) or v0.4 (active)
serialization.py      ← wide → narrow (partition_key, data) format
sinks/kinesis_writer.py  ← PySpark DataSource for AWS Kinesis
```

### Build Pipeline (ScenarioBuilder)

Always in this order:

1. `_create_base_spec()` — `DataGenerator` with `__dsg_id` (omit=True), `seedMethod="hash_fieldname"`
2. `_add_event_type_id()` — `__dsg_event_type_id` (omit=True), float weights
3. `_add_common_fields()` — fields shared across all event types; derived fields (with `base_columns`) are skipped here
4. `_add_conditional_fields()` — per-event-type fields wrapped in CASE WHEN via `SpecDeduplicator`
5. `_add_derived_fields()` — `common_fields` with `base_columns`, topologically sorted

### FieldResolution Strategies

`FieldBuilder.resolve_params()` is **pure Python with zero Spark imports**. It returns a `FieldResolution` with one of four strategies:

| Strategy | When | Spark mechanism |
|---|---|---|
| `native` | numerics, temporal, string+values, boolean | `dbldatagen.withColumn(..., minValue/maxValue/values/weights/random=True)` |
| `sql_expr` | uuid, binary, raw `expr:`, timestamp mode=current, outliers | `withColumn(..., expr="...", baseColumn=[...])` |
| `faker` | `faker:` key in spec | `withColumn(..., text=fakerText(...))` |
| `complex` | struct, array, map | delegated back to `ScenarioBuilder._build_struct/array/map()` |

**Always prefer `native` strategy** — it uses dbldatagen's tested generation logic and seed handling.

### Spec Deduplication

`SpecDeduplicator` groups event types with identical field specs into shared hidden columns. N event types → M ≤ N hidden columns. The routing expression is a CASE WHEN referencing hidden column names (`__base_{field}_{hash8}`).

- **Hidden column budget**: warn at 500, **error at 1000** (`ConfigurationError`)
- Signature is canonical: numerics cast to `float()`, weights normalized to sum-1.0, timestamps parsed to ISO 8601
- MD5 hash of signature → 8-char suffix → hidden column name
- `SpecDeduplicator` instance is **per-build** — do not reuse across builds

### Streaming Lifecycle (ScenarioRunner)

Single-threaded sequential state machine. All timing via `time.sleep()`. No `threading.Timer`, no `asyncio`. `sink_factory` is a `Callable[[DataFrame, str], StreamingQuery]`. All active queries stopped in `finally` block on any exception.

---

## Code Style Rules

### Python

- **Python 3.9+**. Use `list[str]` not `List[str]`, `Optional[X]` not `X | None` (py3.9 compat).
- **Full type hints on all public functions and methods**. `disallow_untyped_defs = true`.
- Use `@dataclass` and `@dataclass(frozen=True)` for data containers (`FieldResolution`, `HiddenColumn`, `DeduplicationResult`, `SinkPlan`, `ScenarioResult`).
- `logging.getLogger(__name__)` at module level. Never use `print()`.
- Raise `ConfigurationError` (from `dblstreamgen.config`) for all config validation errors — not `ValueError` or `RuntimeError`.
- Raise `ValueError` for programming errors (bad arguments to internal functions).
- Import guards in `__init__.py` for Spark-heavy modules: wrap in `try/except ImportError` and assign `None` to allow pure-Python usage.

### PySpark / dbldatagen

- **Never call `.collect()`** on a DataFrame. Never iterate over rows in Python for data generation.
- **Never use Python UDFs directly** — use dbldatagen's `fakerText()` (Pandas UDF wrapper) for Faker integration.
- **Always specify `baseColumn=`** when a column's `expr=` references other generated columns — dbldatagen uses this to ensure correct Spark plan ordering.
- Use `omit=True` for all hidden/intermediate columns (`__dsg_*`, `__base_*`).
- `DataGenerator` is lazily evaluated — it is a query plan. No data is generated until `.build()` is called.
- Use `seedMethod="hash_fieldname"` on all new `DataGenerator` instances for reproducibility.
- For streaming: `spec.build(withStreaming=True, options={"rowsPerSecond": rate, "rampUpTimeSeconds": 0})`.
- For batch: `spec.build()` (no options needed).
- Partition count for batch: `scenario.get("partitions", 8)`. Do not hardcode.

### SQL Expressions

- Use a **single pre-computed `rand()` column** (`omit=True`) when a field needs weighted CASE WHEN over multiple values. Never call `rand()` multiple times in CASE branches for the same field — this causes distribution bias.
- Outlier injection pattern: gate on a single `rand() < total_pct` then dispatch to sub-CASE for multiple outliers.
- Null injection: outermost layer — `CASE WHEN rand() < pn THEN NULL ELSE {expr} END`.

---

## Config Schema

Two schema versions, detected automatically by `Config._detect_schema_version()`:

### v0.4 (active — all new work uses this)

- Has `scenario:` section (`duration_seconds`/`baseline_rows_per_second` for streaming; `total_rows` for batch)
- `event_types[].weight`: **float** summing to 1.0 (tolerance `1e-6`)
- `common_fields.{name}.base_columns`: marks a derived field (topologically sorted, added last)
- No `sink_config` — user provides `sink_factory` callable
- Serialization: optional `serialization.format: json|avro` + `serialization.partition_key_field`
- `scenario.spike` and `scenario.ramp` for load-shape testing

### v0.3 (legacy — `StreamOrchestrator`, deprecated, remove in v0.5)

- Has `streaming_config`/`batch_config`/`sink_config` sections
- `event_types[].weight`: **integer** (e.g., `[6, 3, 1]`), not float
- `StreamOrchestrator` is a deprecation shim in `__init__.py` — do not add new features here

### Reserved Names

- Reserved columns: `__dsg_id`, `__dsg_event_type_id`
- Reserved prefixes: `__dsg_faker_`, `__dsg_rand_`, `__base_`
- Config validation raises `ConfigurationError` if user defines fields with these names

### Supported Field Types

`uuid`, `int`, `float`, `string`, `timestamp`, `boolean`, `long`, `double`, `date`, `decimal`, `byte`, `short`, `binary`, `array`, `struct`, `map`

---

## Library-Specific Context (dbldatagen)

- `dbldatagen>=0.3.0` is a dev/optional dependency — `from dbldatagen import fakerText` must be guarded with `try/except ImportError`.
- `fakerText(method, **kwargs)` wraps Python Faker as a Pandas UDF — it is Spark-safe and partition-aware.
- `DataGenerator.withColumn(name, type, **kwargs)` — `type` is a Spark SQL type **string** (e.g., `"string"`, `"bigint"`, `"decimal(10,2)"`, `"array<string>"`).
- `values=` + `weights=` + `random=True`: native weighted distribution. Prefer over CASE WHEN SQL for common fields.
- `minValue=` / `maxValue=` + `random=True`: uniform range. Use for numerics and temporals.
- `begin=` / `end=` + `random=True`: uniform temporal range. dbldatagen accepts ISO strings.
- `percentNulls=`: native null injection — only use when no outliers; otherwise use SQL CASE WHEN wrapping.
- `spec.option("seed", N)`: sets global seed only when `seed != 42` (42 is the dbldatagen default).

---

## Module Map

| File | Spark? | Purpose |
|---|---|---|
| `config.py` | No | YAML loading, schema detection, validation |
| `scenario/types.py` | No | `SinkPlan`, `ScenarioResult` dataclasses |
| `scenario/field_builder.py` | Thin | `resolve_params()` pure Python; `add_field()` Spark wrapper |
| `scenario/spec_dedup.py` | No | Signature → hidden column deduplication |
| `scenario/builder.py` | Yes | Assembles `DataGenerator`, builds `DataFrame` |
| `scenario/runner.py` | Yes | `StreamingQuery` lifecycle, sleep-based state machine |
| `scenario/scenario.py` | Yes | Public facade — one class for users |
| `serialization.py` | Yes | Wide → narrow (partition_key, data) format |
| `sinks/kinesis_writer.py` | Yes | `PySpark DataSource` for Kinesis |
| `orchestrator/stream_orchestrator.py` | Yes | **DEPRECATED** — v0.3 only, remove in v0.5 |

---

## Test Architecture

```
tests/
  conftest.py             # session-scoped SparkSession: local[2], shuffle.partitions=2
  unit/                   # Pure Python, no Spark, fast — test config, field_builder, spec_dedup, runner plan
  integration/            # Requires Spark — test builder.py, serialization.py
  characterization/       # Output correctness (distributions, types, null rates)
  e2e/                    # Full scenario runs
```

- **Unit tests must not import PySpark** — `FieldBuilder.resolve_params()`, `SpecDeduplicator`, and `Config` are fully testable without a JVM.
- Integration tests use `@pytest.mark.spark` and the session-scoped `spark` fixture from `conftest.py`.
- SparkSession config for tests: `local[2]`, `spark.sql.shuffle.partitions=2`, `spark.ui.enabled=false`.

---

## Key Invariants (Never Violate)

1. **One `DataGenerator` per stream rate.** Spike/ramp creates a second `DataGenerator` (second rate source), not a union.
2. **`FieldBuilder.resolve_params()` has zero Spark imports.** Keep it pure Python for unit testability.
3. **`SpecDeduplicator` has zero Spark imports.** It produces `DeduplicationResult`; `ScenarioBuilder` consumes it.
4. **Hidden column budget is hard-enforced.** 500 = warning, 1000 = `ConfigurationError`.
5. **`baseColumn=` is mandatory** when an `expr=` references other columns in the same `DataGenerator`. Omitting it causes non-deterministic ordering bugs.
6. **`config.py` has no Spark imports.** It is usable in pure-Python environments.
7. **Never `.collect()` inside generation code.** All data must stay distributed.
8. **`fakerText()` must be guarded.** It requires `dbldatagen` and `faker` — both are optional dependencies.
