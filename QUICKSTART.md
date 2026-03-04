# Quick Start Guide

## Installation

> **Note:** `dblstreamgen` is not yet on PyPI. Install directly from the GitHub releases page until then.

### On Databricks

```python
# In a notebook cell — install the latest wheel from GitHub releases
%pip install https://github.com/matthewmoorcroft/dblstreamgen/releases/latest/download/dblstreamgen-py3-none-any.whl
```

Or pin to a specific version:

```python
%pip install https://github.com/matthewmoorcroft/dblstreamgen/releases/download/v0.4.0/dblstreamgen-0.4.0-py3-none-any.whl
```

### Locally (development)

```bash
git clone https://github.com/matthewmoorcroft/dblstreamgen.git
cd dblstreamgen
pip install -e ".[dev]"
```

---

## Basic Usage

### 1. Define a config

```yaml
# config.yaml
generation_mode: streaming

scenario:
  duration_seconds: 300
  baseline_rows_per_second: 1000

common_fields:
  event_id:
    type: uuid
  event_time:
    type: timestamp
    mode: current

event_types:
  - event_type_id: page_view
    weight: 0.60
    fields:
      page:
        type: string
        values: [home, search, product, checkout]

  - event_type_id: click
    weight: 0.30
    fields:
      element:
        type: string
        values: [button, link, image]

  - event_type_id: purchase
    weight: 0.10
    fields:
      amount:
        type: double
        range: [5.0, 500.0]
        step: 0.01
```

### 2. Run with a Delta sink

```python
from dblstreamgen import Config, Scenario

config = Config.from_yaml("config.yaml")
scenario = Scenario(spark, config)

def delta_sink(df, checkpoint_path):
    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .table("my_catalog.my_schema.events")
    )

result = scenario.run(delta_sink, "/tmp/checkpoints/events")
print(f"Sent {result.total_rows_generated:,} rows")
```

### 3. Inspect the schema without running

```python
df = scenario.build(serialize=False)
df.printSchema()
```

---

## Development Workflow

```bash
# Run all checks (format, lint, type-check, tests)
hatch run all

# Run unit tests only (no Spark required)
pytest tests/unit/

# Run integration tests (requires local Spark / Java 17)
pytest tests/integration/ tests/characterization/

# Build wheel
hatch build
```

See [BUILD.md](BUILD.md) for the full release workflow.
