# dblstreamgen

**Databricks stream data generation for harness testing and performance validation**

`dblstreamgen` is a Python library for generating synthetic streaming data to test and validate Databricks data pipelines. It supports both continuous stream generation (Python-based) and high-throughput batch generation (Spark/dbldatagen-based) with publishers for Kinesis and Kafka.

## Features

- 🌊 **Dual Generation Modes**
  - Stream Generator: Continuous Python-based generation (100-5K events/sec)
  - Batch Generator: High-throughput Spark-based generation (10K-100K+ events/sec)

- 📤 **Multiple Publishers**
  - AWS Kinesis
  - Apache Kafka
  - Extensible for other streaming platforms

- ⚙️ **Configuration-Driven**
  - YAML-based configuration
  - Power-law event distributions
  - Customizable schemas and payloads

- 🔧 **Databricks Native**
  - Seamless Spark session integration
  - DBUtils and secrets support
  - DBFS and Workspace Repos path handling

## Installation

### For Development (Local Wheel)

```bash
# Clone the repository
git clone https://github.com/databrickslabs/dblstreamgen.git
cd dblstreamgen

# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Build the wheel
./scripts/build-wheel.sh

# Install in your environment
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl

# Or with optional dependencies
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[all]
```

### For Production (PyPI - Coming Soon)

```bash
pip install dblstreamgen[all]
```

## Quick Start

### Python Script

```python
import dblstreamgen
import time

# Load configuration
config = dblstreamgen.load_config(
    generation_config='config_generation.yaml',
    source_config='config_source_kinesis.yaml'
)

# Create generator and publisher
generator = dblstreamgen.StreamGenerator(config=config)
publisher = dblstreamgen.KinesisPublisher(config=config['kinesis_config'])

# Generate and publish events (you control the orchestration)
start_time = time.time()
total_events = 0

for event in generator.generate():
    publisher.publish_single(event)
    total_events += 1
    
    # Stop after 1 hour
    if time.time() - start_time > 3600:
        generator.stop()
        break

publisher.close()
print(f"✅ Generated {total_events:,} events")
```

### Databricks Notebook

```python
# Install the wheel
%pip install /Workspace/Repos/.../dblstreamgen-0.1.0-py3-none-any.whl[all]

import dblstreamgen
from pyspark.sql import SparkSession
import time

# Get Spark session (already available in Databricks)
spark = SparkSession.getActiveSession()

# Load configuration
config = dblstreamgen.load_config(
    '/Workspace/Repos/.../configs/config_generation.yaml',
    '/Workspace/Repos/.../configs/config_source_kinesis.yaml'
)

# Use Batch Generator for high throughput
generator = dblstreamgen.BatchGenerator(spark=spark, config=config)
publisher = dblstreamgen.KinesisPublisher(config=config['kinesis_config'])

# Generate and publish batches
start_time = time.time()
total_events = 0

for batch_df in generator.generate():
    result = publisher.publish_batch(batch_df)
    total_events += result.records_sent
    
    print(f"Progress: {total_events:,} events sent")
    
    # Stop after 1 hour
    if time.time() - start_time > 3600:
        generator.stop()
        break

publisher.close()
print(f"✅ Complete: {total_events:,} events in {time.time()-start_time:.1f}s")
```

## Development

### Setup

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # or on Windows: .venv\Scripts\activate
uv pip install -e ".[dev]"
```

### Code Quality

```bash
# Format code
ruff format .

# Lint
ruff check .

# Auto-fix linting issues
ruff check --fix .

# Type check
mypy src/

# Run all checks at once
./scripts/check.sh
```

### Testing

```bash
# Run tests
pytest

# With coverage
pytest --cov=dblstreamgen --cov-report=html --cov-report=term

# Run specific test file
pytest tests/test_generators.py

# Run tests with markers
pytest -m "not slow"
```

### Build Wheel

```bash
# Build wheel for distribution
uv build

# Or use the convenience script
./scripts/build-wheel.sh

# Output: dist/dblstreamgen-0.1.0-py3-none-any.whl
```

### Common Commands

```bash
# Install in editable mode (for development)
uv pip install -e ".[dev]"

# Build wheel
uv build

# Run tests
pytest

# Format and lint
ruff format . && ruff check .

# Full quality check
./scripts/check.sh
```

## Project Structure

```
dblstreamgen/
├── src/dblstreamgen/          # Source code
│   ├── generators/            # Stream & Batch generators
│   ├── publishers/            # Kinesis, Kafka publishers
│   ├── schema/               # Schema management
│   └── utils/                # Utilities
├── tests/                    # Unit tests
├── docs/                     # Documentation
├── scripts/                  # Build & development scripts
└── pyproject.toml           # Project configuration
```

## Configuration

See the [Configuration Guide](docs/configuration.md) for detailed information on:
- Event type definitions
- Distribution weights
- Publisher settings
- Databricks secrets integration

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

## Roadmap

- [x] Core stream and batch generation
- [x] Kinesis and Kafka publishers
- [x] Databricks integration
- [ ] Advanced customization (geographic, temporal patterns)
- [ ] Schema registry integration
- [ ] PyPI publication
- [ ] CI/CD pipeline

