# Quick Start Guide

Get up and running with `dblstreamgen` in 5 minutes!

## 🚀 Installation

### Option 1: Install Hatch (Recommended for Development)

```bash
# Install hatch
pipx install hatch

# That's it! Hatch manages environments automatically
```

### Option 2: Traditional Pip Install

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install package
pip install -e .
```

## 🧪 Run Tests

```bash
# Using Hatch (no environment needed!)
hatch run test

# Using pytest directly
pytest tests/unit/          # no Spark required
pytest tests/integration/   # requires local Spark
```

## 🔨 Build Wheel

```bash
# Using Hatch
hatch build

# Using build module
python -m build
```

**Output:** `dist/dblstreamgen-0.4.0-py3-none-any.whl`

## 📝 Development Workflow

### Using Hatch (Recommended)

```bash
# 1. Make your changes to code

# 2. Run all checks
hatch run all           # Runs: format, lint, type-check, test-cov

# 3. Bump version
hatch version patch     # 0.4.0 -> 0.4.1

# 4. Build and publish
hatch build
hatch publish -r test   # Test PyPI first
hatch publish           # Production PyPI
```

## 🎯 Common Commands

### Development

```bash
# Format code
hatch run format

# Lint code
hatch run lint

# Type check
hatch run type-check

# Run tests with coverage
hatch run test-cov
```

### Versioning

```bash
# Current version
hatch version

# Bump versions
hatch version patch     # 0.4.0 -> 0.4.1
hatch version minor     # 0.4.0 -> 0.5.0
hatch version major     # 0.4.0 -> 1.0.0
```

### Building

```bash
# Clean old builds
hatch clean

# Build wheel
hatch build

# Build specific target
hatch build --target wheel
```

### Publishing

```bash
# Publish to Test PyPI
hatch publish -r test

# Publish to PyPI
hatch publish
```

## 📦 Installing Your Built Wheel

### Locally

```bash
pip install dist/dblstreamgen-0.4.0-py3-none-any.whl
```

### With Optional Dependencies

```bash
# Kinesis support
pip install dist/dblstreamgen-0.4.0-py3-none-any.whl[kinesis]

# Kafka support
pip install dist/dblstreamgen-0.4.0-py3-none-any.whl[kafka]

# All optional dependencies
pip install dist/dblstreamgen-0.4.0-py3-none-any.whl[all]

# Development dependencies
pip install dist/dblstreamgen-0.4.0-py3-none-any.whl[dev]
```

## 🧩 Example Usage

After installing the wheel:

```python
from dblstreamgen import Config, Scenario, KinesisDataSource
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

# Load and validate config
config = Config.from_yaml("sample/configs/simple_config.yaml")

# Create scenario
scenario = Scenario(spark, config)

# Build a DataFrame (wide schema -- serialize=False skips JSON serialization)
df = scenario.build(serialize=False)

# Write to Delta
df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/example") \
    .table("my_catalog.my_schema.events")

# --- Or run the full scenario (blocks for duration_seconds) ---
spark.dataSource.register(KinesisDataSource)

def kinesis_sink(df, checkpoint_path):
    return (
        df.writeStream
        .format("dblstreamgen_kinesis")
        .option("stream_name", "my-stream")
        .option("region", "us-east-1")
        .option("service_credential", "my-credential")
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="1 second")
        .start()
    )

result = scenario.run(kinesis_sink, "/tmp/checkpoints/scenario")
print(result)
```

## 📚 More Information

- **Full Build Documentation**: [BUILD.md](BUILD.md)
- **Example Configs**: [sample/configs/](sample/configs/)
- **README**: [README.md](README.md)

## 🆘 Troubleshooting

### Hatch not found

```bash
# Install with pipx (recommended)
pipx install hatch

# Or with pip
pip install --user hatch

# Or with homebrew (macOS)
brew install hatch
```

### Tests failing

```bash
# Clean and reinstall
hatch env prune
hatch run test
```

### Build errors

```bash
# Clean build artifacts
hatch clean
rm -rf dist/ build/ *.egg-info/

# Rebuild
hatch build
```

### Import errors after install

Check that your wheel includes all necessary files:

```bash
unzip -l dist/dblstreamgen-*.whl
```

## 🎓 Learn More About Hatch

Hatch documentation: https://hatch.pypa.io/

**Why Hatch?**
- ✅ Zero configuration (already set up in `pyproject.toml`)
- ✅ Automatic virtual environment management
- ✅ Built-in versioning commands
- ✅ Integrated testing across Python versions
- ✅ Simple publishing workflow
- ✅ No separate requirements files needed

## ⚡ Pro Tips

1. **Use `hatch shell`** to enter a development environment
2. **Use `hatch run all`** before committing to run all checks
3. **Use Test PyPI** first before publishing to production
4. **Use `hatch env show`** to see all available environments
5. **Tag releases in git** to match wheel versions

Happy building! 🎉
