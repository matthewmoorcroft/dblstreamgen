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

# Using Makefile
make test

# Using pytest directly (if installed)
pytest
```

## 🔨 Build Wheel

```bash
# Using Hatch
hatch build

# Using Makefile
make build

# Using build module
python -m build
```

**Output:** `dist/dblstreamgen-0.1.0-py3-none-any.whl`

## 📝 Development Workflow

### Using Hatch (Recommended)

```bash
# 1. Make your changes to code

# 2. Run all checks
hatch run all           # Runs: format, lint, type-check, test-cov

# 3. Bump version
hatch version patch     # 0.1.0 -> 0.1.1

# 4. Build and publish
hatch build
hatch publish -r test   # Test PyPI first
hatch publish           # Production PyPI
```

### Using Makefile (Even Easier!)

```bash
# 1. Make your changes

# 2. Run all checks
make all

# 3. Full release workflow
make release-patch      # Tests, bumps, builds, publishes to Test PyPI

# 4. After testing, publish to production
make publish
```

## 🎯 Common Commands

### Development

```bash
# Format code
hatch run format        # or: make format

# Lint code
hatch run lint          # or: make lint

# Type check
hatch run type-check    # or: make type-check

# Run tests with coverage
hatch run test-cov      # or: make test-cov
```

### Versioning

```bash
# Current version
hatch version

# Bump versions
hatch version patch     # 0.1.0 -> 0.1.1
hatch version minor     # 0.1.0 -> 0.2.0
hatch version major     # 0.1.0 -> 1.0.0

# Or use Makefile
make version-patch
make version-minor
make version-major
```

### Building

```bash
# Clean old builds
hatch clean             # or: make clean

# Build wheel
hatch build             # or: make build

# Build specific target
hatch build --target wheel
```

### Publishing

```bash
# Publish to Test PyPI
hatch publish -r test   # or: make publish-test

# Publish to PyPI
hatch publish           # or: make publish
```

## 📦 Installing Your Built Wheel

### Locally

```bash
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl
```

### With Optional Dependencies

```bash
# Kinesis support
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[kinesis]

# Kafka support
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[kafka]

# All optional dependencies
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[all]

# Development dependencies
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[dev]
```

## 🧩 Example Usage

After installing the wheel:

```python
from dblstreamgen import load_config, StreamOrchestrator
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("test").getOrCreate()

# Load config
config = load_config('sample/configs/extended_types_config.yaml')

# Create orchestrator
orchestrator = StreamOrchestrator(spark, config)

# Generate data (wide schema for file/table sinks)
df = orchestrator.create_unified_stream(serialize=False)

# Or serialize for Kinesis/Kafka
df_serialized = orchestrator.create_unified_stream(serialize=True)

# Write to sink (using Spark's native writers)
df.writeStream \
    .format("parquet") \
    .option("path", "/path/to/output") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()
```

## 📚 More Information

- **Full Build Documentation**: [BUILD.md](BUILD.md)
- **Type System**: [docs/TYPE_SYSTEM.md](docs/TYPE_SYSTEM.md)
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

1. **Use `make help`** to see all available commands
2. **Use `hatch shell`** to enter a development environment
3. **Use `hatch run all`** before committing to run all checks
4. **Use Test PyPI** first before publishing to production
5. **Use `hatch env show`** to see all available environments
6. **Tag releases in git** to match wheel versions

Happy building! 🎉
