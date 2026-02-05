# Building and Distributing dblstreamgen

This guide explains how to build a wheel distribution of `dblstreamgen` for installation and distribution.

## Tool Options

The project uses `hatchling` as the build backend. You have several options for managing the full wheel lifecycle:

### Option 1: Hatch (Recommended - Already Using Hatchling)

Since you're using `hatchling` as the build backend, **Hatch** is the natural choice for full lifecycle management:

```bash
# Install hatch (one-time setup)
pipx install hatch  # Or: brew install hatch

# No need for virtual environments - Hatch manages them automatically!
```

**Advantages:**
- ✅ Zero configuration needed (already set up in `pyproject.toml`)
- ✅ Automatic virtual environment management
- ✅ Built-in version bumping
- ✅ Environment matrix for testing
- ✅ Integrated build and publish commands

### Option 2: Poetry

Popular alternative with dependency locking:

```bash
pipx install poetry
```

**Note:** Would require converting `pyproject.toml` to Poetry format.

### Option 3: PDM

Modern tool with PEP 582 support:

```bash
pipx install pdm
```

**Note:** Would require minimal `pyproject.toml` changes.

### Option 4: Traditional Build + Twine

Manual approach without full lifecycle management:

```bash
pip install build twine
```

**Use when:** You want maximum control and minimal tooling.

## Building the Wheel

### Using Hatch (Recommended)

Hatch provides the simplest workflow since you're already using `hatchling`:

```bash
# Build both wheel and sdist
hatch build

# Build wheel only (faster)
hatch build --target wheel

# Clean and build
hatch clean && hatch build
```

**Output:** `dist/dblstreamgen-0.1.0-py3-none-any.whl`

### Using python -m build (Traditional)

Standard approach using the `build` module:

```bash
# Install build tool first
pip install build

# Build both formats
python -m build

# Build wheel only
python -m build --wheel
```

This creates:
- `dblstreamgen-0.1.0-py3-none-any.whl` (wheel)
- `dblstreamgen-0.1.0.tar.gz` (source distribution)

## What's Included in the Wheel

The current configuration (`pyproject.toml`) includes:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/dblstreamgen"]
```

This packages:
- ✅ All Python source code from `src/dblstreamgen/`
- ✅ `__init__.py` files for module imports
- ❌ Sample configs (`sample/configs/*.yaml`) - **NOT included by default**
- ❌ Documentation (`docs/`) - **NOT included by default**
- ❌ Notebooks (`sample/notebooks/`) - **NOT included by default**

## Including Sample Configs and Docs (Optional)

If you want to bundle sample configs and documentation with the wheel, update `pyproject.toml`:

### Option 1: Include as Package Data

Add to your `pyproject.toml`:

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/dblstreamgen"]

[tool.hatch.build.targets.wheel.shared-data]
"sample/configs" = "share/dblstreamgen/configs"
"docs" = "share/dblstreamgen/docs"
```

**Installed location**: `/path/to/site-packages/../share/dblstreamgen/`

### Option 2: Include in Package (Easier Access)

Create `src/dblstreamgen/sample_configs/` and move configs there:

```bash
mkdir -p src/dblstreamgen/sample_configs
cp sample/configs/*.yaml src/dblstreamgen/sample_configs/
```

Then users can access them in Python:

```python
import dblstreamgen
import os

config_dir = os.path.join(os.path.dirname(dblstreamgen.__file__), 'sample_configs')
config_path = os.path.join(config_dir, 'extended_types_config.yaml')
```

### Option 3: Keep Separate (Recommended)

Keep sample configs and docs in the repository/documentation site, not in the wheel. Users can:
- Clone the repo for examples
- View docs on GitHub/documentation site
- Download specific configs as needed

This keeps the wheel small and focused on the library code.

## Installing the Built Wheel

### Local Installation

After building:

```bash
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl
```

### Editable Installation (Development)

For development, install in editable mode:

```bash
pip install -e .
```

This allows you to make code changes without reinstalling.

### With Optional Dependencies

```bash
# Install with Kinesis support
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[kinesis]

# Install with Kafka support
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[kafka]

# Install with all optional dependencies
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[all]

# Install with dev dependencies (testing, linting)
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl[dev]
```

## Verifying the Build

### Inspect Wheel Contents

```bash
# List files in the wheel
unzip -l dist/dblstreamgen-0.1.0-py3-none-any.whl

# Or use wheel
pip install wheel
wheel unpack dist/dblstreamgen-0.1.0-py3-none-any.whl
```

### Test Installation

```bash
# Create a test environment
python -m venv test_env
source test_env/bin/activate
pip install dist/dblstreamgen-0.1.0-py3-none-any.whl

# Test import
python -c "from dblstreamgen import load_config, StreamOrchestrator; print('Success!')"

# Deactivate when done
deactivate
```

## Publishing to PyPI

### Test PyPI (Recommended First)

```bash
pip install twine

# Upload to Test PyPI
twine upload --repository testpypi dist/*

# Test installation from Test PyPI
pip install --index-url https://test.pypi.org/simple/ dblstreamgen
```

### Production PyPI

```bash
# Upload to PyPI (requires authentication)
twine upload dist/*
```

**Authentication**: You'll need a PyPI account and API token. Configure in `~/.pypirc`:

```ini
[pypi]
  username = __token__
  password = pypi-AgEIcHlwaS5vcmc...  # Your API token
```

## Full Lifecycle Management with Hatch

### Complete Workflow

```bash
# 1. Install hatch (one-time)
pipx install hatch

# 2. Run tests in isolated environment
hatch run pytest

# 3. Bump version (automatically updates pyproject.toml)
hatch version patch    # 0.1.0 -> 0.1.1
hatch version minor    # 0.1.1 -> 0.2.0
hatch version major    # 0.2.0 -> 1.0.0

# 4. Build wheel
hatch build

# 5. Publish to PyPI
hatch publish

# 6. Publish to Test PyPI first (recommended)
hatch publish -r test
```

### Development with Hatch

```bash
# Create and enter a development environment
hatch shell

# Run commands in the environment (without entering it)
hatch run python -c "from dblstreamgen import __version__; print(__version__)"

# Run linters
hatch run ruff check src/
hatch run mypy src/

# Run tests with coverage
hatch run pytest --cov

# Format code
hatch run ruff format src/
```

### Environment Management

Hatch automatically manages virtual environments. No need to create/activate venvs!

```bash
# Show all environments
hatch env show

# Remove all environments (clean slate)
hatch env prune

# Run in specific Python version
hatch run python:3.11 pytest
```

### Versioning

#### Automatic Version Bumping

```bash
# Check current version
hatch version

# Bump patch (0.1.0 -> 0.1.1)
hatch version patch

# Bump minor (0.1.0 -> 0.2.0)
hatch version minor

# Bump major (0.1.0 -> 1.0.0)
hatch version major

# Set specific version
hatch version 1.2.3

# Preview what would change (dry-run)
hatch version minor --dry-run
```

#### Manual Version Update

Edit `pyproject.toml`:

```toml
[project]
version = "0.2.0"  # Update this
```

Then rebuild:

```bash
hatch clean && hatch build
```

## Advanced Hatch Configuration

### Define Test Environments

Add to `pyproject.toml`:

```toml
[tool.hatch.envs.default]
dependencies = [
  "pytest>=7.0",
  "pytest-cov>=4.0",
  "ruff>=0.1.0",
  "mypy>=1.0",
]

[tool.hatch.envs.default.scripts]
test = "pytest {args:tests}"
test-cov = "pytest --cov-report=term-missing --cov-config=pyproject.toml --cov=src/dblstreamgen {args:tests}"
lint = "ruff check {args:src tests}"
format = "ruff format {args:src tests}"
type-check = "mypy {args:src}"

# Matrix testing across Python versions
[[tool.hatch.envs.test.matrix]]
python = ["3.9", "3.10", "3.11", "3.12"]
```

**Usage:**

```bash
# Run tests
hatch run test

# Run tests with coverage
hatch run test-cov

# Lint code
hatch run lint

# Format code
hatch run format

# Type check
hatch run type-check

# Test on all Python versions
hatch run test:test
```

### Configure PyPI Repositories

Add to `pyproject.toml`:

```toml
[tool.hatch.publish.index]
repos = { test = "https://test.pypi.org/legacy/" }
```

Create `~/.pypirc` for authentication:

```ini
[distutils]
index-servers =
    pypi
    test

[pypi]
username = __token__
password = pypi-AgEI...  # Your PyPI API token

[test]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-AgEI...  # Your Test PyPI API token
```

## CI/CD Integration

### GitHub Actions with Hatch

Create `.github/workflows/build.yml`:

```yaml
name: Build and Publish

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
  release:
    types: [published]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ['3.9', '3.10', '3.11', '3.12']
    
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      
      - name: Install Hatch
        run: pipx install hatch
      
      - name: Run tests
        run: hatch run test-cov
      
      - name: Run linters
        run: hatch run lint
      
      - name: Type check
        run: hatch run type-check

  build:
    needs: test
    runs-on: ubuntu-latest
    if: github.event_name == 'release'
    
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install Hatch
        run: pipx install hatch
      
      - name: Build package
        run: hatch build
      
      - name: Publish to PyPI
        run: hatch publish
        env:
          HATCH_INDEX_USER: __token__
          HATCH_INDEX_AUTH: ${{ secrets.PYPI_API_TOKEN }}
```

### GitHub Actions (Traditional)

Using `build` and `twine`:

```yaml
name: Build and Publish

on:
  release:
    types: [published]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install build tools
        run: pip install build twine
      
      - name: Build wheel
        run: python -m build
      
      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          password: ${{ secrets.PYPI_API_TOKEN }}
```

## Troubleshooting

### Build Fails

```bash
# Clean and rebuild
rm -rf dist/ build/ src/*.egg-info/
python -m build
```

### Import Errors After Installation

Check that `src/dblstreamgen/__init__.py` exports the main classes:

```python
from dblstreamgen.config import load_config, StreamConfig
from dblstreamgen.orchestrator.stream_orchestrator import StreamOrchestrator

__all__ = ["load_config", "StreamConfig", "StreamOrchestrator"]
```

### Missing Dependencies

Ensure all dependencies are listed in `pyproject.toml` under `dependencies` or `optional-dependencies`.

## Current Wheel Info

**Package**: `dblstreamgen`  
**Version**: `0.1.0`  
**Size**: ~50-100 KB (code only)  
**Python**: >=3.9  
**Platform**: Any (pure Python)  
**Dependencies**:
- `pyyaml>=6.0`
- `pyspark>=3.3.0`
- `dbldatagen>=0.3.0`

## Best Practices

1. **Version Control**: Tag releases in git matching wheel versions
2. **Changelog**: Update CHANGELOG.md before each release
3. **Test**: Always test the wheel in a clean environment before publishing
4. **Documentation**: Keep README.md in sync with features
5. **Semantic Versioning**: Follow semver (MAJOR.MINOR.PATCH)
   - MAJOR: Breaking changes
   - MINOR: New features (backward compatible)
   - PATCH: Bug fixes

## Tool Comparison

| Feature | Hatch | Poetry | PDM | Build+Twine |
|---------|-------|--------|-----|-------------|
| **Already configured** | ✅ Yes | ❌ No | ❌ No | ⚠️ Partial |
| **Version bumping** | ✅ Built-in | ✅ Built-in | ✅ Built-in | ❌ Manual |
| **Environment mgmt** | ✅ Automatic | ✅ Automatic | ✅ Automatic | ❌ Manual |
| **Dependency locking** | ❌ No | ✅ Yes | ✅ Yes | ❌ No |
| **Build speed** | ⚡ Fast | ⚡ Fast | ⚡ Fast | ⚡ Fast |
| **Publishing** | ✅ `hatch publish` | ✅ `poetry publish` | ✅ `pdm publish` | ✅ `twine upload` |
| **Test matrix** | ✅ Yes | ⚠️ Via tox | ⚠️ Via tox | ❌ Manual |
| **Learning curve** | 🟢 Low | 🟡 Medium | 🟡 Medium | 🟢 Low |
| **Configuration** | 📝 pyproject.toml | 📝 pyproject.toml | 📝 pyproject.toml | 📝 pyproject.toml |

**Recommendation for this project:** Use **Hatch** - it's already configured and requires zero changes.

## Quick Reference

### Hatch Workflow (Recommended)

```bash
# One-time setup
pipx install hatch

# Development
hatch shell                  # Enter dev environment
hatch run test               # Run tests
hatch run lint               # Lint code

# Version bump
hatch version patch          # 0.1.0 -> 0.1.1

# Build and publish
hatch clean                  # Clean old builds
hatch build                  # Build wheel
hatch publish -r test        # Publish to Test PyPI
hatch publish                # Publish to PyPI

# All-in-one release
hatch version minor && hatch clean && hatch build && hatch publish
```

### Traditional Workflow

```bash
# Setup
pip install build twine

# Build
rm -rf dist/ build/
python -m build

# Validate
twine check dist/*

# Test locally
pip install dist/*.whl

# Publish
twine upload --repository testpypi dist/*  # Test PyPI
twine upload dist/*                         # Production PyPI
```

### Poetry Workflow (If Migrating)

```bash
# Setup
pipx install poetry
poetry init  # Convert project

# Development
poetry install
poetry shell

# Version bump
poetry version patch

# Build and publish
poetry build
poetry publish --repository testpypi
poetry publish
```

### PDM Workflow (If Migrating)

```bash
# Setup
pipx install pdm
pdm init  # Convert project

# Development
pdm install
pdm run python

# Version bump
pdm bump patch

# Build and publish
pdm build
pdm publish --repository testpypi
pdm publish
```

### Makefile Shortcuts (Included)

A `Makefile` is included for convenience:

```bash
make help           # Show all available commands
make install        # Install hatch
make test           # Run tests
make test-cov       # Run tests with coverage
make lint           # Run linter
make format         # Format code
make clean          # Clean build artifacts
make build          # Build wheel
make version-patch  # Bump patch version
make publish-test   # Publish to Test PyPI
make publish        # Publish to PyPI
make release-patch  # Full release workflow (test, bump, build, publish)
```

**Example full release:**

```bash
# Run all checks, bump version, build, and publish to Test PyPI
make release-patch

# After testing, publish to production
make publish
```
