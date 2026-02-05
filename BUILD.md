# Building and Distributing dblstreamgen

This guide explains how to build a wheel distribution of `dblstreamgen` for installation and distribution.

## Prerequisites

The project uses `hatchling` as the build backend (configured in `pyproject.toml`). You'll need the `build` package:

```bash
# Option 1: Using a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install build

# Option 2: Using pipx (for system-wide tool)
pipx install build
```

## Building the Wheel

### Quick Build

From the project root directory:

```bash
python -m build
```

This will create two files in the `dist/` directory:
- `dblstreamgen-0.1.0-py3-none-any.whl` (wheel)
- `dblstreamgen-0.1.0.tar.gz` (source distribution)

### Build Wheel Only

To build only the wheel (faster):

```bash
python -m build --wheel
```

### Build with Hatch (Alternative)

If you have `hatch` installed:

```bash
hatch build
```

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

## Versioning

Update version in `pyproject.toml`:

```toml
[project]
version = "0.2.0"  # Update this
```

Then rebuild:

```bash
# Clean old builds
rm -rf dist/ build/ src/*.egg-info/

# Build new version
python -m build
```

## CI/CD Integration

### GitHub Actions Example

Create `.github/workflows/build.yml`:

```yaml
name: Build and Publish

on:
  release:
    types: [published]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install build
        run: pip install build
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

## Quick Reference

```bash
# Full build and publish workflow
rm -rf dist/ build/
python -m build
twine check dist/*
pip install dist/*.whl  # Test locally
twine upload --repository testpypi dist/*  # Test PyPI
twine upload dist/*  # Production (when ready)
```
