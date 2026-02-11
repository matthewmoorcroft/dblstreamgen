# 🛠️ Tooling Guide for dblstreamgen

This project is fully configured with modern Python tooling for lifecycle management.

## 🎯 Quick Answer: Which Tool Should I Use?

```
┌─────────────────────────────────────────────────────────────┐
│                     RECOMMENDED: HATCH                       │
│  ✅ Already configured (zero additional setup)               │
│  ✅ Automatic environment management                         │
│  ✅ Built-in versioning, testing, publishing                 │
│  ✅ One-command workflows                                    │
└─────────────────────────────────────────────────────────────┘
```

## 🔧 Available Tools

### 1. Hatch (★ Recommended)

**Best for:** Complete lifecycle management with zero config

```bash
# Install once
pipx install hatch

# Common workflows
hatch run test          # Run tests
hatch run all           # Run all checks (format, lint, type, test)
hatch version patch     # Bump version 0.1.0 -> 0.1.1
hatch build             # Build wheel
hatch publish           # Publish to PyPI
```

**Why Hatch?**
- ✅ Already 100% configured in `pyproject.toml`
- ✅ No manual virtual environment management
- ✅ Unified command interface
- ✅ Test on multiple Python versions automatically
- ✅ Version bumping with one command

**Configuration:** See `[tool.hatch.*]` sections in `pyproject.toml`

---

### 2. Makefile (★ Even Easier!)

**Best for:** Quick, memorable commands

```bash
make help           # Show all commands
make test           # Run tests
make test-cov       # Tests with coverage
make lint           # Lint code
make format         # Format code
make build          # Build wheel
make release-patch  # Full release workflow
```

**Behind the scenes:** Makefile calls Hatch commands, so you get the best of both worlds!

**Configuration:** See `Makefile`

---

### 3. Traditional (build + twine)

**Best for:** Minimal tooling, maximum control

```bash
# Install tools
pip install build twine

# Build
python -m build

# Publish
twine upload dist/*
```

**Use when:** You want to manage everything manually

---

### 4. Poetry (Alternative - Not Configured)

**Best for:** Projects that need dependency locking

```bash
# Would require migration
poetry init
poetry install
poetry build
poetry publish
```

**Note:** Would require converting `pyproject.toml` to Poetry format

---

### 5. PDM (Alternative - Not Configured)

**Best for:** PEP 582 support, modern package management

```bash
# Would require setup
pdm init
pdm install
pdm build
pdm publish
```

**Note:** Would require updating `pyproject.toml`

---

## 📊 Tool Comparison Matrix

| Feature | Hatch | Makefile | Build+Twine | Poetry | PDM |
|---------|-------|----------|-------------|--------|-----|
| **Setup Required** | ✅ None | ✅ None | ⚠️ Some | ❌ Full | ❌ Full |
| **Virtual Env Management** | ✅ Auto | ⚠️ Manual | ❌ Manual | ✅ Auto | ✅ Auto |
| **Version Bumping** | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes | ✅ Yes |
| **Test Matrix** | ✅ Yes | ✅ Yes | ❌ No | ⚠️ Via tox | ⚠️ Via tox |
| **Publishing** | ✅ Built-in | ✅ Built-in | ✅ Twine | ✅ Built-in | ✅ Built-in |
| **Learning Curve** | 🟢 Low | 🟢 Very Low | 🟢 Low | 🟡 Medium | 🟡 Medium |
| **Customization** | 🟢 High | 🟢 High | 🟢 High | 🟡 Medium | 🟡 Medium |

---

## 🚀 Common Workflows Comparison

### Build Wheel

```bash
# Hatch
hatch build

# Makefile
make build

# Traditional
python -m build

# Poetry (if configured)
poetry build

# PDM (if configured)
pdm build
```

### Run Tests

```bash
# Hatch
hatch run test

# Makefile
make test

# Traditional
pytest

# Poetry (if configured)
poetry run pytest

# PDM (if configured)
pdm run pytest
```

### Bump Version

```bash
# Hatch
hatch version patch

# Makefile
make version-patch

# Traditional
# Edit pyproject.toml manually

# Poetry (if configured)
poetry version patch

# PDM (if configured)
pdm bump patch
```

### Publish to PyPI

```bash
# Hatch
hatch publish

# Makefile
make publish

# Traditional
twine upload dist/*

# Poetry (if configured)
poetry publish

# PDM (if configured)
pdm publish
```

### Full Release Workflow

```bash
# Hatch
hatch run all && hatch version patch && hatch build && hatch publish -r test

# Makefile (easiest!)
make release-patch

# Traditional
pytest && ruff check . && mypy src/ && \
  # edit version manually && \
  python -m build && \
  twine upload --repository testpypi dist/*

# Poetry (if configured)
poetry run pytest && poetry version patch && poetry build && poetry publish -r testpypi

# PDM (if configured)
pdm run pytest && pdm bump patch && pdm build && pdm publish -r testpypi
```

---

## 💡 Recommendations by Use Case

### 🎯 "I just want to build a wheel quickly"

**Use Makefile:**
```bash
make build
```

**Or Hatch:**
```bash
hatch build
```

---

### 🔄 "I need to do full development (test, lint, build, publish)"

**Use Hatch with predefined scripts:**
```bash
hatch run all           # Run all checks
hatch version patch     # Bump version
hatch build             # Build
hatch publish -r test   # Test PyPI
hatch publish           # Production
```

**Or use Makefile for even simpler commands:**
```bash
make release-patch      # Does all of the above!
```

---

### 🎓 "I'm new to Python packaging"

**Start with Makefile:**
```bash
make help       # See all commands
make test       # Try running tests
make build      # Try building
```

Then explore the underlying Hatch commands when comfortable.

---

### ⚙️ "I want maximum control"

**Use traditional tools:**
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pytest
python -m build
twine upload dist/*
```

---

### 🏢 "I need enterprise features (lock files, etc.)"

**Consider Poetry or PDM** - but note:
- Requires project migration
- More complex setup
- This project doesn't currently need lock files

---

## 📖 Getting Started

### For New Contributors

```bash
# 1. Install hatch (one-time)
pipx install hatch

# 2. Run tests to verify setup
hatch run test

# 3. Start developing!
hatch shell  # Enter dev environment
```

### For Maintainers/Releases

```bash
# Option 1: Using Makefile (recommended)
make release-patch      # Full workflow for patch release
make publish            # After testing, publish to production

# Option 2: Using Hatch directly
hatch run all           # All checks
hatch version minor     # Bump version
hatch build             # Build
hatch publish -r test   # Test first
hatch publish           # Production
```

### For CI/CD

See examples in `BUILD.md` for:
- GitHub Actions with Hatch
- GitHub Actions with traditional tools
- GitLab CI
- Jenkins

---

## 🔗 Documentation Links

- **Quick Start:** [QUICKSTART.md](../../QUICKSTART.md)
- **Detailed Build Guide:** [BUILD.md](../../BUILD.md)
- **Hatch Official Docs:** https://hatch.pypa.io/
- **Python Packaging Guide:** https://packaging.python.org/

---

## 🆘 Troubleshooting

### "Hatch command not found"

```bash
# Install with pipx (recommended)
pipx install hatch

# Or with pip
pip install --user hatch

# Or with homebrew (macOS)
brew install hatch
```

### "Make command not found" (Windows)

Either:
1. Install Make for Windows: https://gnuwin32.sourceforge.net/packages/make.htm
2. Use Hatch commands directly instead
3. Use WSL

### "Python version not found"

```bash
# Install required Python versions with pyenv
pyenv install 3.9 3.10 3.11 3.12

# Or use your system package manager
brew install python@3.11  # macOS
```

---

## 🎉 Conclusion

**For this project, we recommend:**

1. **Primary:** Use **Hatch** (already fully configured)
2. **Convenience:** Use **Makefile** (wraps Hatch with shorter commands)
3. **Learning:** Start with Makefile, graduate to Hatch
4. **Advanced:** Consider Poetry/PDM only if you need lock files

**Bottom line:** You're already set up with the best tools! Just install Hatch and you're ready to go.

```bash
# Get started in 3 commands:
pipx install hatch
hatch run test
hatch build
```

Happy coding! 🚀
