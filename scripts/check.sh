#!/usr/bin/env bash
set -euo pipefail

# Run all code quality checks for dblstreamgen

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "🔍 Running code quality checks..."
echo ""

# Check if tools are installed
if ! command -v ruff &> /dev/null; then
    echo "❌ ruff is not installed. Install with: uv pip install ruff"
    exit 1
fi

if ! command -v mypy &> /dev/null; then
    echo "❌ mypy is not installed. Install with: uv pip install mypy"
    exit 1
fi

# Ruff format check
echo "📐 Checking code formatting with ruff..."
if ruff format --check .; then
    echo "✅ Code formatting is correct"
else
    echo "❌ Code formatting issues found. Run: ruff format ."
    exit 1
fi
echo ""

# Ruff lint
echo "🔎 Linting with ruff..."
if ruff check .; then
    echo "✅ No linting issues found"
else
    echo "❌ Linting issues found. Run: ruff check --fix ."
    exit 1
fi
echo ""

# MyPy type checking
echo "🔬 Type checking with mypy..."
if mypy src/; then
    echo "✅ Type checking passed"
else
    echo "❌ Type checking failed"
    exit 1
fi
echo ""

echo "✅ All checks passed!"


