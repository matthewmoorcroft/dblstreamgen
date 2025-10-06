#!/usr/bin/env bash
set -euo pipefail

# Build wheel for dblstreamgen using uv
# This script creates a distributable wheel file that can be installed
# in Databricks or other Python environments.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "🏗️  Building dblstreamgen wheel..."
echo ""

# Clean previous builds
if [ -d "dist" ]; then
    echo "🧹 Cleaning previous builds..."
    rm -rf dist/
fi

if [ -d "build" ]; then
    rm -rf build/
fi

# Ensure uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ uv is not installed. Install it with:"
    echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Build the wheel
echo "📦 Building wheel with uv..."
uv build

echo ""
echo "✅ Build complete!"
echo ""
echo "Wheel location:"
ls -lh dist/*.whl

echo ""
echo "📝 To install:"
echo "   pip install dist/dblstreamgen-*.whl"
echo ""
echo "📝 To install in Databricks notebook:"
echo "   %pip install /dbfs/path/to/dblstreamgen-*.whl[all]"
echo ""


