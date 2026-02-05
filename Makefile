# Makefile for dblstreamgen
# Convenience wrapper around hatch commands

.PHONY: help install dev test test-cov lint format type-check clean build publish

help:  ## Show this help message
	@echo "dblstreamgen - Makefile commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install hatch (one-time setup)
	@echo "Installing hatch..."
	pipx install hatch || pip install --user hatch

dev:  ## Enter development shell
	hatch shell

test:  ## Run tests
	hatch run test

test-cov:  ## Run tests with coverage
	hatch run test-cov

lint:  ## Run linter (ruff)
	hatch run lint

lint-fix:  ## Run linter and auto-fix issues
	hatch run lint-fix

format:  ## Format code with ruff
	hatch run format

format-check:  ## Check code formatting
	hatch run format-check

type-check:  ## Run type checker (mypy)
	hatch run type-check

all:  ## Run all checks (format, lint, type-check, test)
	hatch run all

clean:  ## Clean build artifacts
	hatch clean
	rm -rf dist/ build/ *.egg-info htmlcov/ .coverage .pytest_cache/ .ruff_cache/

build:  ## Build wheel
	hatch build

version-patch:  ## Bump patch version (0.1.0 -> 0.1.1)
	hatch version patch

version-minor:  ## Bump minor version (0.1.0 -> 0.2.0)
	hatch version minor

version-major:  ## Bump major version (0.1.0 -> 1.0.0)
	hatch version major

publish-test:  ## Publish to Test PyPI
	hatch publish -r test

publish:  ## Publish to PyPI
	hatch publish

release-patch:  ## Release new patch version (test all, bump, build, publish)
	@echo "Running tests..."
	hatch run all
	@echo "Bumping version..."
	hatch version patch
	@echo "Building..."
	hatch clean && hatch build
	@echo "Publishing to Test PyPI..."
	hatch publish -r test
	@echo "✅ Patch release complete! Test on Test PyPI before publishing to production."

release-minor:  ## Release new minor version
	@echo "Running tests..."
	hatch run all
	@echo "Bumping version..."
	hatch version minor
	@echo "Building..."
	hatch clean && hatch build
	@echo "Publishing to Test PyPI..."
	hatch publish -r test
	@echo "✅ Minor release complete! Test on Test PyPI before publishing to production."

# Git shortcuts
git-status:  ## Show git status
	git status

git-diff:  ## Show git diff
	git diff

git-log:  ## Show recent git log
	git log --oneline -10
