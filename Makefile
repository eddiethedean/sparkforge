# SparkForge Quality Management Makefile
# Provides easy commands for code quality, testing, and development

.PHONY: help install install-dev quality format lint type-check test test-cov clean pre-commit install-hooks fix-all

# Default target
help: ## Show this help message
	@echo "SparkForge Quality Management"
	@echo "============================"
	@echo ""
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Installation
install: ## Install SparkForge in production mode
	pip install -e .

install-dev: ## Install SparkForge in development mode with all dev dependencies
	pip install -e ".[dev]"

# Code Quality
quality: ## Run all quality checks
	python scripts/quality_check.py

format: ## Format code with Black and isort
	@echo "🎨 Formatting code..."
	python -m black sparkforge/ tests/
	python -m isort sparkforge/ tests/
	@echo "✅ Code formatted successfully"

lint: ## Run linting checks (ruff, pylint)
	@echo "🔍 Running linting checks..."
	python -m ruff check sparkforge/ tests/
	python -m pylint sparkforge/
	@echo "✅ Linting completed"

type-check: ## Run type checking with mypy
	@echo "🔍 Running type checks..."
	python -m mypy sparkforge/
	@echo "✅ Type checking completed"

security: ## Run security checks with bandit
	@echo "🔒 Running security checks..."
	python -m bandit -r sparkforge/ -f json -o bandit-report.json
	@echo "✅ Security checks completed"

# Testing
test: ## Run tests
	@echo "🧪 Running tests..."
	python -m pytest tests/ -v
	@echo "✅ Tests completed"

test-cov: ## Run tests with coverage
	@echo "🧪 Running tests with coverage..."
	python -m pytest tests/ --cov=sparkforge --cov-report=html --cov-report=term-missing
	@echo "✅ Tests with coverage completed"

test-fast: ## Run fast tests only
	@echo "⚡ Running fast tests..."
	python -m pytest tests/ -v -m "not slow"
	@echo "✅ Fast tests completed"

# Pre-commit
install-hooks: ## Install pre-commit hooks
	@echo "🔧 Installing pre-commit hooks..."
	pre-commit install
	@echo "✅ Pre-commit hooks installed"

pre-commit: ## Run pre-commit on all files
	@echo "🔧 Running pre-commit on all files..."
	pre-commit run --all-files
	@echo "✅ Pre-commit completed"

# Fixes
fix-all: format lint type-check ## Fix all fixable issues
	@echo "🔧 Fixing all fixable issues..."
	python -m black sparkforge/ tests/
	python -m isort sparkforge/ tests/
	python -m ruff check --fix sparkforge/ tests/
	@echo "✅ All fixes applied"

# Cleanup
clean: ## Clean up build artifacts and cache files
	@echo "🧹 Cleaning up..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf bandit-report.json
	rm -rf quality_report.json
	@echo "✅ Cleanup completed"

# Development workflow
dev-setup: install-dev install-hooks ## Set up development environment
	@echo "🚀 Development environment ready!"
	@echo "Run 'make quality' to check code quality"
	@echo "Run 'make test' to run tests"
	@echo "Run 'make fix-all' to fix common issues"

# CI/CD helpers
ci-test: install-dev test-cov lint type-check security ## Run all CI checks
	@echo "✅ All CI checks passed"

# Release helpers
check-release: quality test ## Check if code is ready for release
	@echo "🎯 Code is ready for release!"

# Documentation
docs: ## Build documentation
	@echo "📚 Building documentation..."
	cd docs && make html
	@echo "✅ Documentation built"

# Package management
build: clean ## Build package
	@echo "📦 Building package..."
	python -m build
	@echo "✅ Package built"

publish: check-release build ## Publish package to PyPI
	@echo "🚀 Publishing package..."
	python -m twine upload dist/*
	@echo "✅ Package published"

# Quick development commands
quick-check: format lint test-fast ## Quick quality check for development
	@echo "⚡ Quick check completed"

# Show current status
status: ## Show current development status
	@echo "📊 SparkForge Development Status"
	@echo "================================"
	@echo "Python version: $(shell python --version)"
	@echo "Pip version: $(shell pip --version)"
	@echo "Git status: $(shell git status --porcelain | wc -l) files changed"
	@echo "Test files: $(shell find tests -name '*.py' | wc -l)"
	@echo "Source files: $(shell find sparkforge -name '*.py' | wc -l)"
