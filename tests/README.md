# SparkForge Test Suite

This directory contains the comprehensive test suite for SparkForge.

## Overview

The test suite is organized into layers:
- **Unit tests** (`unit/`) - Test individual components in isolation
- **Integration tests** (`integration/`) - Test component interactions
- **System tests** (`system/`) - Test end-to-end scenarios
- **Performance tests** (`performance/`) - Performance and benchmark tests
- **Security tests** (`security/`) - Security-focused tests

## Quick Start

### Running Tests

```bash
# Run all tests (default: mock mode)
python tests/run_tests.py

# Run in real Spark mode
python tests/run_tests.py --mode real

# Run specific layer
python tests/run_tests.py --layer unit

# Run with parallelization
python tests/run_tests.py --parallel --workers 10

# Run with coverage
python tests/run_tests.py --coverage

# Run specific test file
python tests/run_tests.py tests/unit/test_validation.py
```

### Test Modes

Tests can run in two modes:
- **Mock mode** (default) - Uses sparkless for fast, lightweight testing
- **Real mode** - Uses actual PySpark and Delta Lake

Set mode via `--mode` flag or `SPARK_MODE` environment variable.

## Test Organization

### Directory Structure

```
tests/
├── conftest.py              # Root conftest with shared fixtures
├── run_tests.py             # Unified test runner
├── test_helpers/            # Test utilities and helpers
│   ├── isolation.py         # Test isolation utilities
│   ├── data_generators.py   # Test data generation
│   ├── assertions.py        # Custom assertions
│   ├── mocks.py             # Mock factories
│   ├── spark_helpers.py     # Spark session management
│   └── patterns.py          # Common test patterns
├── unit/                    # Unit tests
│   ├── conftest.py          # Unit test fixtures
│   ├── execution/           # Execution engine tests
│   ├── validation/          # Validation service tests
│   ├── storage/             # Storage service tests
│   └── ...
├── integration/             # Integration tests
│   └── conftest.py          # Integration test fixtures
├── system/                  # System tests
│   └── conftest.py          # System test fixtures
└── ...
```

## Test Helpers

The `test_helpers` package provides utilities for common test scenarios:

- **Data Generation**: `TestDataGenerator` for creating test data
- **Assertions**: `TestAssertions` for common checks
- **Mocks**: Mock factories for unit tests
- **Spark Helpers**: Spark session management
- **Isolation**: Test isolation utilities
- **Patterns**: Common test patterns and decorators

## Documentation

- [TEST_PATTERNS.md](TEST_PATTERNS.md) - Test patterns and conventions
- [CONTRIBUTING_TESTS.md](CONTRIBUTING_TESTS.md) - Guide for writing tests
- [ARCHITECTURE.md](ARCHITECTURE.md) - Test architecture documentation
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions

## Best Practices

1. Use appropriate test layer (unit/integration/system)
2. Follow naming conventions (see TEST_PATTERNS.md)
3. Use fixtures for setup/teardown
4. Keep tests isolated and independent
5. Use test helpers for common operations
6. Add docstrings with Given-When-Then format
7. Mark tests appropriately (unit, integration, system, etc.)

## Coverage

Run tests with coverage:

```bash
python tests/run_tests.py --coverage
```

Coverage reports are generated in `htmlcov/` directory.
