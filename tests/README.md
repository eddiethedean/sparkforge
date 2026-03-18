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
# Run all tests (default: sparkless mode)
python tests/run_tests.py

# Run in PySpark mode
python tests/run_tests.py --mode pyspark

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
- **sparkless** (default) - Uses sparkless for fast, lightweight testing
- **pyspark** - Uses actual PySpark and Delta Lake

Set mode via `--mode` flag (recommended) or `SPARKLESS_TEST_MODE` environment variable.

### Which tests run in sparkless vs pyspark mode

All test modules are set up to run in **both** sparkless and pyspark mode:

- **Dual-mode (run in both)**  
  The majority of tests use the shared `spark_session` (or `mock_spark_session`) fixture and conditional imports for `F` and types so they run in either mode. This includes:
  - `integration/test_validation_integration.py`, `unit/test_execution_write_mode.py`
  - `unit/test_validation_mock.py`, `unit/test_validation_enhanced_simple.py`
  - `unit/test_trap_5_default_schema_fallbacks.py`, `unit/sql_source/test_sql_source_builder.py`
  - All `builder_tests/` and `builder_pyspark_tests/` (session comes from root or mode-specific conftest)
  - `compat_pyspark/test_pyspark_compatibility.py` (engine-switching and detection tests run in both; some tests that create a real PySpark session are skipped in sparkless mode)

- **Individual tests that skip in one mode**  
  A few tests still skip in one mode due to external requirements:
  - **pyspark-only (skip in sparkless):** Tests that require a real JVM/Delta stack (or external services like PostgreSQL) skip when not in pyspark mode.
  - **sparkless-only (skip in pyspark):** Rare; e.g. a test that exercises sparkless-only fallback behavior.

The root `conftest.py` applies markers so sparkless mode skips `pyspark_only` and pyspark mode skips `sparkless_only`.

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
