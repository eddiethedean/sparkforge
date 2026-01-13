# Test Architecture

This document describes the architecture of the SparkForge test suite.

## Overview

The test suite follows a layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────┐
│                   Test Runner                            │
│              (tests/run_tests.py)                       │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
│  Unit Tests  │  │ Integration │  │System Tests │
│              │  │    Tests    │  │             │
└───────┬──────┘  └──────┬──────┘  └──────┬──────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
│   Fixtures   │  │   Helpers   │  │   Mocks     │
│  (conftest)  │  │ (test_helpers)│  │             │
└──────────────┘  └──────────────┘  └─────────────┘
```

## Layers

### Unit Tests

Test individual components in isolation with mocked dependencies.

**Location**: `tests/unit/`
**Scope**: Function-scoped fixtures
**Dependencies**: Mocked

### Integration Tests

Test component interactions with real Spark sessions.

**Location**: `tests/integration/`
**Scope**: Function-scoped fixtures
**Dependencies**: Real Spark, mocked external systems

### System Tests

Test end-to-end scenarios with full Spark and Delta Lake.

**Location**: `tests/system/`
**Scope**: Function-scoped fixtures
**Dependencies**: Real Spark, Delta Lake

## Fixture Hierarchy

Fixtures are organized in a hierarchy:

1. **Root conftest** (`tests/conftest.py`)
   - Shared fixtures for all tests
   - Engine configuration
   - Global state management

2. **Layer conftest** (`tests/unit/conftest.py`, etc.)
   - Layer-specific fixtures
   - Spark session configuration

3. **Module conftest** (optional)
   - Module-specific fixtures

## Test Helpers

The `test_helpers` package provides reusable utilities:

- **isolation.py** - Test isolation and cleanup
- **data_generators.py** - Test data generation
- **assertions.py** - Custom assertions
- **mocks.py** - Mock factories
- **spark_helpers.py** - Spark session management
- **patterns.py** - Common patterns and decorators

## Execution Flow

1. Test runner (`run_tests.py`) parses arguments
2. Sets up environment (mode, layer, etc.)
3. Runs pytest with appropriate configuration
4. Pytest discovers tests and applies fixtures
5. Tests execute with appropriate isolation
6. Results are reported

## Isolation Strategy

Tests are isolated through:
- Unique schema names per test
- Function-scoped Spark sessions
- Global state cleanup between tests
- Thread-local environment variables for parallel execution

## Mode Handling

Tests can run in two modes:
- **Mock mode**: Uses sparkless (fast, lightweight)
- **Real mode**: Uses PySpark and Delta Lake (slower, more realistic)

Mode is determined by:
1. `--mode` command-line argument
2. `SPARK_MODE` environment variable
3. Default: mock mode
