# Test Session Interchangeability

## Overview

The test suite now supports interchangeable Spark sessions that automatically adapt based on the `SPARK_MODE` environment variable. This allows tests to work seamlessly with both mock-spark and real PySpark without code changes.

## How It Works

### Primary Fixture: `spark_session`

The `spark_session` fixture is the primary way to get a Spark session in tests. It automatically provides:

- **SPARK_MODE=mock** (default): Returns a mock-spark session
- **SPARK_MODE=real**: Returns a real PySpark session with Delta Lake support

```python
def test_my_feature(spark_session):
    # Works with both mock-spark and PySpark automatically
    df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    assert df.count() == 2
```

### Interchangeable Fixtures

The following fixtures are now interchangeable and all use the same underlying `spark_session`:

1. **`spark_session`** - Primary fixture (recommended)
2. **`mock_spark_session`** - Alias for `spark_session` (for clarity)
3. **`mock_spark`** (in `test_execution_engine.py`) - Alias for `spark_session`

All of these fixtures automatically provide the correct session type based on `SPARK_MODE`.

## Usage Examples

### Basic Usage

```python
def test_simple_operation(spark_session):
    # Automatically uses mock-spark or PySpark based on SPARK_MODE
    df = spark_session.createDataFrame([(1, "test")], ["id", "value"])
    result = df.filter(df.id > 0).collect()
    assert len(result) == 1
```

### Using Different Fixture Names

```python
# All of these work the same way:
def test_with_spark_session(spark_session):
    df = spark_session.createDataFrame([(1,)], ["id"])

def test_with_mock_spark_session(mock_spark_session):
    df = mock_spark_session.createDataFrame([(1,)], ["id"])

def test_with_mock_spark(mock_spark):
    df = mock_spark.createDataFrame([(1,)], ["id"])
```

## Running Tests

### With Mock-Spark (Default)

```bash
# Default - uses mock-spark
pytest tests/

# Explicitly set mock mode
SPARK_MODE=mock pytest tests/
```

### With Real PySpark

```bash
# Use real PySpark
SPARK_MODE=real pytest tests/
```

## Benefits

1. **No Code Changes**: Tests work in both modes without modification
2. **Automatic Adaptation**: Session type is determined by environment variable
3. **Consistent API**: Same fixture names work in both modes
4. **Easy Testing**: Switch between modes by changing one environment variable

## Migration Guide

### Before (Manual Mode Checking)

```python
def test_my_feature(mock_spark, spark_session):
    # Had to manually check mode
    spark = (
        spark_session
        if os.environ.get("SPARK_MODE", "mock").lower() == "real"
        else mock_spark
    )
    df = spark.createDataFrame([(1,)], ["id"])
```

### After (Automatic Interchangeability)

```python
def test_my_feature(spark_session):
    # Automatically uses correct session
    df = spark_session.createDataFrame([(1,)], ["id"])
```

## Advanced Usage

### Tests That Need Specific Mocking

For tests that need to mock specific Spark behavior (like `read.format().load()`), you can still use `Mock` objects:

```python
from unittest.mock import Mock

def test_with_specific_mocking(spark_session):
    # Override specific behavior if needed
    mock_df = Mock()
    spark_session.read.format.return_value.load.return_value = mock_df
    # ... rest of test
```

However, in most cases, the interchangeable fixtures work without additional mocking.

## Fixture Details

### `spark_session` Fixture

- **Scope**: `function` (new session for each test)
- **Mode**: Automatically adapts to `SPARK_MODE`
- **Cleanup**: Automatically handled (tables, schemas, session stop)

### `mock_spark_session` Fixture

- **Scope**: `function`
- **Mode**: Alias for `spark_session` (same behavior)
- **Use Case**: When you want to make it clear you're using an interchangeable session

### `mock_spark` Fixture (in specific test files)

- **Scope**: `function`
- **Mode**: Returns a Mock object (not interchangeable)
- **Use Case**: Tests that need specific Mock behavior (e.g., mocking `read.format().load()`)
- **Note**: For interchangeable behavior, use `spark_session` instead

## Notes

- All fixtures share the same cleanup logic
- Sessions are isolated per test (function scope)
- No manual cleanup needed - fixtures handle everything
- Works with both mock-spark 3.12.0+ and PySpark

