# Spark Mode Switching Guide

This test suite supports both **mock Spark** and **real Spark** environments, allowing you to switch between them on the fly.

## 🚀 Quick Start

### Default (Mock Spark)
```bash
# Run tests with mock Spark (default)
python -m pytest unit/test_validation_standalone.py

# Or use the convenience script
./test_mock.sh unit/test_validation_standalone.py
```

### Real Spark
```bash
# Run tests with real Spark
SPARK_MODE=real python -m pytest unit/test_validation_standalone.py

# Or use the convenience script
./test_real.sh unit/test_validation_standalone.py
```

## 📋 All Available Methods

### 1. Environment Variable (Recommended)
```bash
# Mock Spark (default)
export SPARK_MODE=mock
python -m pytest

# Real Spark
export SPARK_MODE=real
python -m pytest
```

### 2. Inline Environment Variable
```bash
# Mock Spark
SPARK_MODE=mock python -m pytest

# Real Spark
SPARK_MODE=real python -m pytest
```

### 3. Convenience Scripts
```bash
# Mock Spark
./test_mock.sh unit/test_validation_standalone.py

# Real Spark
./test_real.sh unit/test_validation_standalone.py
```

### 4. Python Test Runner
```bash
# Mock Spark (default)
python run_tests.py unit/test_validation_standalone.py

# Real Spark
python run_tests.py --real unit/test_validation_standalone.py

# Mock Spark (explicit)
python run_tests.py --mock unit/test_validation_standalone.py
```

## 🔧 Configuration

The `conftest.py` file automatically detects the `SPARK_MODE` environment variable:

- `SPARK_MODE=mock` (default): Uses mock_spark for fast, lightweight testing
- `SPARK_MODE=real`: Uses real Spark with Delta Lake support

## 📊 Test Markers

You can mark tests to run only in specific modes:

```python
import pytest

@pytest.mark.mock_only
def test_mock_specific():
    """This test only runs with mock Spark"""
    pass

@pytest.mark.real_spark_only
def test_real_spark_specific():
    """This test only runs with real Spark"""
    pass
```

## 🎯 Use Cases

### Mock Spark (Default)
- **Fast execution** - No JVM startup time
- **No dependencies** - No need for Java or Spark installation
- **Isolated testing** - Perfect for unit tests
- **CI/CD friendly** - Works in any environment

### Real Spark
- **Full compatibility** - Tests actual Spark behavior
- **Integration testing** - Tests real Spark features
- **Performance testing** - Real performance characteristics
- **Production validation** - Ensures compatibility with real Spark

## 🚨 Requirements

### Mock Spark
- No additional requirements
- Works out of the box

### Real Spark
- Java 8 or 11
- PySpark installed
- Delta Lake (optional, for full functionality)
- Sufficient memory (1GB+ recommended)

## 🔍 Troubleshooting

### Real Spark Issues
If you encounter issues with real Spark:

1. **Delta Lake not found**:
   ```bash
   pip install delta-spark
   ```

2. **Java not found**:
   ```bash
   # Install Java 8 or 11
   brew install openjdk@11  # macOS
   sudo apt-get install openjdk-11-jdk  # Ubuntu
   ```

3. **Memory issues**:
   ```bash
   # Use basic Spark without Delta Lake
   SPARKFORGE_BASIC_SPARK=1 SPARK_MODE=real python -m pytest
   ```

4. **Skip Delta Lake entirely**:
   ```bash
   SPARKFORGE_SKIP_DELTA=1 SPARK_MODE=real python -m pytest
   ```

### Mock Spark Issues
If you encounter issues with mock Spark:

1. **Import errors**: Ensure mock_spark is in your Python path
2. **Test failures**: Check that tests are designed to work with mock objects

## 📈 Performance Comparison

| Mode | Startup Time | Memory Usage | Test Speed | Compatibility |
|------|-------------|--------------|------------|---------------|
| Mock | ~0.1s | ~10MB | Very Fast | High |
| Real | ~2-5s | ~500MB+ | Fast | Complete |

## 🎉 Best Practices

1. **Default to mock** for development and CI/CD
2. **Use real Spark** for integration tests and before releases
3. **Mark tests appropriately** with `@pytest.mark.mock_only` or `@pytest.mark.real_spark_only`
4. **Test both modes** before major releases
5. **Use environment variables** in your CI/CD pipeline

## 🔄 Switching Examples

```bash
# Quick switch during development
SPARK_MODE=mock python -m pytest unit/test_validation_standalone.py
SPARK_MODE=real python -m pytest unit/test_validation_standalone.py

# Run all tests in both modes
./test_mock.sh
./test_real.sh

# Run specific test file in both modes
python run_tests.py --mock unit/test_validation_standalone.py
python run_tests.py --real unit/test_validation_standalone.py
```
