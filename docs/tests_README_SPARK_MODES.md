# Spark Mode Switching Guide

This test suite supports two modes — **sparkless** and **pyspark** — allowing you to switch between them on the fly.

## 🚀 Quick Start

### Default (sparkless)
```bash
# Run tests with sparkless (default)
python -m pytest unit/test_validation_standalone.py

# Or use the convenience script
./test_mock.sh unit/test_validation_standalone.py
```

### PySpark
```bash
# Run tests with PySpark
SPARKLESS_TEST_MODE=pyspark python -m pytest unit/test_validation_standalone.py

# Or use the convenience script
./test_real.sh unit/test_validation_standalone.py
```

## 📋 All Available Methods

### 1. Environment Variable (Recommended)
```bash
# sparkless (default)
export SPARKLESS_TEST_MODE=sparkless
python -m pytest

# PySpark
export SPARKLESS_TEST_MODE=pyspark
python -m pytest
```

### 2. Inline Environment Variable
```bash
# sparkless
SPARKLESS_TEST_MODE=sparkless python -m pytest

# PySpark
SPARKLESS_TEST_MODE=pyspark python -m pytest
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
# sparkless (default)
python run_tests.py unit/test_validation_standalone.py

# PySpark
python run_tests.py --mode pyspark unit/test_validation_standalone.py
```

## 🔧 Configuration

The `conftest.py` file automatically detects the `SPARKLESS_TEST_MODE` environment variable:

- `SPARKLESS_TEST_MODE=sparkless` (default): Uses sparkless for fast, lightweight testing
- `SPARKLESS_TEST_MODE=pyspark`: Uses PySpark with Delta Lake support

## 📊 Test Markers

You can mark tests to run only in specific modes:

```python
import pytest

@pytest.mark.sparkless_only
def test_sparkless_specific():
    """This test only runs with sparkless"""
    pass

@pytest.mark.pyspark_only
def test_pyspark_specific():
    """This test only runs with PySpark"""
    pass
```

## 🎯 Use Cases

### sparkless (Default)
- **Fast execution** - No JVM startup time
- **No dependencies** - No need for Java or Spark installation
- **Isolated testing** - Perfect for unit tests
- **CI/CD friendly** - Works in any environment

### PySpark
- **Full compatibility** - Tests actual Spark behavior
- **Integration testing** - Tests real Spark features
- **Performance testing** - Real performance characteristics
- **Production validation** - Ensures compatibility with real Spark

## 🚨 Requirements

### sparkless
- No additional requirements
- Works out of the box

### PySpark
- Java 8 or 11
- PySpark installed
- Delta Lake (optional, for full functionality)
- Sufficient memory (1GB+ recommended)

## 🔍 Troubleshooting

### PySpark Issues
If you encounter issues with PySpark:

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
   SPARKFORGE_BASIC_SPARK=1 SPARKLESS_TEST_MODE=pyspark python -m pytest
   ```

4. **Skip Delta Lake entirely**:
   ```bash
   SPARKFORGE_SKIP_DELTA=1 SPARKLESS_TEST_MODE=pyspark python -m pytest
   ```

### sparkless Issues
If you encounter issues with sparkless:

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
SPARKLESS_TEST_MODE=sparkless python -m pytest unit/test_validation_standalone.py
SPARKLESS_TEST_MODE=pyspark python -m pytest unit/test_validation_standalone.py

# Run all tests in both modes
./test_mock.sh
./test_real.sh

# Run specific test file in both modes
python run_tests.py --mode sparkless unit/test_validation_standalone.py
python run_tests.py --mode pyspark unit/test_validation_standalone.py
```
