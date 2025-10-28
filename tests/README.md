# SparkForge Test Suite Documentation

## Overview

This document provides comprehensive documentation for the SparkForge test suite, including structure, organization, guidelines, and best practices for writing and maintaining tests.

## Test Structure

The test suite is organized into three main categories:

### 1. Unit Tests (`tests/unit/`)
- **Purpose**: Test individual components in isolation
- **Coverage**: 71% overall coverage
- **Execution Time**: ~13 seconds
- **Files**: 19 test files covering core functionality

#### Key Test Categories:
- **Dependencies**: `test_analyzer.py`, `test_graph.py`, `test_exceptions.py`
- **Models**: `test_models.py`, `test_models_new.py`, `test_models_simple.py`
- **Core Modules**: `test_constants.py`, `test_errors.py`, `test_logging.py`
- **Operations**: `test_table_operations.py`, `test_validation.py`, `test_reporting.py`
- **Performance**: `test_performance.py`

### 2. Integration Tests (`tests/integration/`)
- **Purpose**: Test component interactions and data flow
- **Coverage**: Full pipeline execution scenarios
- **Execution Time**: ~75 seconds
- **Files**: 11 test files

#### Key Test Categories:
- **Execution Engine**: `test_execution_engine.py`, `test_execution_engine_new.py`
- **Pipeline Builder**: `test_pipeline_builder.py`
- **Pipeline Runner**: `test_pipeline_runner.py`
- **Pipeline Monitor**: `test_pipeline_monitor.py`
- **Step Execution**: `test_step_execution.py`
- **Validation Integration**: `test_validation_integration.py`

### 3. System Tests (`tests/system/`)
- **Purpose**: Test complete system functionality with real Spark
- **Coverage**: End-to-end scenarios with Delta Lake
- **Execution Time**: ~34 seconds
- **Files**: 11 test files

#### Key Test Categories:
- **Delta Lake**: `test_delta_lake.py`
- **Real Spark**: `test_simple_real_spark.py`
- **DataFrame Access**: `test_dataframe_access.py`
- **Multi-Schema**: `test_multi_schema_support.py`
- **User Experience**: `test_improved_user_experience.py`
- **System Utils**: `test_utils.py`, `test_helpers.py`

## Test Configuration

### Environment Setup

#### Prerequisites
- **Python 3.8**: Required for SparkForge compatibility
- **Java 11**: Required for Apache Spark
- **Apache Spark**: For running integration and system tests
- **Delta Lake**: For system tests with ACID transactions

#### Environment Variables
```bash
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28
export PATH=$JAVA_HOME/bin:$PATH
```

#### Virtual Environment
```bash
# Create and activate virtual environment
python3.8 -m venv venv38
source venv38/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install pytest pytest-cov pytest-xdist mypy black isort ruff bandit
```

### Configuration Files

#### `pytest.ini`
```ini
[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts =
    --strict-markers
    --strict-config
    --verbose
    --tb=short
markers =
    unit: Unit tests
    integration: Integration tests
    system: System tests
    delta: Delta Lake tests
    spark: Spark tests
    slow: Slow tests
```

#### `mypy.ini`
```ini
[mypy]
python_version = 3.8
warn_return_any = True
warn_unused_configs = True
disallow_untyped_defs = True
disallow_incomplete_defs = True
check_untyped_defs = True
disallow_untyped_decorators = True
no_implicit_optional = True
warn_redundant_casts = True
warn_unused_ignores = True
warn_no_return = True
warn_unreachable = True
strict_equality = True

[mypy-tests.*]
disallow_untyped_defs = False
```

## Test Execution

### Running All Tests
```bash
# Run complete test suite
python tests/run_all_tests.py

# Or using pytest directly
python -m pytest tests/
```

### Running by Category
```bash
# Unit tests only
python tests/run_unit_tests.py
python -m pytest tests/unit/

# Integration tests only
python tests/run_integration_tests.py
python -m pytest tests/integration/

# System tests only
python tests/run_system_tests.py
python -m pytest tests/system/
```

### Running with Coverage
```bash
# Unit tests with coverage
python -m pytest tests/unit/ --cov=pipeline_builder --cov-report=term-missing --cov-report=html

# All tests with coverage
python -m pytest tests/ --cov=pipeline_builder --cov-report=term-missing --cov-report=html
```

### Running Specific Tests
```bash
# Run specific test file
python -m pytest tests/unit/test_models.py

# Run specific test class
python -m pytest tests/unit/test_models.py::TestBronzeStep

# Run specific test method
python -m pytest tests/unit/test_models.py::TestBronzeStep::test_bronze_step_creation
```

## Test Writing Guidelines

### 1. Test Structure

#### Basic Test Structure
```python
import pytest
from unittest.mock import Mock, patch
from pipeline_builder.models import BronzeStep, SilverStep, GoldStep

class TestExample:
    """Test class for example functionality."""

    def test_basic_functionality(self):
        """Test basic functionality works correctly."""
        # Arrange
        expected = "expected_value"

        # Act
        result = function_under_test()

        # Assert
        assert result == expected

    def test_edge_case(self):
        """Test edge case handling."""
        with pytest.raises(ValueError):
            function_under_test(invalid_input)
```

### 2. Naming Conventions

- **Test files**: `test_*.py`
- **Test classes**: `Test*` (e.g., `TestBronzeStep`)
- **Test methods**: `test_*` (e.g., `test_bronze_step_creation`)
- **Descriptive names**: Use clear, descriptive names that explain what is being tested

### 3. Test Categories and Markers

#### Unit Tests
```python
import pytest

@pytest.mark.unit
class TestUnitFunctionality:
    def test_isolated_function(self):
        """Test individual function in isolation."""
        pass
```

#### Integration Tests
```python
@pytest.mark.integration
class TestIntegration:
    def test_component_interaction(self):
        """Test how components work together."""
        pass
```

#### System Tests
```python
@pytest.mark.system
class TestSystem:
    def test_end_to_end_scenario(self):
        """Test complete system functionality."""
        pass
```

#### Delta Lake Tests
```python
@pytest.mark.delta
class TestDeltaLake:
    def test_acid_transactions(self):
        """Test Delta Lake ACID transactions."""
        pass
```

#### Spark Tests
```python
@pytest.mark.spark
class TestSparkOperations:
    def test_spark_dataframe_operations(self):
        """Test Spark DataFrame operations."""
        pass
```

### 4. Fixtures

#### Shared Fixtures
```python
@pytest.fixture
def sample_spark_session():
    """Provide a Spark session for tests."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_dataframe(sample_spark_session):
    """Provide a sample DataFrame for tests."""
    return sample_spark_session.createDataFrame([(1, "test")], ["id", "name"])
```

#### Fixture Scopes
- **function**: Default scope, created for each test
- **class**: Created once per test class
- **module**: Created once per test module
- **session**: Created once per test session

### 5. Mocking and Patching

#### Mocking External Dependencies
```python
from unittest.mock import Mock, patch

def test_with_mock():
    """Test using mocks for external dependencies."""
    with patch('pipeline_builder.external_module.function') as mock_func:
        mock_func.return_value = "mocked_value"

        result = function_that_uses_external()

        assert result == "expected_result"
        mock_func.assert_called_once()
```

#### Mocking Spark Operations
```python
def test_spark_operations():
    """Test Spark operations using mocks."""
    mock_spark = Mock()
    mock_dataframe = Mock()
    mock_spark.createDataFrame.return_value = mock_dataframe

    result = create_dataframe(mock_spark)

    mock_spark.createDataFrame.assert_called_once()
    assert result == mock_dataframe
```

### 6. Assertions

#### Basic Assertions
```python
def test_assertions():
    """Examples of various assertion types."""
    # Equality
    assert result == expected

    # Inequality
    assert result != unexpected

    # Truthiness
    assert result is True
    assert result is not None

    # Containment
    assert item in collection
    assert item not in collection

    # Exception raising
    with pytest.raises(ValueError):
        function_that_raises()

    # Exception with message
    with pytest.raises(ValueError, match="expected message"):
        function_that_raises()
```

#### Custom Assertions
```python
def assert_dataframe_equals(df1, df2):
    """Custom assertion for DataFrame equality."""
    assert df1.collect() == df2.collect()
    assert df1.schema == df2.schema
```

### 7. Test Data

#### Test Data Generation
```python
def generate_test_data():
    """Generate test data for tests."""
    return [
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35)
    ]

@pytest.fixture
def sample_data():
    """Provide sample data for tests."""
    return generate_test_data()
```

#### Parameterized Tests
```python
@pytest.mark.parametrize("input,expected", [
    (1, "one"),
    (2, "two"),
    (3, "three"),
])
def test_number_to_word(input, expected):
    """Test number to word conversion with multiple inputs."""
    assert number_to_word(input) == expected
```

## Coverage Guidelines

### Current Coverage Status
- **Overall Coverage**: 71%
- **Unit Tests**: Comprehensive coverage of core modules
- **Integration Tests**: Full pipeline execution scenarios
- **System Tests**: End-to-end functionality

### Coverage Targets
- **Unit Tests**: 100% coverage for core modules
- **Integration Tests**: 90% coverage for interaction scenarios
- **System Tests**: 80% coverage for end-to-end scenarios

### Coverage Improvement Areas
1. **pipeline_builder/execution.py**: 35% → 90%+
2. **pipeline_builder/pipeline/builder.py**: 25% → 90%+
3. **pipeline_builder/pipeline/runner.py**: 29% → 90%+
4. **pipeline_builder/pipeline/monitor.py**: 33% → 90%+
5. **pipeline_builder/validation.py**: 48% → 90%+

## Best Practices

### 1. Test Organization
- Group related tests in classes
- Use descriptive test names
- Follow AAA pattern (Arrange, Act, Assert)
- Keep tests focused and simple

### 2. Test Isolation
- Each test should be independent
- Use fixtures for setup and teardown
- Avoid shared state between tests
- Clean up resources properly

### 3. Test Performance
- Keep unit tests fast (< 1 second each)
- Use mocks for external dependencies
- Avoid unnecessary Spark sessions in unit tests
- Use appropriate test categories

### 4. Test Maintenance
- Update tests when code changes
- Remove obsolete tests
- Keep test data current
- Document complex test scenarios

### 5. Error Handling
- Test both success and failure cases
- Test edge cases and boundary conditions
- Verify error messages and exception types
- Test recovery scenarios

## Troubleshooting

### Common Issues

#### 1. Import Errors
```bash
# Check Python path
python -c "import sys; print(sys.path)"

# Verify virtual environment
which python
```

#### 2. Java/Spark Issues
```bash
# Check Java version
java -version

# Check JAVA_HOME
echo $JAVA_HOME

# Test Spark session creation
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('test').master('local[1]').getOrCreate(); print('Spark OK'); spark.stop()"
```

#### 3. Test Failures
```bash
# Run with verbose output
python -m pytest tests/unit/test_failing.py -v -s

# Run with full traceback
python -m pytest tests/unit/test_failing.py --tb=long

# Run specific test only
python -m pytest tests/unit/test_failing.py::TestClass::test_method
```

#### 4. Coverage Issues
```bash
# Generate HTML coverage report
python -m pytest tests/unit/ --cov=pipeline_builder --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Debug Mode
```bash
# Run with debug output
python -m pytest tests/ -v -s --log-cli-level=DEBUG

# Run with pdb on failure
python -m pytest tests/ --pdb
```

## Continuous Integration

### GitHub Actions
The test suite is designed to run in CI/CD environments with:
- Python 3.8
- Java 11
- Proper environment setup
- Coverage reporting
- Test result publishing

### Local Development
```bash
# Run tests before committing
make test

# Run with coverage
make test-coverage

# Run linting
make lint

# Run all checks
make check-all
```

## Contributing

### Adding New Tests
1. Create test file following naming conventions
2. Add appropriate test markers
3. Follow test structure guidelines
4. Add documentation for complex tests
5. Ensure adequate coverage

### Modifying Existing Tests
1. Update tests when changing functionality
2. Maintain backward compatibility where possible
3. Update documentation if needed
4. Run full test suite to ensure no regressions

### Test Review Checklist
- [ ] Tests are focused and well-named
- [ ] Appropriate test category and markers
- [ ] Good coverage of functionality
- [ ] Proper error handling tests
- [ ] No hardcoded values or magic numbers
- [ ] Proper cleanup and resource management
- [ ] Documentation is up to date

## Conclusion

This test suite provides comprehensive coverage of the SparkForge functionality with a well-organized structure that supports both development and maintenance. Following these guidelines will ensure consistent, reliable, and maintainable tests.

For questions or issues, please refer to the troubleshooting section or contact the development team.
