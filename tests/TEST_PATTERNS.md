# Test Pattern Guidelines

This document outlines the standard patterns and conventions for writing tests in the SparkForge test suite.

## Test Organization

### Directory Structure

Tests are organized by layer:
- `tests/unit/` - Unit tests for individual components
- `tests/integration/` - Integration tests for component interactions
- `tests/system/` - System tests for end-to-end scenarios
- `tests/performance/` - Performance and benchmark tests
- `tests/security/` - Security-focused tests

Within each layer, tests are organized by feature/module:
- `tests/unit/execution/` - Execution engine tests
- `tests/unit/validation/` - Validation service tests
- `tests/unit/storage/` - Storage service tests
- etc.

## Naming Conventions

### Test Files

- Use `test_<module_name>.py` format
- Example: `test_step_executors.py`, `test_execution_validator.py`

### Test Classes

- Use `Test<Feature><Aspect>` format
- Example: `TestBronzeStepExecutor`, `TestExecutionValidator`, `TestTableService`

### Test Methods

- Use `test_<scenario>_<expected_result>` format
- Example: `test_execute_with_valid_data`, `test_validate_step_output_with_rules`

## Test Structure

### Standard Test Pattern

```python
class TestFeature:
    """Tests for Feature component."""
    
    def test_scenario_expected_result(self, spark_session):
        """
        Test description using Given-When-Then format.
        
        Given: Initial state
        When: Action is performed
        Then: Expected result
        """
        # Arrange (Given)
        service = Service(spark_session)
        data = create_test_data(spark_session)
        
        # Act (When)
        result = service.method(data)
        
        # Assert (Then)
        assert result is not None
        assert result.count() > 0
```

## Pytest Markers

### Layer Markers

- `@pytest.mark.unit` - Unit tests
- `@pytest.mark.integration` - Integration tests
- `@pytest.mark.system` - System tests
- `@pytest.mark.performance` - Performance tests
- `@pytest.mark.security` - Security tests

### Feature Markers

- `@pytest.mark.step_executor` - Step executor tests
- `@pytest.mark.validator` - Validation tests
- `@pytest.mark.storage` - Storage service tests
- `@pytest.mark.transformation` - Transformation tests
- `@pytest.mark.reporting` - Reporting tests
- `@pytest.mark.error_handling` - Error handling tests

### Test Category Markers

- `@pytest.mark.happy_path` - Happy path tests
- `@pytest.mark.edge_case` - Edge case tests
- `@pytest.mark.error_handling` - Error handling tests
- `@pytest.mark.slow` - Slow-running tests
- `@pytest.mark.fast` - Fast-running tests

### Mode Markers

- `@pytest.mark.mock_only` - Tests that only work with mock Spark
- `@pytest.mark.real_spark_only` - Tests that only work with real Spark

## Fixture Usage

### Common Fixtures

- `spark_session` - Spark session (function-scoped, mode-aware)
- `isolated_spark_session` - Isolated Spark session for tests needing complete isolation
- `test_config` - PipelineConfig for testing
- `sample_dataframe` - Sample DataFrame with test data

### Custom Fixtures

Create custom fixtures in `conftest.py` files at the appropriate level:
- Root `conftest.py` - Shared across all tests
- Layer `conftest.py` - Shared within layer (unit/integration/system)
- Module `conftest.py` - Shared within test module

## Test Data Generation

Use `TestDataGenerator` from `tests.test_helpers.data_generators`:

```python
from tests.test_helpers.data_generators import TestDataGenerator

def test_with_generated_data(spark_session):
    data = TestDataGenerator.create_events_data(spark_session, num_records=100)
    # Use data in test
```

## Assertions

Use `TestAssertions` from `tests.test_helpers.assertions`:

```python
from tests.test_helpers.assertions import TestAssertions

def test_with_assertions(spark_session):
    df = create_dataframe(spark_session)
    TestAssertions.assert_dataframe_not_empty(df)
    TestAssertions.assert_dataframe_has_columns(df, ["id", "name"])
```

## Mock Usage

Use mock factories from `tests.test_helpers.mocks`:

```python
from tests.test_helpers.mocks import create_mock_spark_session, create_mock_dataframe

def test_with_mocks():
    spark = create_mock_spark_session()
    df = create_mock_dataframe()
    # Use mocks in test
```

## Test Isolation

Use isolation utilities from `tests.test_helpers.isolation`:

```python
from tests.test_helpers.isolation import get_unique_schema

def test_with_isolation(spark_session):
    schema = get_unique_schema("test")
    # Use unique schema for isolation
```

## Spark Mode Handling

Use patterns from `tests.test_helpers.patterns`:

```python
from tests.test_helpers.patterns import skip_if_mock, isolated_spark_test

@skip_if_mock("This test requires real Spark")
def test_real_spark_feature(spark_session):
    # Test code
```

## Best Practices

1. **One assertion per test concept** - Each test should verify one behavior
2. **Use descriptive names** - Test names should clearly describe what they test
3. **Arrange-Act-Assert** - Structure tests with clear sections
4. **Use fixtures** - Don't duplicate setup code, use fixtures
5. **Test edge cases** - Include tests for error conditions and edge cases
6. **Keep tests fast** - Unit tests should run quickly
7. **Isolate tests** - Tests should not depend on each other
8. **Use appropriate scope** - Choose fixture scope based on cost and isolation needs

## Documentation

- Add docstrings to test classes describing what they test
- Add docstrings to test methods using Given-When-Then format
- Document complex test scenarios
- Include examples in docstrings when helpful
