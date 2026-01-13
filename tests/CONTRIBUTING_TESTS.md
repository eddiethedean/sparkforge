# Contributing Tests Guide

This guide helps contributors write tests that follow SparkForge conventions.

## Quick Start

1. **Choose the right layer**
   - Unit tests for individual components
   - Integration tests for component interactions
   - System tests for end-to-end scenarios

2. **Create test file**
   - Use `test_<module_name>.py` format
   - Place in appropriate layer directory

3. **Write test class**
   - Use `Test<Feature><Aspect>` naming
   - Add docstring describing what is tested

4. **Write test methods**
   - Use `test_<scenario>_<expected_result>` naming
   - Follow Arrange-Act-Assert pattern
   - Add docstring with Given-When-Then format

5. **Use fixtures and helpers**
   - Use `spark_session` fixture for Spark sessions
   - Use `TestDataGenerator` for test data
   - Use `TestAssertions` for assertions
   - Use isolation utilities for test isolation

## Example Test

```python
"""
Tests for MyFeature component.
"""

import pytest
from tests.test_helpers.data_generators import TestDataGenerator
from tests.test_helpers.assertions import TestAssertions
from pipeline_builder.my_feature import MyFeature


class TestMyFeature:
    """Tests for MyFeature component."""
    
    def test_process_data_success(self, spark_session):
        """
        Test processing data successfully.
        
        Given: Valid input data
        When: Data is processed
        Then: Processed data is returned
        """
        # Arrange
        feature = MyFeature(spark_session)
        data = TestDataGenerator.create_events_data(spark_session, num_records=10)
        
        # Act
        result = feature.process(data)
        
        # Assert
        TestAssertions.assert_dataframe_not_empty(result)
        TestAssertions.assert_dataframe_has_columns(result, ["id", "processed"])
```

## Running Tests

```bash
# Run all tests
python tests/run_tests.py

# Run specific layer
python tests/run_tests.py --layer unit

# Run with coverage
python tests/run_tests.py --coverage

# Run in parallel
python tests/run_tests.py --parallel --workers 4

# Run specific test
python tests/run_tests.py tests/unit/test_my_feature.py
```

## Common Patterns

See `TEST_PATTERNS.md` for detailed patterns and conventions.
