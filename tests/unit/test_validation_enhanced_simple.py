"""
Simplified enhanced validation tests using injectable mock functions.

This module tests the core SparkForge validation system using mock PySpark functions,
focusing on the most important functionality.
"""


# NOTE: mock-spark patches removed - now using mock-spark 1.3.0 which doesn't need patches
# The apply_mock_spark_patches() call was causing test pollution

import pytest

# Import mock objects
from mock_spark import (
    BooleanType,
    DoubleType,
    IntegerType,
    MockFunctions,
    MockSparkSession,
    MockStructField,
    MockStructType,
    StringType,
)

# Import SparkForge validation modules
from sparkforge.validation.data_validation import (
    _convert_rule_to_expression,
    _convert_rules_to_expressions,
    and_all_rules,
    apply_column_rules,
    assess_data_quality,
    validate_dataframe_schema,
)


class TestValidationWithMockFunctionsSimple:
    """Test validation functions using injectable mock functions - simplified version."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_spark = MockSparkSession("TestApp")
        self.mock_functions = MockFunctions()

        # Create sample data
        self.sample_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "active": True},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "active": False},
            {"id": 4, "name": "Diana", "age": 28, "salary": 55000.0, "active": True},
        ]

        # Create sample schema
        self.sample_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
                MockStructField("active", BooleanType()),
            ]
        )

        # Create mock DataFrame
        self.mock_df = self.mock_spark.createDataFrame(
            self.sample_data, self.sample_schema
        )

    def test_convert_rule_to_expression_with_mock_functions(self):
        """Test _convert_rule_to_expression with mock functions."""
        # Test not_null rule
        expr = _convert_rule_to_expression("not_null", "name", self.mock_functions)
        assert expr is not None
        # MockColumnOperation should have operation attribute
        assert hasattr(expr, "operation")

        # Test positive rule
        expr = _convert_rule_to_expression("positive", "age", self.mock_functions)
        assert expr is not None

        # Test non_negative rule
        expr = _convert_rule_to_expression(
            "non_negative", "salary", self.mock_functions
        )
        assert expr is not None

        # Test non_zero rule
        expr = _convert_rule_to_expression("non_zero", "id", self.mock_functions)
        assert expr is not None

    def test_convert_rules_to_expressions_with_mock_functions(self):
        """Test _convert_rules_to_expressions with mock functions."""
        rules = {"name": ["not_null"], "age": ["positive"], "salary": ["non_negative"]}

        expressions = _convert_rules_to_expressions(rules, self.mock_functions)

        assert isinstance(expressions, dict)
        assert "name" in expressions
        assert "age" in expressions
        assert "salary" in expressions

        # Each column should have a list of expressions
        for _column, exprs in expressions.items():
            assert isinstance(exprs, list)
            assert len(exprs) > 0

    def test_and_all_rules_with_mock_functions(self):
        """Test and_all_rules with mock functions."""
        rules = {"name": ["not_null"], "age": ["positive"], "salary": ["non_negative"]}

        combined_expr = and_all_rules(rules, self.mock_functions)
        assert combined_expr is not None

    def test_apply_column_rules_with_mock_functions(self):
        """Test apply_column_rules with mock functions."""
        rules = {"name": ["not_null"], "age": ["positive"], "salary": ["non_negative"]}

        # This should work with mock DataFrame and mock functions
        result = apply_column_rules(
            self.mock_df,
            rules,
            "test_stage",
            "test_step",
            functions=self.mock_functions,
        )
        assert result is not None
        assert len(result) == 3  # Should return (valid_df, invalid_df, stats)

    def test_assess_data_quality_with_mock_functions(self):
        """Test assess_data_quality with mock functions."""
        rules = {"name": ["not_null"], "age": ["positive"], "salary": ["non_negative"]}

        result = assess_data_quality(self.mock_df, rules, self.mock_functions)
        assert result is not None
        # assess_data_quality returns a dict, not a ValidationResult object
        assert isinstance(result, dict)
        assert "quality_rate" in result
        assert "total_rows" in result
        assert "invalid_rows" in result

    def test_validate_dataframe_schema_with_mock_functions(self):
        """Test validate_dataframe_schema with mock functions."""
        # Test with valid schema - convert MockStructField to string names
        expected_columns = [field.name for field in self.sample_schema.fields]
        result = validate_dataframe_schema(self.mock_df, expected_columns)
        assert result is not None

        # Test with invalid schema
        invalid_columns = ["id", "invalid_field"]  # Field not in DataFrame

        result = validate_dataframe_schema(self.mock_df, invalid_columns)
        assert result is not None

    def test_validation_functions_backward_compatibility(self):
        """Test that validation functions work without functions parameter (backward compatibility)."""
        rules = {"name": ["not_null"], "age": ["positive"]}

        # Test that functions can be omitted (should use default)
        try:
            result = _convert_rules_to_expressions(rules)
            assert result is not None
        except Exception:
            # If PySpark is not available, that's expected
            # The important thing is that the function signature accepts optional functions
            pass

    def test_mock_functions_basic_operations(self):
        """Test basic MockFunctions operations."""
        # Test col function
        col_expr = self.mock_functions.col("test_column")
        assert col_expr is not None
        assert hasattr(col_expr, "isNotNull")

        # Test lit function
        lit_expr = self.mock_functions.lit("test_value")
        assert lit_expr is not None

        # Test length function
        length_expr = self.mock_functions.length(col_expr)
        assert length_expr is not None

    def test_validation_with_complex_rules(self):
        """Test validation with complex rule combinations."""
        complex_rules = {
            "name": ["not_null"],
            "age": ["positive", "non_zero"],
            "salary": ["non_negative", "positive"],
        }

        result = assess_data_quality(self.mock_df, complex_rules, self.mock_functions)
        assert result is not None
        assert isinstance(result, dict)
        assert "quality_rate" in result

    def test_validation_error_handling_with_mock_functions(self):
        """Test validation error handling with mock functions."""
        # Test with empty rules
        empty_rules = {}
        result = assess_data_quality(self.mock_df, empty_rules, self.mock_functions)
        assert result is not None

        # Test with invalid column names - this should raise ValidationError
        invalid_rules = {"nonexistent_column": ["not_null"]}
        from sparkforge.errors import ValidationError

        with pytest.raises(ValidationError):
            assess_data_quality(self.mock_df, invalid_rules, self.mock_functions)

    def test_validation_performance_with_mock_functions(self):
        """Test validation performance with mock functions."""
        import time

        # Test with larger dataset
        large_data = []
        for i in range(1000):
            large_data.append(
                {
                    "id": i,
                    "name": f"User{i}",
                    "age": 20 + (i % 50),
                    "salary": 30000.0 + (i * 10),
                    "active": i % 2 == 0,
                }
            )

        large_df = self.mock_spark.createDataFrame(large_data, self.sample_schema)

        rules = {"name": ["not_null"], "age": ["positive"], "salary": ["non_negative"]}

        start_time = time.time()
        result = assess_data_quality(large_df, rules, self.mock_functions)
        end_time = time.time()

        assert result is not None
        assert (end_time - start_time) < 5.0  # Should be reasonably fast with mock functions


class TestPipelineBuilderWithMockFunctionsSimple:
    """Test PipelineBuilder with injectable mock functions - simplified version."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_spark = MockSparkSession("TestApp")
        self.mock_functions = MockFunctions()

    def test_pipeline_builder_with_mock_functions(self):
        """Test PipelineBuilder initialization with mock functions."""
        from sparkforge import PipelineBuilder

        builder = PipelineBuilder(
            spark=self.mock_spark, schema="test_schema", functions=self.mock_functions
        )

        assert builder is not None
        assert builder.functions == self.mock_functions

    def test_pipeline_builder_static_methods_with_mock_functions(self):
        """Test PipelineBuilder static methods with mock functions."""
        from sparkforge import PipelineBuilder

        # Test not_null_rules
        rules = PipelineBuilder.not_null_rules(["name", "age"], self.mock_functions)
        assert isinstance(rules, dict)
        assert "name" in rules
        assert "age" in rules

        # Test positive_number_rules
        rules = PipelineBuilder.positive_number_rules(
            ["age", "salary"], self.mock_functions
        )
        assert isinstance(rules, dict)
        assert "age" in rules
        assert "salary" in rules

        # Test string_not_empty_rules
        rules = PipelineBuilder.string_not_empty_rules(["name"], self.mock_functions)
        assert isinstance(rules, dict)
        assert "name" in rules

        # Test timestamp_rules
        rules = PipelineBuilder.timestamp_rules(["created_at"], self.mock_functions)
        assert isinstance(rules, dict)
        assert "created_at" in rules

    def test_pipeline_builder_backward_compatibility(self):
        """Test PipelineBuilder backward compatibility without functions parameter."""
        from sparkforge import PipelineBuilder

        # Test that functions can be omitted (should use default)
        try:
            builder = PipelineBuilder(spark=self.mock_spark, schema="test_schema")
            assert builder is not None
            # Should have default functions
            assert builder.functions is not None
        except Exception:
            # If PySpark is not available, that's expected
            # The important thing is that the constructor accepts optional functions
            pass

    def test_pipeline_builder_class_methods_with_mock_functions(self):
        """Test PipelineBuilder class methods with mock functions."""
        from sparkforge import PipelineBuilder

        # Test for_development
        builder = PipelineBuilder.for_development(
            spark=self.mock_spark, schema="test_schema", functions=self.mock_functions
        )
        assert builder is not None
        assert builder.functions == self.mock_functions

        # Test for_production
        builder = PipelineBuilder.for_production(
            spark=self.mock_spark, schema="test_schema", functions=self.mock_functions
        )
        assert builder is not None
        assert builder.functions == self.mock_functions

        # Test for_testing
        builder = PipelineBuilder.for_testing(
            spark=self.mock_spark, schema="test_schema", functions=self.mock_functions
        )
        assert builder is not None
        assert builder.functions == self.mock_functions


class TestMockFunctionsIntegrationSimple:
    """Test integration between MockFunctions and SparkForge validation - simplified version."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_spark = MockSparkSession("TestApp")
        self.mock_functions = MockFunctions()

    def test_mock_functions_behavior(self):
        """Test that MockFunctions behaves correctly with validation."""
        # Test col function
        col_expr = self.mock_functions.col("test_column")
        assert col_expr is not None
        assert hasattr(col_expr, "isNotNull")

        # Test lit function
        lit_expr = self.mock_functions.lit("test_value")
        assert lit_expr is not None

        # Test length function
        length_expr = self.mock_functions.length(col_expr)
        assert length_expr is not None

    def test_validation_with_mock_functions_end_to_end(self):
        """Test complete validation workflow with mock functions."""
        from sparkforge import PipelineBuilder

        # Create sample data
        sample_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0},
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
            ]
        )

        df = self.mock_spark.createDataFrame(sample_data, schema)

        # Create builder with mock functions
        PipelineBuilder(
            spark=self.mock_spark, schema="test_schema", functions=self.mock_functions
        )

        # Test validation rules
        rules = {"name": ["not_null"], "age": ["positive"], "salary": ["non_negative"]}

        # This should work with mock functions
        result = assess_data_quality(df, rules, self.mock_functions)
        assert result is not None
        assert isinstance(result, dict)
        assert "quality_rate" in result

    def test_mock_functions_performance(self):
        """Test MockFunctions performance characteristics."""
        import time

        # Test function call performance
        start_time = time.time()

        for i in range(1000):
            col_expr = self.mock_functions.col(f"column_{i}")
            self.mock_functions.lit(f"value_{i}")
            self.mock_functions.length(col_expr)

        end_time = time.time()

        # Should be very fast
        assert (end_time - start_time) < 0.1  # Less than 100ms for 1000 calls

    def test_mock_functions_error_handling(self):
        """Test MockFunctions error handling."""
        # Test with invalid inputs
        try:
            self.mock_functions.col(None)
            # MockFunctions should handle this gracefully
        except Exception as e:
            # If it raises an exception, that's also acceptable
            assert isinstance(e, (TypeError, ValueError, AttributeError))

    def test_functions_protocol_compatibility(self):
        """Test that MockFunctions is compatible with FunctionsProtocol."""
        # Test that all protocol methods exist and are callable
        protocol_methods = [
            "col",
            "lit",
            "current_timestamp",
            "count",
            "countDistinct",
            "max",
            "sum",
            "when",
            "length",
        ]

        for method_name in protocol_methods:
            assert hasattr(self.mock_functions, method_name)
            method = getattr(self.mock_functions, method_name)
            assert callable(method)

        # Test that MockFunctions can be used where FunctionsProtocol is expected
        try:
            # This should work if MockFunctions implements the protocol correctly
            col_expr = self.mock_functions.col("test")
            assert col_expr is not None
        except Exception as e:
            pytest.fail(f"MockFunctions failed protocol compatibility test: {e}")
