"""
Unified validation system for SparkForge.

This module provides a comprehensive validation system that handles both
data validation and pipeline validation with early error detection and
clear validation messages.

Key Features:
- **String Rules Conversion**: Convert human-readable rules to PySpark expressions
- **Early Validation**: Validate pipeline steps during construction
- **Data Quality Assessment**: Comprehensive data quality validation
- **Clear Error Messages**: Detailed validation errors with suggestions
- **Configurable Thresholds**: Customizable validation thresholds per layer

String Rules Support:
    - "not_null" → F.col("column").isNotNull()
    - "gt", value → F.col("column") > value
    - "lt", value → F.col("column") < value
    - "eq", value → F.col("column") == value
    - "in", [values] → F.col("column").isin(values)
    - "between", min, max → F.col("column").between(min, max)

Example:
    >>> from sparkforge.validation import _convert_rules_to_expressions
    >>> from pyspark.sql import functions as F
    >>>
    >>> # Convert string rules to PySpark expressions
    >>> rules = {"user_id": ["not_null"], "age": ["gt", 0], "status": ["in", ["active", "inactive"]]}
    >>> converted = _convert_rules_to_expressions(rules)
    >>> # Result: {"user_id": [F.col("user_id").isNotNull()], "age": [F.col("age") > 0], ...}
"""

# Import all validation functions and classes for easy access
from .data_validation import (
    _convert_rule_to_expression,
    _convert_rules_to_expressions,
    and_all_rules,
    apply_column_rules,
    assess_data_quality,
    validate_dataframe_schema,
)
from .pipeline_validation import StepValidator, UnifiedValidator, ValidationResult
from .utils import get_dataframe_info, safe_divide
from ..types import ColumnRules

# Make all validation components available at package level
__all__ = [
    # Data validation functions
    "_convert_rule_to_expression",
    "_convert_rules_to_expressions",
    "and_all_rules",
    "apply_column_rules",
    "assess_data_quality",
    "validate_dataframe_schema",
    # Pipeline validation
    "StepValidator",
    "ValidationResult",
    "UnifiedValidator",
    # Utility functions
    "safe_divide",
    "get_dataframe_info",
    # Types
    "ColumnRules",
]
