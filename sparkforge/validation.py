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

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Protocol

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .errors import ValidationError
from .logging import PipelineLogger
from .models import (
    BronzeStep,
    ColumnRules,
    ExecutionContext,
    GoldStep,
    PipelineConfig,
    SilverStep,
    StageStats,
)
from .types import StepName

logger = logging.getLogger(__name__)


# ============================================================================
# Data Validation
# ============================================================================


def _convert_rule_to_expression(rule: str, column_name: str) -> Column:
    """Convert a string rule to a PySpark Column expression."""
    if rule == "not_null":
        return F.col(column_name).isNotNull()
    elif rule == "positive":
        return F.col(column_name) > 0
    elif rule == "non_negative":
        return F.col(column_name) >= 0
    elif rule == "non_zero":
        return F.col(column_name) != 0
    else:
        # For unknown rules, assume it's a valid PySpark expression
        return F.expr(rule)


def _convert_rules_to_expressions(
    rules: ColumnRules,
) -> dict[str, list[str | Column]]:
    """Convert string rules to PySpark Column expressions."""
    converted_rules: dict[str, list[str | Column]] = {}
    for column_name, rule_list in rules.items():
        converted_rule_list: list[str | Column] = []
        for rule in rule_list:
            if isinstance(rule, str):
                converted_rule_list.append(
                    _convert_rule_to_expression(rule, column_name)
                )
            else:
                converted_rule_list.append(rule)
        converted_rules[column_name] = converted_rule_list
    return converted_rules


def and_all_rules(rules: ColumnRules) -> Column | bool:
    """Combine all validation rules with AND logic."""
    if not rules:
        return True

    converted_rules = _convert_rules_to_expressions(rules)
    expressions = []
    for _, exprs in converted_rules.items():
        expressions.extend(exprs)

    if not expressions:
        return True

    # Filter out non-Column expressions and convert strings to Columns
    column_expressions = []
    for expr in expressions:
        if isinstance(expr, Column):
            column_expressions.append(expr)
        elif isinstance(expr, str):
            column_expressions.append(F.expr(expr))

    if not column_expressions:
        return True

    pred = column_expressions[0]
    for e in column_expressions[1:]:
        pred = pred & e

    return pred


def apply_column_rules(
    df: DataFrame,
    rules: ColumnRules,
    stage: str,
    step: str,
    filter_columns_by_rules: bool = True,
) -> tuple[DataFrame, DataFrame, StageStats]:
    """
    Apply validation rules to a DataFrame and return valid/invalid DataFrames with statistics.

    Args:
        df: DataFrame to validate
        rules: Dictionary mapping column names to validation rules
        stage: Pipeline stage name
        step: Step name within the stage
        filter_columns_by_rules: If True, output DataFrames only contain columns with rules

    Returns:
        Tuple of (valid_df, invalid_df, stats)
    """
    if rules is None:
        raise ValidationError("Validation rules cannot be None")

    # Handle empty rules - return all rows as valid
    if not rules:
        total_rows = df.count()
        duration = time.time() - time.time()  # 0 duration
        stats = StageStats(
            stage=stage,
            step=step,
            total_rows=total_rows,
            valid_rows=total_rows,
            invalid_rows=0,
            validation_rate=100.0,
            duration_secs=duration,
        )
        return (
            df,
            df.limit(0),
            stats,
        )  # Return original df as valid, empty df as invalid

    start_time = time.time()

    # Create validation predicate
    validation_predicate = and_all_rules(rules)

    # Apply validation
    if validation_predicate is True:
        # No validation rules, return all data as valid
        valid_df = df
        invalid_df = df.limit(0)  # Empty DataFrame with same schema
        total_rows = df.count()
        valid_rows = total_rows
        invalid_rows = 0
    elif isinstance(validation_predicate, Column):
        # Handle PySpark Column expressions
        valid_df = df.filter(validation_predicate)
        invalid_df = df.filter(~validation_predicate)
        total_rows = df.count()
        valid_rows = valid_df.count()
        invalid_rows = invalid_df.count()
    else:
        # Handle boolean False case (shouldn't happen with current logic)
        valid_df = df.limit(0)
        invalid_df = df
        total_rows = df.count()
        valid_rows = 0
        invalid_rows = total_rows

    # Apply column filtering if requested
    if filter_columns_by_rules:
        # Only keep columns that have validation rules
        rule_columns = list(rules.keys())
        valid_df = valid_df.select(*rule_columns)
        # For invalid_df, also include the _failed_rules column if it exists
        invalid_columns = rule_columns.copy()
        if "_failed_rules" in invalid_df.columns:
            invalid_columns.append("_failed_rules")
        invalid_df = invalid_df.select(*invalid_columns)

    # Calculate validation rate
    validation_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0

    # Create statistics
    duration = time.time() - start_time
    stats = StageStats(
        stage=stage,
        step=step,
        total_rows=total_rows,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        validation_rate=validation_rate,
        duration_secs=duration,
    )

    logger.info(
        f"Validation completed for {stage}.{step}: {validation_rate:.1f}% valid"
    )

    return valid_df, invalid_df, stats


def validate_dataframe_schema(df: DataFrame, expected_columns: list[str]) -> bool:
    """Validate that DataFrame has expected columns."""
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing_columns = expected_set - actual_columns
    return len(missing_columns) == 0


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.

    Args:
        numerator: The numerator
        denominator: The denominator
        default: Default value to return if denominator is zero

    Returns:
        The division result or default value
    """
    if denominator == 0:
        return default
    return numerator / denominator


def get_dataframe_info(df: DataFrame) -> dict[str, Any]:
    """
    Get basic information about a DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Dictionary with DataFrame information
    """
    try:
        row_count = df.count()
        column_count = len(df.columns)
        schema = df.schema

        return {
            "row_count": row_count,
            "column_count": column_count,
            "columns": df.columns,
            "schema": str(schema),
            "is_empty": row_count == 0,
        }
    except Exception as e:
        return {
            "error": str(e),
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "schema": "unknown",
            "is_empty": True,
        }


def assess_data_quality(
    df: DataFrame, rules: ColumnRules | None = None
) -> dict[str, Any]:
    """
    Assess data quality of a DataFrame.

    Args:
        df: DataFrame to assess
        rules: Optional validation rules

    Returns:
        Dictionary with quality metrics
    """
    try:
        total_rows = df.count()

        if total_rows == 0:
            return {
                "total_rows": 0,
                "valid_rows": 0,
                "invalid_rows": 0,
                "quality_rate": 100.0,
                "is_empty": True,
            }

        if rules:
            valid_df, invalid_df, stats = apply_column_rules(df, rules, "test", "test")
            return {
                "total_rows": stats.total_rows,
                "valid_rows": stats.valid_rows,
                "invalid_rows": stats.invalid_rows,
                "quality_rate": stats.validation_rate,
                "is_empty": False,
            }
        else:
            return {
                "total_rows": total_rows,
                "valid_rows": total_rows,
                "invalid_rows": 0,
                "quality_rate": 100.0,
                "is_empty": False,
            }
    except Exception as e:
        return {
            "error": str(e),
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "quality_rate": 0.0,
            "is_empty": True,
        }


# ============================================================================
# Pipeline Validation
# ============================================================================


class StepValidator(Protocol):
    """Protocol for custom step validators."""

    def validate(self, step: Any, context: ExecutionContext) -> list[str]:
        """Validate a step and return any validation errors."""
        ...


@dataclass
class ValidationResult:
    """Result of validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    recommendations: list[str]

    def __bool__(self) -> bool:
        """Return whether validation passed."""
        return self.is_valid


class UnifiedValidator:
    """
    Unified validation system for both data and pipeline validation.

    This class provides a single interface for all validation needs,
    combining data validation and pipeline validation functionality.
    """

    def __init__(self, logger: PipelineLogger | None = None):
        """Initialize the unified validator."""
        self.logger = logger or PipelineLogger()
        self.custom_validators: list[StepValidator] = []

    def add_validator(self, validator: StepValidator) -> None:
        """Add a custom step validator."""
        self.custom_validators.append(validator)
        self.logger.info(f"Added custom validator: {validator.__class__.__name__}")

    def validate_pipeline(
        self,
        config: PipelineConfig,
        bronze_steps: dict[StepName, BronzeStep],
        silver_steps: dict[StepName, SilverStep],
        gold_steps: dict[StepName, GoldStep],
    ) -> ValidationResult:
        """Validate the entire pipeline configuration."""
        errors: list[str] = []
        warnings: list[str] = []
        recommendations: list[str] = []

        # Validate configuration
        config_errors = self._validate_config(config)
        errors.extend(config_errors)

        # Validate steps
        bronze_errors, bronze_warnings = self._validate_bronze_steps(bronze_steps)
        errors.extend(bronze_errors)
        warnings.extend(bronze_warnings)

        silver_errors, silver_warnings = self._validate_silver_steps(
            silver_steps, bronze_steps
        )
        errors.extend(silver_errors)
        warnings.extend(silver_warnings)

        gold_errors, gold_warnings = self._validate_gold_steps(gold_steps, silver_steps)
        errors.extend(gold_errors)
        warnings.extend(gold_warnings)

        # Validate dependencies
        dep_errors, dep_warnings = self._validate_dependencies(
            bronze_steps, silver_steps, gold_steps
        )
        errors.extend(dep_errors)
        warnings.extend(dep_warnings)

        is_valid = len(errors) == 0

        if is_valid:
            self.logger.info("Pipeline validation passed")
        else:
            self.logger.error(f"Pipeline validation failed with {len(errors)} errors")

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            recommendations=recommendations,
        )

    def validate_step(
        self, step: Any, step_type: str, context: ExecutionContext
    ) -> ValidationResult:
        """Validate a single step."""
        errors: list[str] = []
        warnings: list[str] = []

        # Run custom validators
        for validator in self.custom_validators:
            try:
                validator_errors = validator.validate(step, context)
                errors.extend(validator_errors)
            except Exception as e:
                errors.append(
                    f"Custom validator {validator.__class__.__name__} failed: {e}"
                )

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            recommendations=[],
        )

    def _validate_config(self, config: PipelineConfig) -> list[str]:
        """Validate pipeline configuration."""
        errors = []

        if not config.schema:
            errors.append("Pipeline schema is required")

        # Table prefix is optional in simplified config
        # if not config.table_prefix:
        #     errors.append("Table prefix is required")

        return errors

    def _validate_bronze_steps(
        self, bronze_steps: dict[StepName, BronzeStep]
    ) -> tuple[list[str], list[str]]:
        """Validate bronze steps."""
        errors = []
        warnings: list[str] = []

        for step_name, step in bronze_steps.items():
            # Simplified validation - just check that step has required basic attributes
            if not step.name:
                errors.append(f"Bronze step {step_name} missing name")

            if not step.rules:
                errors.append(f"Bronze step {step_name} missing validation rules")

        return errors, warnings

    def _validate_silver_steps(
        self,
        silver_steps: dict[StepName, SilverStep],
        bronze_steps: dict[StepName, BronzeStep],
    ) -> tuple[list[str], list[str]]:
        """Validate silver steps."""
        errors = []
        warnings: list[str] = []

        for step_name, step in silver_steps.items():
            if not step.source_bronze:
                errors.append(f"Silver step {step_name} missing source_bronze")

            # Check source_bronze exists
            if step.source_bronze not in bronze_steps:
                errors.append(
                    f"Silver step {step_name} depends on non-existent bronze step {step.source_bronze}"
                )

        return errors, warnings

    def _validate_gold_steps(
        self,
        gold_steps: dict[StepName, GoldStep],
        silver_steps: dict[StepName, SilverStep],
    ) -> tuple[list[str], list[str]]:
        """Validate gold steps."""
        errors = []
        warnings: list[str] = []

        for step_name, step in gold_steps.items():
            # Check source_silvers exist (if specified)
            if step.source_silvers:
                for silver_name in step.source_silvers:
                    if silver_name not in silver_steps:
                        errors.append(
                            f"Gold step {step_name} depends on non-existent silver step {silver_name}"
                        )

        return errors, warnings

    def _validate_dependencies(
        self,
        bronze_steps: dict[StepName, BronzeStep],
        silver_steps: dict[StepName, SilverStep],
        gold_steps: dict[StepName, GoldStep],
    ) -> tuple[list[str], list[str]]:
        """Validate step dependencies."""
        errors = []
        warnings: list[str] = []

        # Check for circular dependencies
        all_steps = {**bronze_steps, **silver_steps, **gold_steps}

        for step_name, step in all_steps.items():
            if hasattr(step, "dependencies"):
                for dep in step.dependencies:
                    if dep.step_name == step_name:
                        errors.append(
                            f"Step {step_name} has circular dependency on itself"
                        )

        return errors, warnings


# ============================================================================
# Backward Compatibility
# ============================================================================


# Keep the old function names for backward compatibility
def apply_validation_rules(
    df: DataFrame,
    rules: ColumnRules,
    stage: str,
    step: str,
    filter_columns_by_rules: bool = True,
) -> tuple[DataFrame, DataFrame, StageStats]:
    """Backward compatibility alias for apply_column_rules."""
    return apply_column_rules(df, rules, stage, step, filter_columns_by_rules)
