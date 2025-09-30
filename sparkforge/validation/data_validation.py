"""
Data validation functions for SparkForge.

This module provides functions for validating data using PySpark expressions,
including string rule conversion, column validation, and data quality assessment.
"""

from __future__ import annotations

import time
from typing import Any, Dict

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..errors import ValidationError
from ..logging import PipelineLogger
from ..models import ColumnRules, StageStats

logger = PipelineLogger("DataValidation")


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
) -> Dict[str, list[str | Column]]:
    """Convert string rules to PySpark Column expressions."""
    converted_rules: Dict[str, list[str | Column]] = {}
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

    # Validate that all columns referenced in rules exist in the DataFrame
    df_columns = set(df.columns)
    rule_columns = set(rules.keys())
    missing_columns = rule_columns - df_columns
    
    if missing_columns:
        available_columns = sorted(df_columns)
        missing_columns_list = sorted(missing_columns)
        raise ValidationError(
            f"Columns referenced in validation rules do not exist in DataFrame. "
            f"Missing columns: {missing_columns_list}. "
            f"Available columns: {available_columns}. "
            f"Stage: {stage}, Step: {step}"
        )

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


def assess_data_quality(
    df: DataFrame, rules: ColumnRules | None = None
) -> Dict[str, Any]:
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
    except ValidationError as e:
        # Re-raise validation errors as they are specific and actionable
        raise e
    except Exception as e:
        # Log the unexpected error and re-raise with context
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Unexpected error in assess_data_quality: {e}")
        raise ValidationError(
            f"Data quality assessment failed: {e}",
            context={"function": "assess_data_quality", "original_error": str(e)}
        ) from e
