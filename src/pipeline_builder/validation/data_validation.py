# mypy: ignore-errors
"""
Data validation functions for the framework.

This module provides functions for validating data using PySpark expressions,
including string rule conversion, column validation, and data quality assessment.

# Depends on:
#   compat
#   errors
#   functions
#   logging
#   models.execution
#   models.types
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional, Union, cast

from pipeline_builder_base.errors import ValidationError
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import StageStats

from ..compat import Column, DataFrame
from ..compat_helpers import detect_spark_type
from ..functions import FunctionsProtocol, get_default_functions
from ..models import ColumnRules

logger = PipelineLogger("DataValidation")


def _convert_rule_to_expression(
    rule: Union[str, list],
    column_name: str,
    functions: Optional[FunctionsProtocol] = None,
) -> Column:
    """Convert a string rule to a PySpark Column expression."""
    if functions is None:
        functions = get_default_functions()

    # Handle list-based rules like ["gt", 0]
    if isinstance(rule, list):
        if len(rule) == 0:
            # Empty rule means no validation
            return functions.lit(True)
        elif len(rule) == 1:
            return _convert_rule_to_expression(rule[0], column_name, functions)
        elif len(rule) == 2:
            op, value = rule
            if op == "gt":
                result = functions.col(column_name) > value
                return cast(Column, result)
            elif op == "gte":
                result = functions.col(column_name) >= value
                return cast(Column, result)
            elif op == "lt":
                result = functions.col(column_name) < value
                return cast(Column, result)
            elif op == "lte":
                result = functions.col(column_name) <= value
                return cast(Column, result)
            elif op == "eq":
                result = functions.col(column_name) == value
                return cast(Column, result)
            elif op == "ne":
                result = functions.col(column_name) != value
                return cast(Column, result)
            elif op == "in":
                if not isinstance(value, (list, tuple, set)):
                    raise ValidationError(
                        f"'in' rule for column '{column_name}' requires list/tuple/set values"
                    )
                result = functions.col(column_name).isin(list(value))  # type: ignore[attr-defined]
                return cast(Column, result)
            elif op == "not_in":
                if not isinstance(value, (list, tuple, set)):
                    raise ValidationError(
                        f"'not_in' rule for column '{column_name}' requires list/tuple/set values"
                    )
                result = ~functions.col(column_name).isin(list(value))  # type: ignore[attr-defined]
                return cast(Column, result)
            elif op == "like":
                result = functions.col(column_name).like(value)
                return cast(Column, result)
            else:
                # For unknown operators, assume it's a valid PySpark expression
                return functions.expr(f"{column_name} {op} {value}")
        elif len(rule) == 3:
            op, min_val, max_val = rule
            if op == "between":
                result = functions.col(column_name).between(min_val, max_val)
                return result
            else:
                # For unknown operators, assume it's a valid PySpark expression
                return functions.expr(f"{column_name} {op} {min_val} {max_val}")
        else:
            # For complex rules, assume it's a valid PySpark expression
            return functions.expr(str(rule))

    # Handle string-based rules
    if rule == "not_null":
        result = functions.col(column_name).isNotNull()
        return result
    elif rule == "positive":
        result = functions.col(column_name) > 0
        return result
    elif rule == "non_negative":
        result = functions.col(column_name) >= 0
        return result
    elif rule == "non_zero":
        return functions.col(column_name) != 0
    else:
        # For unknown rules, assume it's a valid PySpark expression
        return functions.expr(rule)


def _convert_rules_to_expressions(
    rules: ColumnRules,
    functions: Optional[FunctionsProtocol] = None,
) -> Dict[str, list[Union[str, Column]]]:
    """Convert string rules to PySpark Column expressions."""
    if functions is None:
        functions = get_default_functions()

    converted_rules: Dict[str, list[Union[str, Column]]] = {}
    for column_name, rule_list in rules.items():
        converted_rule_list: list[Union[str, Column]] = []
        for rule in rule_list:
            if isinstance(rule, (str, list)):
                converted_rule_list.append(
                    _convert_rule_to_expression(rule, column_name, functions)
                )
            else:
                converted_rule_list.append(rule)
        converted_rules[column_name] = converted_rule_list
    return converted_rules


def and_all_rules(
    rules: ColumnRules,
    functions: Optional[FunctionsProtocol] = None,
) -> Union[Column, bool]:
    """Combine all validation rules with AND logic."""
    if not rules:
        return True

    if functions is None:
        functions = get_default_functions()

    converted_rules = _convert_rules_to_expressions(rules, functions)
    expressions = []
    for _, exprs in converted_rules.items():
        expressions.extend(exprs)

    if not expressions:
        return True

    # Filter out non-Column expressions and convert strings to Columns
    column_expressions = []
    for expr in expressions:
        # Check if it's a Column-like object (has column operations)
        if isinstance(expr, str):
            column_expressions.append(functions.expr(expr))
        elif isinstance(expr, Column):
            column_expressions.append(expr)
        else:
            # This handles Column-like objects that aren't str or Column
            # Check if it has Column-like methods
            if hasattr(expr, "__and__") and hasattr(expr, "__invert__"):
                column_expressions.append(cast(Column, expr))

    if not column_expressions:
        return True

    pred = column_expressions[0]
    for e in column_expressions[1:]:
        pred = pred & e

    # Note: sparkless 3.17.1+ fixes the bug where combined ColumnOperation expressions
    # were treated as column names, so we can return the combined expression directly
    return pred


def apply_column_rules(
    df: DataFrame,
    rules: ColumnRules,
    stage: str,
    step: str,
    filter_columns_by_rules: bool = True,
    functions: Optional[FunctionsProtocol] = None,
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
        
        # Filter out rules for non-existent columns with a warning
        # This handles cases where transforms drop columns that were in the input
        filtered_rules = {
            col: rules[col] for col in rules.keys() if col in df_columns
        }
        
        if not filtered_rules:
            # All rules reference missing columns - this is an error
            raise ValidationError(
                f"All columns referenced in validation rules do not exist in DataFrame. "
                f"Missing columns: {missing_columns_list}. "
                f"Available columns: {available_columns}. "
                f"Stage: {stage}, Step: {step}. "
                f"This may indicate that the transform function dropped columns that are referenced in validation rules. "
                f"Please update validation rules to only reference columns that exist after the transform."
            )
        
        # Log warning about filtered rules
        logger.warning(
            f"Validation rules reference columns that do not exist in DataFrame after transform. "
            f"Filtered out rules for missing columns: {missing_columns_list}. "
            f"Available columns: {available_columns}. "
            f"Stage: {stage}, Step: {step}. "
            f"This may indicate that the transform function dropped columns. "
            f"Continuing validation with remaining rules for existing columns."
        )
        
        # Use filtered rules for validation
        rules = filtered_rules

    start_time = time.time()

    # Create validation predicate
    validation_predicate = and_all_rules(rules, functions)

    # Apply validation
    if validation_predicate is True:
        # No validation rules, return all data as valid
        valid_df = df
        invalid_df = df.limit(0)  # Empty DataFrame with same schema
        total_rows = df.count()
        valid_rows = total_rows
        invalid_rows = 0
    elif isinstance(validation_predicate, Column) or (
        hasattr(validation_predicate, "__and__")
        and hasattr(validation_predicate, "__invert__")
        and not isinstance(validation_predicate, bool)
    ):
        # Handle PySpark Column expressions
        # Note: sparkless 3.17.1+ fixes the bug where combined ColumnOperation expressions
        # were treated as column names, so we can use the combined predicate for both
        # sparkless and PySpark
        if isinstance(validation_predicate, str):
            validation_predicate = functions.expr(validation_predicate)
        elif not isinstance(validation_predicate, Column):
            # Check if we're in real PySpark mode and predicate is not a PySpark Column
            # This can happen when tests use sparkless functions in real PySpark mode
            try:
                # Try to detect if we're in real PySpark mode
                spark_type = detect_spark_type(df.sql_ctx.sparkSession)  # type: ignore[attr-defined]
                if spark_type == "pyspark":
                    # Check if predicate is a PySpark Column by checking for _jc attribute
                    if not hasattr(validation_predicate, "_jc"):
                        # Not a PySpark Column - try to convert via string representation
                        # This handles ColumnOperation from sparkless
                        try:
                            # Try to get string representation and convert
                            pred_str = str(validation_predicate)
                            validation_predicate = functions.expr(pred_str)
                        except Exception:
                            # If conversion fails, cast and hope it works
                            # This will raise an error if it doesn't work, which is better than silent failure
                            validation_predicate = cast(Column, validation_predicate)
                    else:
                        # It's a PySpark Column, just cast for type checking
                        validation_predicate = cast(Column, validation_predicate)
                else:
                    # Not in real PySpark mode, safe to cast
                    validation_predicate = cast(Column, validation_predicate)
            except Exception:
                # If detection fails, try casting anyway
                validation_predicate = cast(Column, validation_predicate)

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
        rule_columns_list: list[str] = list(rules.keys())
        valid_df = valid_df.select(*rule_columns_list)
        # For invalid_df, also include the _failed_rules column if it exists
        invalid_columns: list[str] = rule_columns_list.copy()
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


def validate_dataframe_schema(
    df: DataFrame,
    expected_columns: list[str],
) -> bool:
    """Validate that DataFrame has expected columns."""
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    missing_columns = expected_set - actual_columns
    return len(missing_columns) == 0


def assess_data_quality(
    df: DataFrame,
    rules: Optional[ColumnRules] = None,
    functions: Optional[FunctionsProtocol] = None,
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
            valid_df, invalid_df, stats = apply_column_rules(
                df, rules, "test", "test", functions=functions
            )
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
            context={"function": "assess_data_quality", "original_error": str(e)},
        ) from e
