# validation.py
"""
Data validation utilities for the SparkForge pipeline framework.

This module contains functions for validating DataFrames, applying column rules,
and assessing data quality.
"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime
import time
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType

from .models import StageStats, ColumnRules
from .exceptions import ValidationError

logger = logging.getLogger(__name__)


def _convert_rule_to_expression(rule: str, column_name: str) -> Any:
    """
    Convert a string rule to a PySpark Column expression.
    
    Args:
        rule: String rule name (e.g., "not_null", "positive")
        column_name: Name of the column to apply the rule to
        
    Returns:
        PySpark Column expression
    """
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
        # This allows for custom rules to be passed as expressions
        return rule


def _convert_rules_to_expressions(rules: ColumnRules) -> ColumnRules:
    """
    Convert string rules to PySpark Column expressions.
    
    Args:
        rules: Dictionary of column rules (can contain strings or expressions)
        
    Returns:
        Dictionary with all rules converted to PySpark expressions
    """
    converted_rules = {}
    for column_name, rule_list in rules.items():
        converted_rule_list = []
        for rule in rule_list:
            if isinstance(rule, str):
                converted_rule_list.append(_convert_rule_to_expression(rule, column_name))
            else:
                # Already a PySpark expression
                converted_rule_list.append(rule)
        converted_rules[column_name] = converted_rule_list
    return converted_rules


def and_all_rules(rules: ColumnRules) -> Any:
    """
    Combine all validation rules with AND logic.
    
    Args:
        rules: Dictionary of column rules (can contain strings or expressions)
        
    Returns:
        Combined predicate expression
    """
    if not rules:
        return F.lit(True)
    
    # Convert string rules to PySpark expressions
    converted_rules = _convert_rules_to_expressions(rules)
    
    pred = F.lit(True)
    for _, exprs in converted_rules.items():
        for e in exprs:
            pred = pred & e
    return pred


def validate_dataframe_schema(df: DataFrame, expected_columns: List[str]) -> bool:
    """
    Validate that DataFrame has expected columns.
    
    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
        
    Returns:
        True if schema is valid, False otherwise
    """
    actual_columns = set(df.columns)
    expected_columns_set = set(expected_columns)
    
    missing_columns = expected_columns_set - actual_columns
    if missing_columns:
        logger.warning(f"Missing columns: {missing_columns}")
        return False
    
    return True


def get_dataframe_info(df: DataFrame) -> Dict[str, Any]:
    """
    Get comprehensive information about a DataFrame.
    
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
            "is_empty": row_count == 0
        }
    except Exception as e:
        logger.error(f"Failed to get DataFrame info: {e}")
        return {
            "row_count": 0,
            "column_count": 0,
            "columns": [],
            "schema": "unknown",
            "is_empty": True,
            "error": str(e)
        }


def apply_column_rules(
    df: DataFrame,
    rules: ColumnRules,
    stage: str,
    step: str,
) -> Tuple[DataFrame, DataFrame, StageStats]:
    """
    Apply validation rules to a DataFrame and return valid/invalid DataFrames with stats.
    
    Args:
        df: DataFrame to validate
        rules: Column validation rules
        stage: Stage name (bronze/silver/gold)
        step: Step name
        
    Returns:
        Tuple of (valid_df, invalid_df, stats)
        
    Raises:
        ValidationError: If validation fails
    """
    if rules is None:
        raise ValidationError(f"[{stage}:{step}] Validation rules cannot be None.")

    t0 = time.time()
    total = df.count()

    if rules:
        # Convert string rules to PySpark expressions
        converted_rules = _convert_rules_to_expressions(rules)
        logger.debug(f"[{stage}:{step}] Original rules: {rules}")
        logger.debug(f"[{stage}:{step}] Converted rules: {converted_rules}")
        pred = and_all_rules(rules)
        marked = df.withColumn("__is_valid__", pred)
        valid_df = marked.filter(F.col("__is_valid__")).drop("__is_valid__")
        invalid_df = marked.filter(~F.col("__is_valid__")).drop("__is_valid__")
        logger.debug(f"[{stage}:{step}] Validation completed - valid rows: {valid_df.count()}, invalid rows: {invalid_df.count()}")

        # Add detailed failure information
        failed_arrays = []
        for col_name, exprs in converted_rules.items():
            for idx, e in enumerate(exprs):
                tag = F.lit(f"{col_name}#{idx + 1}")
                failed_arrays.append(
                    F.when(~e, F.array(tag)).otherwise(F.array().cast(ArrayType(StringType())))
                )
        
        if failed_arrays:
            invalid_df = invalid_df.withColumn(
                "_failed_rules", F.array_distinct(F.flatten(F.array(*failed_arrays)))
            )
    else:
        valid_df, invalid_df = df, df.limit(0)

    valid_count = valid_df.count()
    invalid_count = total - valid_count
    rate = safe_divide(valid_count * 100.0, total, 100.0)

    # Select only columns that have rules (if any) for all stages
    keep_cols = [c for c in rules.keys() if c in valid_df.columns] if rules else valid_df.columns
    valid_proj = valid_df.select(*keep_cols) if keep_cols else valid_df
    logger.debug(f"[{stage}:{step}] Filtering columns based on rules keys: {list(rules.keys()) if rules else 'no rules'}")
    logger.debug(f"[{stage}:{step}] Available columns: {valid_df.columns}")
    logger.debug(f"[{stage}:{step}] Keeping columns: {keep_cols}")
    logger.debug(f"[{stage}:{step}] Final columns: {valid_proj.columns}")

    stats = StageStats(
        stage=stage,
        step=step,
        total_rows=total,
        valid_rows=valid_count,
        invalid_rows=invalid_count,
        validation_rate=rate,
        duration_secs=round(time.time() - t0, 6),
    )
    
    logger.info(f"Validation completed for {stage}:{step} - {valid_count}/{total} rows valid ({rate:.2f}%)")
    return valid_proj, invalid_df, stats


def assess_data_quality(df: DataFrame, rules: Optional[ColumnRules] = None) -> Dict[str, Any]:
    """
    Assess data quality of a DataFrame.
    
    Args:
        df: DataFrame to assess
        rules: Optional validation rules
        
    Returns:
        Dictionary with data quality metrics
    """
    info = get_dataframe_info(df)
    
    if info["is_empty"]:
        return {
            "total_rows": 0,
            "quality_score": 100.0,
            "issues": ["Empty dataset"],
            "recommendations": ["Check data source"]
        }
    
    total_rows = info["row_count"]
    quality_issues = []
    recommendations = []
    
    # Check for null values
    null_counts = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            null_counts[col] = null_count
            null_percentage = (null_count / total_rows) * 100
            if null_percentage > 50:
                quality_issues.append(f"High null percentage in {col}: {null_percentage:.1f}%")
                recommendations.append(f"Investigate null values in {col}")
    
    # Check for duplicates
    duplicate_count = total_rows - df.distinct().count()
    if duplicate_count > 0:
        duplicate_percentage = (duplicate_count / total_rows) * 100
        quality_issues.append(f"Duplicate rows: {duplicate_count} ({duplicate_percentage:.1f}%)")
        recommendations.append("Consider deduplication strategy")
    
    # Apply validation rules if provided
    if rules:
        try:
            _, _, stats = apply_column_rules(df, rules, "quality_check", "assessment")
            if stats.validation_rate < 95:
                quality_issues.append(f"Low validation rate: {stats.validation_rate:.1f}%")
                recommendations.append("Review data validation rules")
        except Exception as e:
            quality_issues.append(f"Validation error: {e}")
    
    # Calculate quality score
    quality_score = 100.0
    if quality_issues:
        quality_score = max(0, 100 - len(quality_issues) * 10)
    
    return {
        "total_rows": total_rows,
        "quality_score": quality_score,
        "null_counts": null_counts,
        "duplicate_rows": duplicate_count,
        "issues": quality_issues,
        "recommendations": recommendations
    }


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if denominator is zero
        
    Returns:
        Division result or default value
    """
    return numerator / denominator if denominator != 0 else default
