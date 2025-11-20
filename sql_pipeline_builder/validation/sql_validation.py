"""
SQL validation functions for the framework.

This module provides functions for validating data using SQLAlchemy expressions,
including applying validation rules to queries and calculating validation statistics.
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import StageStats
from pipeline_builder_base.validation import safe_divide
from sql_pipeline_builder.types import SqlColumnRules

logger = PipelineLogger("SqlValidation")


def apply_sql_validation_rules(
    query: Any,
    rules: SqlColumnRules,
    step_name: str,
    session: Any,
) -> Tuple[Any, Any, StageStats]:
    """
    Apply SQLAlchemy validation rules to a query.

    Args:
        query: SQLAlchemy Query object
        rules: Dictionary mapping column names to validation rule lists
        step_name: Name of the step being validated
        session: SQLAlchemy session for executing queries

    Returns:
        Tuple of (valid_query, invalid_query, stats)
        - valid_query: Query with valid rows
        - invalid_query: Query with invalid rows
        - stats: StageStats with validation statistics
    """
    from datetime import datetime
    from pipeline_builder_base.models import PipelinePhase

    start_time = datetime.now()

    try:
        # Apply all validation rules as filters
        # SQLAlchemy rules are already ColumnElement expressions
        valid_query = query
        for column_name, rule_list in rules.items():
            for rule in rule_list:
                # Rule is already a SQLAlchemy ColumnElement expression
                # Apply it as a filter
                valid_query = valid_query.filter(rule)

        # Get counts
        # For valid rows, count the filtered query
        valid_count = valid_query.count() if hasattr(valid_query, "count") else 0

        # For invalid rows, we need to count the original query minus valid
        # This is approximate - in practice, we'd need to execute both queries
        total_count = query.count() if hasattr(query, "count") else 0
        invalid_count = max(0, total_count - valid_count)

        # Calculate validation rate
        validation_rate = safe_divide(valid_count, total_count, 0.0) * 100

        # Create invalid query (rows that don't match any rule)
        # This is a simplified approach - in practice, you'd need to negate the rules
        invalid_query = query.filter(~valid_query.whereclause) if hasattr(query, "whereclause") else query.filter(False)

        end_time = datetime.now()
        duration_secs = (end_time - start_time).total_seconds()

        stats = StageStats(
            stage="validation",
            step=step_name,
            total_rows=total_count,
            valid_rows=valid_count,
            invalid_rows=invalid_count,
            validation_rate=validation_rate,
            duration_secs=duration_secs,
            start_time=start_time,
            end_time=end_time,
        )

        logger.info(
            f"Validation for {step_name}: {valid_count}/{total_count} valid "
            f"({validation_rate:.2f}%)"
        )

        return valid_query, invalid_query, stats

    except Exception as e:
        logger.error(f"Validation failed for {step_name}: {e}")
        # Return original query as invalid on error
        end_time = datetime.now()
        duration_secs = (end_time - start_time).total_seconds()
        
        stats = StageStats(
            stage="validation",
            step=step_name,
            total_rows=0,
            valid_rows=0,
            invalid_rows=0,
            validation_rate=0.0,
            duration_secs=duration_secs,
            start_time=start_time,
            end_time=end_time,
        )
        
        return query, query, stats


def validate_query(
    query: Any,
    rules: SqlColumnRules,
    step_name: str,
    session: Any,
    min_validation_rate: float = 95.0,
) -> Tuple[bool, StageStats]:
    """
    Validate a query against rules and check if it meets the minimum validation rate.

    Args:
        query: SQLAlchemy Query object
        rules: Dictionary mapping column names to validation rule lists
        step_name: Name of the step being validated
        session: SQLAlchemy session for executing queries
        min_validation_rate: Minimum validation rate required (0-100)

    Returns:
        Tuple of (is_valid, stats)
    """
    valid_query, invalid_query, stats = apply_sql_validation_rules(
        query, rules, step_name, session
    )

    is_valid = stats.validation_rate >= min_validation_rate

    if not is_valid:
        logger.warning(
            f"Validation failed for {step_name}: "
            f"{stats.validation_rate:.2f}% < {min_validation_rate}%"
        )

    return is_valid, stats

