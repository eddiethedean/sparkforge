# utils.py
"""
Enhanced utility functions and helpers for the Pipeline Builder.

This module contains utility functions for data validation, transformation,
table operations, report generation, and common pipeline operations.

Key Features:
- Comprehensive data validation utilities
- Enhanced table operations with error handling
- Rich reporting and statistics generation
- Performance monitoring and timing utilities
- Data quality assessment tools
- Common pipeline operations
"""

from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional, Callable
from datetime import datetime
import time
import logging
from functools import wraps
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType
from pyspark.sql.utils import AnalysisException

from .models import StageStats, ColumnRules, PipelinePhase, ValidationResult


# ============================================================================
# Logging Setup
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# Custom Exceptions
# ============================================================================

class ValidationError(Exception):
    """Raised when data validation fails."""
    pass


class TableOperationError(Exception):
    """Raised when table operations fail."""
    pass


class PerformanceError(Exception):
    """Raised when performance thresholds are exceeded."""
    pass


# ============================================================================
# Decorators and Context Managers
# ============================================================================

def time_operation(operation_name: str = "operation"):
    """Decorator to time operations and log performance."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger.info(f"Starting {operation_name}...")
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(f"Completed {operation_name} in {duration:.3f}s")
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(f"Failed {operation_name} after {duration:.3f}s: {e}")
                raise
        
        return wrapper
    return decorator


@contextmanager
def performance_monitor(operation_name: str, max_duration: Optional[float] = None):
    """Context manager to monitor operation performance."""
    start_time = time.time()
    logger.info(f"Starting {operation_name}...")
    
    try:
        yield
        duration = time.time() - start_time
        logger.info(f"Completed {operation_name} in {duration:.3f}s")
        
        if max_duration and duration > max_duration:
            logger.warning(f"{operation_name} took {duration:.3f}s, exceeding threshold of {max_duration}s")
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Failed {operation_name} after {duration:.3f}s: {e}")
        raise


# ============================================================================
# Basic Utilities
# ============================================================================

def fqn(schema: str, table: str) -> str:
    """
    Create a fully qualified table name.
    
    Args:
        schema: Database schema name
        table: Table name
        
    Returns:
        Fully qualified table name
        
    Raises:
        ValueError: If schema or table is empty
    """
    if not schema or not table:
        raise ValueError("Schema and table names cannot be empty")
    return f"{schema}.{table}"


def now_dt() -> datetime:
    """Get current UTC datetime."""
    return datetime.utcnow()


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


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


# ============================================================================
# Table Operations
# ============================================================================

@time_operation("table write (overwrite)")
def write_overwrite_table(df: DataFrame, fqn: str, **options) -> int:
    """
    Write DataFrame to Delta table in overwrite mode.
    
    Args:
        df: DataFrame to write
        fqn: Fully qualified table name
        **options: Additional write options
        
    Returns:
        Number of rows written
        
    Raises:
        TableOperationError: If write operation fails
    """
    try:
        cnt = df.count()
        writer = (df.write
                 .format("parquet")
                 .mode("overwrite")
                 .option("overwriteSchema", "true"))
        
        # Apply additional options
        for key, value in options.items():
            writer = writer.option(key, value)
        
        writer.saveAsTable(fqn)
        logger.info(f"Successfully wrote {cnt} rows to {fqn} in overwrite mode")
        return cnt
        
    except Exception as e:
        raise TableOperationError(f"Failed to write table {fqn}: {e}")


@time_operation("table write (append)")
def write_append_table(df: DataFrame, fqn: str, **options) -> int:
    """
    Write DataFrame to Delta table in append mode.
    
    Args:
        df: DataFrame to write
        fqn: Fully qualified table name
        **options: Additional write options
        
    Returns:
        Number of rows written
        
    Raises:
        TableOperationError: If write operation fails
    """
    try:
        cnt = df.count()
        writer = (df.write
                 .format("parquet")
                 .mode("append"))
        
        # Apply additional options
        for key, value in options.items():
            writer = writer.option(key, value)
        
        writer.saveAsTable(fqn)
        logger.info(f"Successfully wrote {cnt} rows to {fqn} in append mode")
        return cnt
        
    except Exception as e:
        raise TableOperationError(f"Failed to write table {fqn}: {e}")


def read_table(spark: SparkSession, fqn: str) -> DataFrame:
    """
    Read data from a Delta table.
    
    Args:
        spark: Spark session
        fqn: Fully qualified table name
        
    Returns:
        DataFrame with table data
        
    Raises:
        TableOperationError: If read operation fails
    """
    try:
        df = spark.table(fqn)
        logger.info(f"Successfully read table {fqn}")
        return df
    except AnalysisException as e:
        raise TableOperationError(f"Table {fqn} does not exist: {e}")
    except Exception as e:
        raise TableOperationError(f"Failed to read table {fqn}: {e}")


def table_exists(spark: SparkSession, fqn: str) -> bool:
    """
    Check if a table exists.
    
    Args:
        spark: Spark session
        fqn: Fully qualified table name
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        spark.table(fqn).count()
        return True
    except AnalysisException:
        return False
    except Exception:
        return False


def drop_table(spark: SparkSession, fqn: str) -> bool:
    """
    Drop a table if it exists.
    
    Args:
        spark: Spark session
        fqn: Fully qualified table name
        
    Returns:
        True if table was dropped, False if it didn't exist
    """
    try:
        if table_exists(spark, fqn):
            spark.sql(f"DROP TABLE {fqn}")
            logger.info(f"Dropped table {fqn}")
            return True
        return False
    except Exception as e:
        logger.warning(f"Failed to drop table {fqn}: {e}")
        return False


# ============================================================================
# Data Validation
# ============================================================================

def and_all_rules(rules: ColumnRules) -> Any:
    """
    Combine all validation rules with AND logic.
    
    Args:
        rules: Dictionary of column rules
        
    Returns:
        Combined predicate expression
    """
    if not rules:
        return F.lit(True)
    
    pred = F.lit(True)
    for _, exprs in rules.items():
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


@time_operation("data validation")
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
        pred = and_all_rules(rules)
        marked = df.withColumn("__is_valid__", pred)
        valid_df = marked.filter(F.col("__is_valid__")).drop("__is_valid__")
        invalid_df = marked.filter(~F.col("__is_valid__")).drop("__is_valid__")

        # Add detailed failure information
        failed_arrays = []
        for col_name, exprs in rules.items():
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

    # Select only columns that have rules (if any)
    keep_cols = [c for c in rules.keys() if c in valid_df.columns] if rules else valid_df.columns
    valid_proj = valid_df.select(*keep_cols) if keep_cols else valid_df

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


# ============================================================================
# Performance Monitoring
# ============================================================================

@time_operation("write operation")
def time_write_operation(mode: str, df: DataFrame, fqn: str, **options) -> Tuple[int, float, datetime, datetime]:
    """
    Time a write operation and return results with timing info.
    
    Args:
        mode: Write mode (overwrite/append)
        df: DataFrame to write
        fqn: Fully qualified table name
        **options: Additional write options
        
    Returns:
        Tuple of (rows_written, duration_secs, start_time, end_time)
        
    Raises:
        ValueError: If mode is invalid
        TableOperationError: If write operation fails
    """
    start = now_dt()
    t0 = time.time()
    
    try:
        if mode == "overwrite":
            rows = write_overwrite_table(df, fqn, **options)
        elif mode == "append":
            rows = write_append_table(df, fqn, **options)
        else:
            raise ValueError(f"Unknown write mode '{mode}'. Supported modes: overwrite, append")
        
        t1 = time.time()
        end = now_dt()
        duration = round(t1 - t0, 3)
        
        logger.info(f"Write operation completed: {rows} rows in {duration}s to {fqn}")
        return rows, duration, start, end
        
    except Exception as e:
        t1 = time.time()
        end = now_dt()
        duration = round(t1 - t0, 3)
        logger.error(f"Write operation failed after {duration}s: {e}")
        raise


def monitor_performance(operation_name: str, max_duration: Optional[float] = None) -> Callable:
    """
    Decorator factory for performance monitoring.
    
    Args:
        operation_name: Name of the operation
        max_duration: Maximum allowed duration in seconds
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with performance_monitor(operation_name, max_duration):
                return func(*args, **kwargs)
        return wrapper
    return decorator


# ============================================================================
# Reporting Utilities
# ============================================================================

def create_validation_dict(stats: StageStats, *, start_at: datetime, end_at: datetime) -> Dict[str, Any]:
    """
    Create validation dictionary for reporting.
    
    Args:
        stats: Stage statistics
        start_at: Start time
        end_at: End time
        
    Returns:
        Validation dictionary
    """
    if stats is None:
        return {
            "stage": None, "step": None,
            "total_rows": 0, "valid_rows": 0, "invalid_rows": 0,
            "validation_rate": 100.0, "duration_secs": 0.0,
            "start_at": start_at, "end_at": end_at,
        }
    
    return {
        "stage": stats.stage, "step": stats.step,
        "total_rows": stats.total_rows,
        "valid_rows": stats.valid_rows,
        "invalid_rows": stats.invalid_rows,
        "validation_rate": round(stats.validation_rate, 2),
        "duration_secs": round(stats.duration_secs, 3),
        "start_at": start_at, "end_at": end_at,
    }


def create_transform_dict(
    input_rows: int, 
    output_rows: int, 
    duration_secs: float, 
    skipped: bool, 
    *, 
    start_at: datetime, 
    end_at: datetime
) -> Dict[str, Any]:
    """
    Create transform dictionary for reporting.
    
    Args:
        input_rows: Number of input rows
        output_rows: Number of output rows
        duration_secs: Duration in seconds
        skipped: Whether operation was skipped
        start_at: Start time
        end_at: End time
        
    Returns:
        Transform dictionary
    """
    return {
        "input_rows": int(input_rows),
        "output_rows": int(output_rows),
        "duration_secs": round(duration_secs, 3),
        "skipped": bool(skipped),
        "start_at": start_at, "end_at": end_at,
    }


def create_write_dict(
    mode: str, 
    rows: int, 
    duration_secs: float, 
    table_fqn: str, 
    skipped: bool, 
    *, 
    start_at: datetime, 
    end_at: datetime
) -> Dict[str, Any]:
    """
    Create write dictionary for reporting.
    
    Args:
        mode: Write mode
        rows: Number of rows written
        duration_secs: Duration in seconds
        table_fqn: Fully qualified table name
        skipped: Whether operation was skipped
        start_at: Start time
        end_at: End time
        
    Returns:
        Write dictionary
    """
    return {
        "mode": mode, "rows_written": int(rows),
        "duration_secs": round(duration_secs, 3),
        "table_fqn": table_fqn, "skipped": bool(skipped),
        "start_at": start_at, "end_at": end_at,
    }


def create_summary_report(
    total_steps: int,
    successful_steps: int,
    failed_steps: int,
    total_duration: float,
    total_rows_processed: int,
    total_rows_written: int,
    avg_validation_rate: float
) -> Dict[str, Any]:
    """
    Create a summary report for pipeline execution.
    
    Args:
        total_steps: Total number of steps
        successful_steps: Number of successful steps
        failed_steps: Number of failed steps
        total_duration: Total duration in seconds
        total_rows_processed: Total rows processed
        total_rows_written: Total rows written
        avg_validation_rate: Average validation rate
        
    Returns:
        Summary report dictionary
    """
    success_rate = safe_divide(successful_steps * 100.0, total_steps, 0.0)
    failure_rate = 100.0 - success_rate
    
    return {
        "execution_summary": {
            "total_steps": total_steps,
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "success_rate": round(success_rate, 2),
            "failure_rate": round(failure_rate, 2)
        },
        "performance_metrics": {
            "total_duration_secs": round(total_duration, 3),
            "formatted_duration": format_duration(total_duration),
            "avg_validation_rate": round(avg_validation_rate, 2)
        },
        "data_metrics": {
            "total_rows_processed": total_rows_processed,
            "total_rows_written": total_rows_written,
            "processing_efficiency": round(safe_divide(total_rows_written * 100.0, total_rows_processed, 0.0), 2)
        }
    }


# ============================================================================
# Data Transformation Utilities
# ============================================================================

def add_metadata_columns(df: DataFrame, run_id: str, created_at: Optional[datetime] = None) -> DataFrame:
    """
    Add metadata columns to a DataFrame.
    
    Args:
        df: DataFrame to enhance
        run_id: Pipeline run ID
        created_at: Creation timestamp (defaults to now)
        
    Returns:
        DataFrame with metadata columns
    """
    if created_at is None:
        created_at = now_dt()
    
    return (df
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_created_at", F.lit(created_at))
            .withColumn("_updated_at", F.lit(created_at)))


def remove_metadata_columns(df: DataFrame) -> DataFrame:
    """
    Remove metadata columns from a DataFrame.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        DataFrame without metadata columns
    """
    metadata_columns = ["_run_id", "_created_at", "_updated_at", "__is_valid__", "_failed_rules"]
    existing_metadata = [col for col in metadata_columns if col in df.columns]
    
    if existing_metadata:
        return df.drop(*existing_metadata)
    return df


def create_empty_dataframe(spark: SparkSession, schema: StructType) -> DataFrame:
    """
    Create an empty DataFrame with the specified schema.
    
    Args:
        spark: Spark session
        schema: DataFrame schema
        
    Returns:
        Empty DataFrame with specified schema
    """
    return spark.createDataFrame([], schema)


# ============================================================================
# Utility Functions for Common Operations
# ============================================================================

def coalesce_dataframes(dataframes: List[DataFrame]) -> DataFrame:
    """
    Coalesce multiple DataFrames into one.
    
    Args:
        dataframes: List of DataFrames to coalesce
        
    Returns:
        Coalesced DataFrame
        
    Raises:
        ValueError: If no DataFrames provided
    """
    if not dataframes:
        raise ValueError("At least one DataFrame must be provided")
    
    if len(dataframes) == 1:
        return dataframes[0]
    
    # Use union to combine DataFrames
    result = dataframes[0]
    for df in dataframes[1:]:
        result = result.union(df)
    
    return result


def sample_dataframe(df: DataFrame, fraction: float = 0.1, seed: int = 42) -> DataFrame:
    """
    Sample a DataFrame for testing or analysis.
    
    Args:
        df: DataFrame to sample
        fraction: Sampling fraction (0.0 to 1.0)
        seed: Random seed for reproducibility
        
    Returns:
        Sampled DataFrame
    """
    if not 0 < fraction <= 1:
        raise ValueError("Fraction must be between 0 and 1")
    
    return df.sample(fraction=fraction, seed=seed)


def get_column_statistics(df: DataFrame, column: str) -> Dict[str, Any]:
    """
    Get statistics for a specific column.
    
    Args:
        df: DataFrame to analyze
        column: Column name
        
    Returns:
        Dictionary with column statistics
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in DataFrame")
    
    try:
        stats = df.select(column).describe().collect()
        result = {}
        for row in stats:
            result[row[0]] = row[1]
        return result
    except Exception as e:
        logger.error(f"Failed to get statistics for column {column}: {e}")
        return {"error": str(e)}


# ============================================================================
# Legacy Functions (for backward compatibility)
# ============================================================================

# These functions are kept for backward compatibility but may be deprecated
# in future versions. Use the enhanced versions above instead.

def legacy_time_write_operation(mode: str, df: DataFrame, fqn: str) -> Tuple[int, float, datetime, datetime]:
    """Legacy version of time_write_operation for backward compatibility."""
    return time_write_operation(mode, df, fqn)