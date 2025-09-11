# data_utils.py
"""
Data utilities for the SparkForge pipeline framework.

This module contains functions for DataFrame manipulation, transformation,
and common data operations.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from datetime import datetime
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .performance import now_dt

logger = logging.getLogger(__name__)


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
