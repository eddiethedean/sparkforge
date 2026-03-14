"""
Helper utility for consistent type imports across tests.

This module provides a standardized way to import Spark types (StructType, StructField, etc.)
using the pipeline_builder compatibility layer, ensuring tests work with both sparkless and PySpark.
"""

from pipeline_builder.compat import types


def get_spark_types():
    """
    Get Spark types from the compatibility layer.

    Returns:
        Dict with types (StructType, StructField, StringType, etc.)
    """
    return {
        "DoubleType": types.DoubleType,
        "IntegerType": types.IntegerType,
        "StringType": types.StringType,
        "StructField": types.StructField,
        "StructType": types.StructType,
    }
