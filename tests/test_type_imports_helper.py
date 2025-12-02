"""
Helper utility for consistent type imports across tests.

This module provides a standardized way to import Spark types (StructType, StructField, etc.)
based on SPARK_MODE environment variable, ensuring tests work with both mock-spark and real PySpark.
"""
import os


def get_spark_types():
    """
    Get Spark types based on SPARK_MODE environment variable.
    
    Returns:
        Module-like object with types (StructType, StructField, StringType, etc.)
    """
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
    
    if spark_mode == "real":
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
        return {
            "DoubleType": DoubleType,
            "IntegerType": IntegerType,
            "StringType": StringType,
            "StructField": StructField,
            "StructType": StructType,
        }
    else:
        from mock_spark.spark_types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
        return {
            "DoubleType": DoubleType,
            "IntegerType": IntegerType,
            "StringType": StringType,
            "StructField": StructField,
            "StructType": StructType,
        }

