"""
Engine implementations for the pipeline builder.

This module provides engine implementations that satisfy the abstracts.Engine interface,
enabling different execution backends (Spark, SQL, etc.).
"""

from .spark_engine import SparkEngine

__all__ = ["SparkEngine"]
