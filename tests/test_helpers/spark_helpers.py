"""
Spark session management utilities for tests.

This module provides utilities for creating and managing Spark sessions in tests.
"""

import os
import shutil
import tempfile
from typing import Optional

from pipeline_builder.compat import SparkSession


def create_test_spark_session(
    app_name: Optional[str] = None,
    master: str = "local[1]",
    warehouse_dir: Optional[str] = None,
    enable_delta: bool = True,
) -> SparkSession:
    """
    Create a Spark session configured for testing.

    Args:
        app_name: Application name (default: auto-generated)
        master: Spark master URL (default: "local[1]")
        warehouse_dir: Warehouse directory path (default: temporary directory)
        enable_delta: Whether to enable Delta Lake (default: True)

    Returns:
        Configured SparkSession
    """
    spark_mode = os.environ.get("SPARK_MODE", "mock").lower()

    if spark_mode == "mock":
        from sparkless import SparkSession as MockSparkSession  # type: ignore[import]

        if app_name is None:
            app_name = f"SparkForgeTest-{os.getpid()}"

        return MockSparkSession(app_name)
    else:
        # Real Spark session
        from pyspark.sql import SparkSession as PySparkSparkSession

        if app_name is None:
            app_name = f"SparkForgeTest-{os.getpid()}"

        if warehouse_dir is None:
            warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")

        builder = (
            PySparkSparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )

        if enable_delta:
            try:
                from delta import configure_spark_with_delta_pip

                builder = builder.config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                ).config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )

                spark = configure_spark_with_delta_pip(builder).getOrCreate()
            except ImportError:
                # Delta not available, use basic Spark
                spark = builder.getOrCreate()
        else:
            spark = builder.getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        return spark


def cleanup_spark_session(
    spark: SparkSession, warehouse_dir: Optional[str] = None
) -> None:
    """
    Clean up a Spark session and associated resources.

    Args:
        spark: SparkSession to clean up
        warehouse_dir: Warehouse directory to remove (if temporary)
    """
    try:
        if spark:
            spark.stop()
    except Exception:
        pass

    if warehouse_dir and os.path.exists(warehouse_dir):
        try:
            shutil.rmtree(warehouse_dir, ignore_errors=True)
        except Exception:
            pass


def create_test_database(spark: SparkSession, schema_name: str = "test_schema") -> None:
    """
    Create a test database/schema.

    Args:
        spark: SparkSession instance
        schema_name: Name of the schema to create
    """
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    except Exception:
        pass  # Ignore errors if database already exists


def drop_test_database(
    spark: SparkSession, schema_name: str = "test_schema", cascade: bool = True
) -> None:
    """
    Drop a test database/schema.

    Args:
        spark: SparkSession instance
        schema_name: Name of the schema to drop
        cascade: Whether to cascade drop (default: True)
    """
    try:
        cascade_clause = " CASCADE" if cascade else ""
        spark.sql(f"DROP DATABASE IF EXISTS {schema_name}{cascade_clause}")
    except Exception:
        pass  # Ignore errors if database doesn't exist
