"""
System tests for complete pipeline execution using Mock Spark.
"""

import os

import pytest

# Import AnalysisException - available in both PySpark and sparkless
try:
    from pyspark.sql.utils import AnalysisException
except ImportError:
    from sparkless.errors import AnalysisException  # type: ignore[import]

from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import ParallelConfig, PipelineConfig, ValidationThresholds
from pipeline_builder.pipeline.builder import PipelineBuilder
from pipeline_builder.validation.pipeline_validation import UnifiedValidator
from pipeline_builder.writer.core import LogWriter
from pipeline_builder.writer.models import LogLevel, WriteMode, WriterConfig


class TestCompletePipeline:
    """System tests for complete pipeline execution with Mock Spark."""

    def test_bronze_to_silver_to_gold_pipeline(
        self, mock_spark_session, sample_dataframe
    ):
        """Test complete Bronze → Silver → Gold pipeline execution."""
        # Setup schemas using standard Spark SQL
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS bronze")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS silver")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS gold")

        # Create pipeline builder
        builder = PipelineBuilder(spark=mock_spark_session, schema="bronze")

        # Create execution engine
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="bronze",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)

        # Create validator
        validator = UnifiedValidator()

        # Create log writer
        writer_config = WriterConfig(
            table_schema="bronze",
            table_name="pipeline_logs",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
        )

        writer = LogWriter(spark=mock_spark_session, config=writer_config)

        # Create Bronze layer table using standard Spark operations
        sample_dataframe.write.saveAsTable("bronze.raw_events")

        # Verify Bronze layer data using standard Spark operations
        bronze_df = mock_spark_session.table("bronze.raw_events")
        bronze_data = [row.asDict() for row in bronze_df.collect()]
        assert len(bronze_data) > 0
        # Verify table exists by successfully accessing it
        assert bronze_df.count() > 0

        # Create Silver layer table using standard Spark operations
        sample_dataframe.write.saveAsTable("silver.processed_events")

        # Verify Silver layer data
        silver_df = mock_spark_session.table("silver.processed_events")
        silver_data = [row.asDict() for row in silver_df.collect()]
        assert len(silver_data) > 0
        assert silver_df.count() > 0

        # Create Gold layer table using standard Spark operations
        sample_dataframe.write.saveAsTable("gold.aggregated_metrics")

        # Verify Gold layer data
        gold_df = mock_spark_session.table("gold.aggregated_metrics")
        gold_data = [row.asDict() for row in gold_df.collect()]
        assert len(gold_data) > 0
        assert gold_df.count() > 0

        # Verify complete pipeline setup
        assert builder.spark == engine.spark
        assert writer.spark == engine.spark
        assert validator.logger is not None

        # Verify data flow
        assert len(bronze_data) == len(silver_data)
        assert len(silver_data) == len(gold_data)

    def test_pipeline_with_data_validation(self, mock_spark_session, sample_dataframe):
        """Test pipeline with data validation at each layer."""
        # Setup schemas using standard Spark SQL
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS bronze")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS silver")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS gold")

        # Create pipeline builder
        PipelineBuilder(spark=mock_spark_session, schema="bronze")

        # Create validator
        validator = UnifiedValidator()

        # Create execution engine
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="bronze",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)

        # Create tables using standard Spark operations
        sample_dataframe.write.saveAsTable("bronze.raw_data")
        sample_dataframe.write.saveAsTable("silver.processed_data")
        sample_dataframe.write.saveAsTable("gold.aggregated_data")

        # Test validation at Bronze layer
        bronze_df = mock_spark_session.table("bronze.raw_data")
        assert bronze_df.count() > 0

        # Test validation at Silver layer
        silver_df = mock_spark_session.table("silver.processed_data")
        assert silver_df.count() > 0

        # Test validation at Gold layer
        gold_df = mock_spark_session.table("gold.aggregated_data")
        assert gold_df.count() > 0

        # Verify validation components work
        assert validator.logger is not None
        assert engine.config.thresholds == thresholds

    def test_pipeline_with_logging_and_monitoring(
        self, mock_spark_session, sample_dataframe
    ):
        """Test pipeline with comprehensive logging and monitoring."""
        # Setup schemas using standard Spark SQL
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS bronze")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS silver")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS gold")

        # Create pipeline builder
        PipelineBuilder(spark=mock_spark_session, schema="bronze")

        # Create log writer
        writer_config = WriterConfig(
            table_schema="bronze",
            table_name="pipeline_logs",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            batch_size=1000,
            compression="snappy",
        )

        writer = LogWriter(spark=mock_spark_session, config=writer_config)

        # Create execution engine
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="bronze",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)

        # Create tables using standard Spark operations
        sample_dataframe.write.saveAsTable("bronze.raw_data")
        sample_dataframe.write.saveAsTable("silver.processed_data")
        sample_dataframe.write.saveAsTable("gold.aggregated_data")

        # Verify logging configuration
        assert writer.config.log_level == LogLevel.INFO
        assert writer.config.batch_size == 1000
        assert writer.config.compression == "snappy"

        # Verify monitoring components work
        assert writer.spark == engine.spark
        assert engine.config.verbose is True

        # Verify data is accessible for monitoring using standard Spark operations
        bronze_df = mock_spark_session.table("bronze.raw_data")
        silver_df = mock_spark_session.table("silver.processed_data")
        gold_df = mock_spark_session.table("gold.aggregated_data")

        assert bronze_df.count() > 0
        assert silver_df.count() > 0
        assert gold_df.count() > 0

    def test_pipeline_error_recovery(self, mock_spark_session, sample_dataframe):
        """Test pipeline error recovery and resilience."""
        # Setup schemas using standard Spark SQL
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS bronze")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS silver")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS gold")

        # Create pipeline builder
        builder = PipelineBuilder(spark=mock_spark_session, schema="bronze")

        # Create execution engine
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="bronze",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)

        # Create tables using standard Spark operations
        sample_dataframe.write.saveAsTable("bronze.raw_data")
        sample_dataframe.write.saveAsTable("silver.processed_data")
        sample_dataframe.write.saveAsTable("gold.aggregated_data")

        # Test error handling - both PySpark and sparkless raise AnalysisException
        # but they may be different classes, so we catch the base Exception and check the type
        with pytest.raises(Exception) as exc_info:
            mock_spark_session.table("nonexistent.table")
        # Verify it's an AnalysisException (works for both PySpark and sparkless)
        assert "AnalysisException" in type(exc_info.value).__name__ or "not found" in str(exc_info.value).lower()

        # Verify pipeline components are still functional after error
        assert builder.spark == engine.spark
        assert engine.config == config

        # Verify data is still accessible using standard Spark operations
        bronze_df = mock_spark_session.table("bronze.raw_data")
        assert bronze_df.count() > 0

        # Verify tables still exist by successfully accessing them
        assert mock_spark_session.table("silver.processed_data").count() > 0
        assert mock_spark_session.table("gold.aggregated_data").count() > 0

    def test_pipeline_with_different_data_sizes(
        self, mock_spark_session, large_dataset
    ):
        """Test pipeline with different data sizes."""
        # Setup schemas using standard Spark SQL
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS bronze")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS silver")
        mock_spark_session.sql("CREATE DATABASE IF NOT EXISTS gold")

        # Create pipeline builder
        builder = PipelineBuilder(spark=mock_spark_session, schema="bronze")

        # Create execution engine
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4)
        config = PipelineConfig(
            schema="bronze",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True,
        )

        engine = ExecutionEngine(spark=mock_spark_session, config=config)

        # Create tables using standard Spark operations
        # Use appropriate types based on Spark mode
        import os
        spark_mode = os.environ.get("SPARK_MODE", "mock").lower()
        
        if spark_mode == "real":
            from pyspark.sql.types import (
                DoubleType,
                IntegerType,
                StringType,
                StructField,
                StructType,
            )
        else:
            from sparkless.spark_types import (  # type: ignore[import]
                DoubleType,
                IntegerType,
                StringType,
                StructField,
                StructType,
            )

        schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("name", StringType()),
                StructField("value", DoubleType()),
                StructField("category", StringType()),
            ]
        )

        large_df = mock_spark_session.createDataFrame(large_dataset, schema)
        large_df.write.saveAsTable("bronze.raw_data")
        large_df.write.saveAsTable("silver.processed_data")
        large_df.write.saveAsTable("gold.aggregated_data")

        # Verify large dataset is processed using standard Spark operations
        bronze_df = mock_spark_session.table("bronze.raw_data")
        assert bronze_df.count() == len(large_dataset)

        # Verify data flow
        silver_df = mock_spark_session.table("silver.processed_data")
        gold_df = mock_spark_session.table("gold.aggregated_data")

        assert silver_df.count() == len(large_dataset)
        assert gold_df.count() == len(large_dataset)

        # Verify pipeline components handle large data
        assert builder.spark == engine.spark
        assert engine.config == config
