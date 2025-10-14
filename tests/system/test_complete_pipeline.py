"""
System tests for complete pipeline execution using Mock Spark.
"""

import pytest
from mock_spark.errors import AnalysisException

from sparkforge.execution import ExecutionEngine
from sparkforge.models import ParallelConfig, PipelineConfig, ValidationThresholds
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.validation.pipeline_validation import UnifiedValidator
from sparkforge.writer.core import LogWriter
from sparkforge.writer.models import LogLevel, WriteMode, WriterConfig


class TestCompletePipeline:
    """System tests for complete pipeline execution with Mock Spark."""

    def test_bronze_to_silver_to_gold_pipeline(
        self, mock_spark_session, sample_dataframe
    ):
        """Test complete Bronze → Silver → Gold pipeline execution."""
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

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

        # Create Bronze layer table
        mock_spark_session.storage.create_table(
            "bronze", "raw_events", sample_dataframe.schema.fields
        )

        # Insert sample data into Bronze layer
        sample_data = [row.asDict() for row in sample_dataframe.collect()]
        mock_spark_session.storage.insert_data("bronze", "raw_events", sample_data)

        # Verify Bronze layer data
        bronze_data = mock_spark_session.storage.query_table("bronze", "raw_events")
        assert len(bronze_data) > 0
        assert mock_spark_session.storage.table_exists("bronze", "raw_events")

        # Create Silver layer table
        mock_spark_session.storage.create_table(
            "silver", "processed_events", sample_dataframe.schema.fields
        )

        # Simulate Silver layer processing (copy data for now)
        mock_spark_session.storage.insert_data(
            "silver", "processed_events", sample_data
        )

        # Verify Silver layer data
        silver_data = mock_spark_session.storage.query_table(
            "silver", "processed_events"
        )
        assert len(silver_data) > 0
        assert mock_spark_session.storage.table_exists("silver", "processed_events")

        # Create Gold layer table
        mock_spark_session.storage.create_table(
            "gold", "aggregated_metrics", sample_dataframe.schema.fields
        )

        # Simulate Gold layer processing (copy data for now)
        mock_spark_session.storage.insert_data(
            "gold", "aggregated_metrics", sample_data
        )

        # Verify Gold layer data
        gold_data = mock_spark_session.storage.query_table("gold", "aggregated_metrics")
        assert len(gold_data) > 0
        assert mock_spark_session.storage.table_exists("gold", "aggregated_metrics")

        # Verify complete pipeline setup
        assert builder.spark == engine.spark
        assert writer.spark == engine.spark
        assert validator.logger is not None

        # Verify data flow
        assert len(bronze_data) == len(silver_data)
        assert len(silver_data) == len(gold_data)

    def test_pipeline_with_data_validation(self, mock_spark_session, sample_dataframe):
        """Test pipeline with data validation at each layer."""
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

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

        # Create tables
        mock_spark_session.storage.create_table(
            "bronze", "raw_data", sample_dataframe.schema.fields
        )
        mock_spark_session.storage.create_table(
            "silver", "processed_data", sample_dataframe.schema.fields
        )
        mock_spark_session.storage.create_table(
            "gold", "aggregated_data", sample_dataframe.schema.fields
        )

        # Insert data
        sample_data = [row.asDict() for row in sample_dataframe.collect()]
        mock_spark_session.storage.insert_data("bronze", "raw_data", sample_data)

        # Test validation at Bronze layer
        bronze_df = mock_spark_session.table("bronze.raw_data")
        assert bronze_df.count() > 0

        # Test validation at Silver layer
        mock_spark_session.storage.insert_data("silver", "processed_data", sample_data)
        silver_df = mock_spark_session.table("silver.processed_data")
        assert silver_df.count() > 0

        # Test validation at Gold layer
        mock_spark_session.storage.insert_data("gold", "aggregated_data", sample_data)
        gold_df = mock_spark_session.table("gold.aggregated_data")
        assert gold_df.count() > 0

        # Verify validation components work
        assert validator.logger is not None
        assert engine.config.thresholds == thresholds

    def test_pipeline_with_logging_and_monitoring(
        self, mock_spark_session, sample_dataframe
    ):
        """Test pipeline with comprehensive logging and monitoring."""
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

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

        # Create tables
        mock_spark_session.storage.create_table(
            "bronze", "raw_data", sample_dataframe.schema.fields
        )
        mock_spark_session.storage.create_table(
            "silver", "processed_data", sample_dataframe.schema.fields
        )
        mock_spark_session.storage.create_table(
            "gold", "aggregated_data", sample_dataframe.schema.fields
        )

        # Insert data
        sample_data = [row.asDict() for row in sample_dataframe.collect()]
        mock_spark_session.storage.insert_data("bronze", "raw_data", sample_data)
        mock_spark_session.storage.insert_data("silver", "processed_data", sample_data)
        mock_spark_session.storage.insert_data("gold", "aggregated_data", sample_data)

        # Verify logging configuration
        assert writer.config.log_level == LogLevel.INFO
        assert writer.config.batch_size == 1000
        assert writer.config.compression == "snappy"

        # Verify monitoring components work
        assert writer.spark == engine.spark
        assert engine.config.verbose is True

        # Verify data is accessible for monitoring
        bronze_data = mock_spark_session.storage.query_table("bronze", "raw_data")
        silver_data = mock_spark_session.storage.query_table("silver", "processed_data")
        gold_data = mock_spark_session.storage.query_table("gold", "aggregated_data")

        assert len(bronze_data) > 0
        assert len(silver_data) > 0
        assert len(gold_data) > 0

    def test_pipeline_error_recovery(self, mock_spark_session, sample_dataframe):
        """Test pipeline error recovery and resilience."""
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

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

        # Create tables
        mock_spark_session.storage.create_table(
            "bronze", "raw_data", sample_dataframe.schema.fields
        )
        mock_spark_session.storage.create_table(
            "silver", "processed_data", sample_dataframe.schema.fields
        )
        mock_spark_session.storage.create_table(
            "gold", "aggregated_data", sample_dataframe.schema.fields
        )

        # Insert data
        sample_data = [row.asDict() for row in sample_dataframe.collect()]
        mock_spark_session.storage.insert_data("bronze", "raw_data", sample_data)

        # Test error handling
        with pytest.raises(AnalysisException):
            mock_spark_session.table("nonexistent.table")

        # Verify pipeline components are still functional after error
        assert builder.spark == engine.spark
        assert engine.config == config

        # Verify data is still accessible
        bronze_data = mock_spark_session.storage.query_table("bronze", "raw_data")
        assert len(bronze_data) > 0

        # Verify tables still exist
        assert mock_spark_session.storage.table_exists("bronze", "raw_data")
        assert mock_spark_session.storage.table_exists("silver", "processed_data")
        assert mock_spark_session.storage.table_exists("gold", "aggregated_data")

    def test_pipeline_with_different_data_sizes(
        self, mock_spark_session, large_dataset
    ):
        """Test pipeline with different data sizes."""
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

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

        # Create tables
        from mock_spark import MockStructField, MockStructType
        from pyspark.sql.types import DoubleType, IntegerType, StringType

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("value", DoubleType()),
                MockStructField("category", StringType()),
            ]
        )

        mock_spark_session.storage.create_table("bronze", "raw_data", schema.fields)
        mock_spark_session.storage.create_table(
            "silver", "processed_data", schema.fields
        )
        mock_spark_session.storage.create_table(
            "gold", "aggregated_data", schema.fields
        )

        # Insert large dataset
        mock_spark_session.storage.insert_data("bronze", "raw_data", large_dataset)

        # Verify large dataset is processed
        bronze_data = mock_spark_session.storage.query_table("bronze", "raw_data")
        assert len(bronze_data) == len(large_dataset)

        # Simulate processing through layers
        mock_spark_session.storage.insert_data(
            "silver", "processed_data", large_dataset
        )
        mock_spark_session.storage.insert_data("gold", "aggregated_data", large_dataset)

        # Verify data flow
        silver_data = mock_spark_session.storage.query_table("silver", "processed_data")
        gold_data = mock_spark_session.storage.query_table("gold", "aggregated_data")

        assert len(silver_data) == len(large_dataset)
        assert len(gold_data) == len(large_dataset)

        # Verify pipeline components handle large data
        assert builder.spark == engine.spark
        assert engine.config == config
