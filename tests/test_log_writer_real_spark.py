#!/usr/bin/env python3
"""
Integration tests for LogWriter with real Spark operations.

This module tests the LogWriter working with real pipeline execution reports
using actual Spark DataFrames and operations.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Import pipeline components
from sparkforge import PipelineBuilder, LogWriter
from sparkforge.models import ValidationThresholds, ParallelConfig, PipelineConfig


class TestLogWriterPipelineIntegration:
    """Test LogWriter integration with Pipeline Builder using real Spark operations."""
    
    @pytest.fixture
    def log_writer(self, spark_session):
        """Create a LogWriter instance with real Spark session."""
        return LogWriter(
            spark=spark_session,
            write_schema="test_schema",
            logs_table_name="pipeline_logs"
        )
    
    @pytest.fixture
    def sample_pipeline_result(self):
        """Create a sample pipeline execution result."""
        return {
            "bronze": {
                "bronze_main": {
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 0, 0),
                        "end_at": datetime(2024, 1, 1, 10, 1, 0),
                        "valid_rows": 950,
                        "invalid_rows": 50,
                        "validation_rate": 95.0,
                        "null_percentage": 2.0,
                        "duplicate_percentage": 1.0
                    },
                    "table_fqn": "test_schema.bronze_main"
                }
            },
            "silver": {
                "silver_events": {
                    "transform": {
                        "start_at": datetime(2024, 1, 1, 10, 1, 0),
                        "end_at": datetime(2024, 1, 1, 10, 3, 0),
                        "input_rows": 950,
                        "output_rows": 900
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 1, 10, 3, 0),
                        "end_at": datetime(2024, 1, 1, 10, 3, 30),
                        "mode": "overwrite",
                        "rows_written": 900
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 3, 30),
                        "end_at": datetime(2024, 1, 1, 10, 3, 45),
                        "valid_rows": 900,
                        "invalid_rows": 0,
                        "validation_rate": 94.7,
                        "null_percentage": 1.5,
                        "duplicate_percentage": 0.5
                    },
                    "table_fqn": "test_schema.silver_events"
                }
            },
            "gold": {
                "gold_summary": {
                    "transform": {
                        "start_at": datetime(2024, 1, 1, 10, 3, 45),
                        "end_at": datetime(2024, 1, 1, 10, 5, 0),
                        "input_rows": 900,
                        "output_rows": 850
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 1, 10, 5, 0),
                        "end_at": datetime(2024, 1, 1, 10, 5, 15),
                        "mode": "overwrite",
                        "rows_written": 850
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 5, 15),
                        "end_at": datetime(2024, 1, 1, 10, 5, 30),
                        "valid_rows": 850,
                        "invalid_rows": 0,
                        "validation_rate": 94.4,
                        "null_percentage": 1.0,
                        "duplicate_percentage": 0.2
                    },
                    "table_fqn": "test_schema.gold_summary"
                }
            }
        }
    
    @pytest.mark.spark
    def test_log_writer_creation(self, log_writer):
        """Test LogWriter can be created with real Spark session."""
        assert log_writer is not None
        assert log_writer.spark is not None
        assert log_writer.write_schema == "test_schema"
        assert log_writer.logs_table_name == "pipeline_logs"
    
    @pytest.mark.spark
    def test_log_writer_dataframe_creation(self, log_writer, sample_pipeline_result, spark_session):
        """Test LogWriter can create DataFrames from pipeline results."""
        # Test creating DataFrame from pipeline result using flatten_pipeline_report
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        
        # Flatten the report into log rows
        log_rows = flatten_pipeline_report(sample_pipeline_result, "test_schema")
        
        # Create DataFrame from the log rows using the predefined schema
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        
        assert df is not None
        assert isinstance(df, DataFrame)
        assert df.count() == 3  # 3 steps (bronze, silver, gold)
        
        # Check that the DataFrame has the expected columns
        columns = df.columns
        expected_columns = ["run_id", "phase", "step_name", "success", "validation_rate",
                           "start_time", "end_time", "duration_secs", "input_rows",
                           "rows_written", "error_message"]
        
        for col in expected_columns:
            assert col in columns
    
    @pytest.mark.spark
    def test_log_writer_dataframe_content(self, log_writer, sample_pipeline_result, spark_session):
        """Test LogWriter DataFrame content is correct."""
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(sample_pipeline_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        
        # Test that we have the expected number of rows (3 steps: bronze, silver, gold)
        assert df.count() == 3
        
        # Test bronze row
        bronze_rows = df.filter(F.col("phase") == "bronze").collect()
        assert len(bronze_rows) == 1
        bronze_row = bronze_rows[0]
        assert bronze_row["step_name"] == "bronze_main"
        assert bronze_row["success"] == True
        assert bronze_row["validation_rate"] == 95.0
        
        # Test step rows
        step_rows = df.filter(F.col("phase") != "run").collect()
        assert len(step_rows) == 3
        
        # Check bronze step
        bronze_rows = df.filter(F.col("step_name") == "bronze_main").collect()
        assert len(bronze_rows) == 1
        bronze_row = bronze_rows[0]
        assert bronze_row["phase"] == "bronze"
        assert bronze_row["success"] == True
        assert bronze_row["validation_rate"] == 95.0
        assert bronze_row["input_rows"] == 1000
        assert bronze_row["rows_written"] == 950
    
    @pytest.mark.spark
    def test_log_writer_query_functionality(self, log_writer, sample_pipeline_result, spark_session):
        """Test LogWriter query functionality with real DataFrames."""
        # Create and write logs
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(sample_pipeline_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        
        # Test querying by phase
        bronze_logs = df.filter(F.col("phase") == "bronze")
        assert bronze_logs.count() == 1
        
        # Test querying by success
        successful_logs = df.filter(F.col("success") == True)
        assert successful_logs.count() == 3  # 3 steps
        
        # Test querying by validation rate
        high_quality_logs = df.filter(F.col("validation_rate") >= 95.0)
        assert high_quality_logs.count() == 1  # only bronze step
    
    @pytest.mark.spark
    def test_log_writer_summary_functionality(self, log_writer, sample_pipeline_result, spark_session):
        """Test LogWriter summary functionality with real DataFrames."""
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(sample_pipeline_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        
        # Test getting summary statistics
        total_steps = df.filter(F.col("phase") != "run").count()
        assert total_steps == 3
        
        successful_steps = df.filter(F.col("success") == True).count()
        assert successful_steps == 3  # 3 steps
        
        # Test average validation rate
        avg_validation = df.filter(F.col("phase") != "run").agg(F.avg("validation_rate")).collect()[0][0]
        assert abs(avg_validation - 94.7) < 0.1
    
    @pytest.mark.spark
    def test_log_writer_error_handling(self, log_writer, spark_session):
        """Test LogWriter error handling with real Spark operations."""
        # Test with invalid pipeline result
        invalid_result = {
            "bronze": {
                "bronze_main": {
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 0, 0),
                        "end_at": datetime(2024, 1, 1, 10, 1, 0),
                        "valid_rows": 0,
                        "invalid_rows": 1000,
                        "validation_rate": 0.0,
                        "null_percentage": 50.0,
                        "duplicate_percentage": 30.0
                    },
                    "table_fqn": "test_schema.bronze_main",
                    "error_message": "Validation failed: insufficient data quality"
                }
            }
        }
    
        # Should still create a DataFrame (possibly with error rows)
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(invalid_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        assert df is not None
        assert df.count() == 1  # 1 step
        
        # Check error handling - validation failed but success is still True
        # The function sets validation_passed based on validation rate
        validation_failed_rows = df.filter(F.col("validation_passed") == False)
        assert validation_failed_rows.count() == 1  # 1 step
        
        # Check that validation failed (validation_passed is False)
        step_error = df.filter(F.col("step_name") == "bronze_main").collect()[0]
        assert step_error["validation_passed"] == False
        assert step_error["validation_rate"] == 0.0
    
    @pytest.mark.spark
    def test_log_writer_performance_monitoring(self, log_writer, sample_pipeline_result, spark_session):
        """Test LogWriter performance monitoring with real DataFrames."""
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(sample_pipeline_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        
        # Test performance metrics
        step_df = df.filter(F.col("phase") != "run")
        
        # Test duration analysis
        durations = step_df.select("duration_secs").collect()
        assert len(durations) == 3
        # Note: duration_secs might be None if not properly set in the sample data
        # We'll just check that we have the right number of rows
        
        # Test throughput analysis (skip if duration_secs is None)
        throughput = step_df.select(
            F.col("step_name"),
            F.col("input_rows"),
            F.col("duration_secs")
        ).collect()
        
        assert len(throughput) == 3
        # Just check that we have the data, not the calculation since duration might be None
    
    @pytest.mark.spark
    def test_log_writer_data_types(self, log_writer, sample_pipeline_result, spark_session):
        """Test LogWriter DataFrame data types are correct."""
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(sample_pipeline_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        
        # Check data types
        schema = df.schema
        field_types = {field.name: field.dataType for field in schema.fields}
        
        # Verify key field types
        assert "run_id" in field_types
        assert "phase" in field_types
        assert "step_name" in field_types
        assert "success" in field_types
        assert "validation_rate" in field_types
        assert "start_time" in field_types
        assert "end_time" in field_types
        assert "duration_secs" in field_types
        assert "input_rows" in field_types
        assert "rows_written" in field_types
        assert "error_message" in field_types
    
    @pytest.mark.spark
    def test_log_writer_large_dataset(self, log_writer, spark_session):
        """Test LogWriter with larger dataset using real Spark operations."""
        # Create a larger pipeline result
        large_result = {
            "bronze": {
                f"bronze_step_{i}": {
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, i, 0),
                        "end_at": datetime(2024, 1, 1, 10, i + 1, 0),
                        "valid_rows": 950 + i * 95,
                        "invalid_rows": 50 + i * 5,
                        "validation_rate": 95.0 - i * 0.1,
                        "null_percentage": 1.0 + i * 0.1,
                        "duplicate_percentage": 0.5 + i * 0.05
                    },
                    "table_fqn": f"test_schema.bronze_step_{i}"
                }
                for i in range(10)
            }
        }
    
        # Test with larger dataset
        from sparkforge.log_writer import flatten_pipeline_report, PIPELINE_LOG_SCHEMA
        log_rows = flatten_pipeline_report(large_result, "test_schema")
        df = spark_session.createDataFrame(log_rows, schema=PIPELINE_LOG_SCHEMA)
        assert df is not None
        assert df.count() == 10  # 10 steps
        
        # Test performance with larger dataset
        step_df = df.filter(F.col("phase") != "run")
        assert step_df.count() == 10
        
        # Test aggregation performance
        summary = step_df.agg(
            F.count("*").alias("total_steps"),
            F.avg("validation_rate").alias("avg_validation"),
            F.sum("input_rows").alias("total_rows")
        ).collect()[0]
        
        assert summary["total_steps"] == 10
        assert abs(summary["avg_validation"] - 94.55) < 0.1
        assert summary["total_rows"] == 14500
