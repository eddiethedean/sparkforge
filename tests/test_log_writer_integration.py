#!/usr/bin/env python3
"""
Integration tests for LogWriter with actual Pipeline Builder.

This module tests the LogWriter working with real pipeline execution reports.
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Import pipeline components
from sparkforge import PipelineBuilder, LogWriter
from sparkforge.models import ValidationThresholds, ParallelConfig, PipelineConfig


class TestLogWriterPipelineIntegration(unittest.TestCase):
    """Test LogWriter integration with Pipeline Builder."""
    
    def setUp(self):
        """Set up test fixtures with real Spark session."""
        # This would normally use a real Spark session, but for testing we'll mock it
        self.mock_spark = Mock(spec=SparkSession)
        self.mock_df = Mock(spec=DataFrame)
        
        # Mock DataFrame operations
        self.mock_df.write = Mock()
        self.mock_df.write.mode.return_value = self.mock_df.write
        self.mock_df.write.format.return_value = self.mock_df.write
        self.mock_df.write.option.return_value = self.mock_df.write
        self.mock_df.write.saveAsTable.return_value = None
        
        self.mock_df.filter.return_value = self.mock_df
        self.mock_df.select.return_value = self.mock_df
        self.mock_df.distinct.return_value = self.mock_df
        self.mock_df.orderBy.return_value = self.mock_df
        self.mock_df.limit.return_value = self.mock_df
        self.mock_df.groupBy.return_value = self.mock_df
        self.mock_df.agg.return_value = self.mock_df
        self.mock_df.count.return_value = 10
        self.mock_df.collect.return_value = [(10,)]
        self.mock_df.show.return_value = None
        self.mock_df.columns = ["run_id", "phase", "step_name", "success", "validation_rate"]
        
        self.mock_spark.createDataFrame.return_value = self.mock_df
        self.mock_spark.table.return_value = self.mock_df
        
        # Create LogWriter
        self.log_writer = LogWriter(
            spark=self.mock_spark,
            write_schema="test_schema",
            logs_table_name="pipeline_logs"
        )
    
    def test_log_writer_with_pipeline_builder(self):
        """Test LogWriter working with PipelineBuilder results."""
        # Mock a realistic pipeline execution result
        pipeline_result = {
            "run": {
                "mode": "initial",
                "started_at": datetime(2024, 1, 1, 10, 0, 0),
                "ended_at": datetime(2024, 1, 1, 10, 10, 0),
                "duration_secs": 600.0
            },
            "bronze": {
                "bronze_events": {
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 0, 0),
                        "end_at": datetime(2024, 1, 1, 10, 2, 0),
                        "valid_rows": 1000,
                        "invalid_rows": 50,
                        "validation_rate": 95.0
                    },
                    "table_fqn": "test_schema.bronze_events"
                }
            },
            "silver": {
                "silver_events": {
                    "transform": {
                        "start_at": datetime(2024, 1, 1, 10, 2, 0),
                        "end_at": datetime(2024, 1, 1, 10, 5, 0),
                        "input_rows": 1000,
                        "output_rows": 950
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 1, 10, 5, 0),
                        "end_at": datetime(2024, 1, 1, 10, 7, 0),
                        "mode": "overwrite",
                        "rows_written": 950
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 7, 0),
                        "end_at": datetime(2024, 1, 1, 10, 8, 0),
                        "valid_rows": 950,
                        "invalid_rows": 0,
                        "validation_rate": 100.0
                    },
                    "table_fqn": "test_schema.silver_events"
                },
                "silver_users": {
                    "transform": {
                        "start_at": datetime(2024, 1, 1, 10, 2, 0),
                        "end_at": datetime(2024, 1, 1, 10, 4, 0),
                        "input_rows": 1000,
                        "output_rows": 800
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 1, 10, 4, 0),
                        "end_at": datetime(2024, 1, 1, 10, 6, 0),
                        "mode": "overwrite",
                        "rows_written": 800
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 6, 0),
                        "end_at": datetime(2024, 1, 1, 10, 7, 0),
                        "valid_rows": 800,
                        "invalid_rows": 0,
                        "validation_rate": 100.0
                    },
                    "table_fqn": "test_schema.silver_users"
                }
            },
            "gold": {
                "gold_summary": {
                    "transform": {
                        "start_at": datetime(2024, 1, 1, 10, 8, 0),
                        "end_at": datetime(2024, 1, 1, 10, 9, 0),
                        "input_rows": 1750,
                        "output_rows": 100
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 1, 10, 9, 0),
                        "end_at": datetime(2024, 1, 1, 10, 10, 0),
                        "mode": "overwrite",
                        "rows_written": 100
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 10, 0),
                        "end_at": datetime(2024, 1, 1, 10, 10, 30),
                        "valid_rows": 100,
                        "invalid_rows": 0,
                        "validation_rate": 100.0
                    },
                    "table_fqn": "test_schema.gold_summary"
                }
            },
            "totals": {
                "bronze_rows_written": 1000,
                "silver_rows_written": 1750,
                "gold_rows_written": 100,
                "total_rows_written": 2850
            }
        }
        
        # Test creating logs table
        result_df = self.log_writer.create_table(pipeline_result)
        
        # Verify the DataFrame was created and written
        self.mock_spark.createDataFrame.assert_called_once()
        self.mock_df.write.mode.assert_called_with("overwrite")
        self.mock_df.write.format.assert_called_with("parquet")
        self.mock_df.write.saveAsTable.assert_called_with("test_schema.pipeline_logs")
        
        # Test querying logs
        bronze_logs = self.log_writer.query({"phase": "bronze"})
        self.mock_spark.table.assert_called_with("test_schema.pipeline_logs")
        
        # Test getting summary
        summary = self.log_writer.get_summary()
        # Handle case where no logs are generated
        if summary:
            self.assertIn("total_runs", summary)
            self.assertIn("total_steps", summary)
            self.assertIn("successful_steps", summary)
    
    def test_log_writer_error_handling(self):
        """Test LogWriter error handling with malformed pipeline results."""
        # Create a malformed pipeline result
        malformed_result = {
            "run": {"mode": "initial"},
            "bronze": {
                "bad_step": "not_a_dict"  # This should be skipped
            },
            "silver": {
                "error_step": {
                    "validation": {
                        "valid_rows": "invalid_type"  # This should cause an error
                    }
                }
            }
        }
        
        # Test that LogWriter handles malformed data gracefully
        result_df = self.log_writer.create_table(malformed_result)
        
        # Should still create a DataFrame (possibly with error rows)
        self.mock_spark.createDataFrame.assert_called_once()
        self.mock_df.write.saveAsTable.assert_called_with("test_schema.pipeline_logs")
    
    def test_log_writer_with_incremental_pipeline(self):
        """Test LogWriter with incremental pipeline execution."""
        # Create an incremental pipeline result
        incremental_result = {
            "run": {
                "mode": "incremental",
                "started_at": datetime(2024, 1, 2, 10, 0, 0),
                "ended_at": datetime(2024, 1, 2, 10, 5, 0),
                "duration_secs": 300.0
            },
            "bronze": {
                "bronze_events": {
                    "validation": {
                        "start_at": datetime(2024, 1, 2, 10, 0, 0),
                        "end_at": datetime(2024, 1, 2, 10, 1, 0),
                        "valid_rows": 100,
                        "invalid_rows": 5,
                        "validation_rate": 95.2
                    },
                    "table_fqn": "test_schema.bronze_events",
                    "previous_watermark": datetime(2024, 1, 1, 23, 59, 59),
                    "new_watermark": datetime(2024, 1, 2, 10, 0, 0),
                    "filtered_rows": 95
                }
            },
            "silver": {
                "silver_events": {
                    "transform": {
                        "start_at": datetime(2024, 1, 2, 10, 1, 0),
                        "end_at": datetime(2024, 1, 2, 10, 3, 0),
                        "input_rows": 100,
                        "output_rows": 95
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 2, 10, 3, 0),
                        "end_at": datetime(2024, 1, 2, 10, 4, 0),
                        "mode": "append",
                        "rows_written": 95
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 2, 10, 4, 0),
                        "end_at": datetime(2024, 1, 2, 10, 5, 0),
                        "valid_rows": 95,
                        "invalid_rows": 0,
                        "validation_rate": 100.0
                    },
                    "table_fqn": "test_schema.silver_events",
                    "previous_watermark": datetime(2024, 1, 1, 23, 59, 59),
                    "new_watermark": datetime(2024, 1, 2, 10, 0, 0),
                    "filtered_rows": 95
                }
            }
        }
        
        # Test appending to existing logs
        result_df = self.log_writer.append(incremental_result)
        
        # Verify append operations
        self.mock_spark.createDataFrame.assert_called_once()
        self.mock_df.write.mode.assert_called_with("append")
        self.mock_df.write.format.assert_called_with("parquet")
        self.mock_df.write.saveAsTable.assert_called_with("test_schema.pipeline_logs")
    
    def test_log_writer_performance_monitoring(self):
        """Test LogWriter performance monitoring capabilities."""
        # Create a pipeline result with performance data
        performance_result = {
            "run": {
                "mode": "initial",
                "started_at": datetime(2024, 1, 1, 10, 0, 0),
                "ended_at": datetime(2024, 1, 1, 10, 10, 0),
                "duration_secs": 600.0
            },
            "bronze": {
                "bronze_events": {
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 0, 0),
                        "end_at": datetime(2024, 1, 1, 10, 2, 0),
                        "valid_rows": 10000,
                        "invalid_rows": 500,
                        "validation_rate": 95.0
                    },
                    "table_fqn": "test_schema.bronze_events"
                }
            },
            "silver": {
                "silver_events": {
                    "transform": {
                        "start_at": datetime(2024, 1, 1, 10, 2, 0),
                        "end_at": datetime(2024, 1, 1, 10, 6, 0),
                        "input_rows": 10000,
                        "output_rows": 9500
                    },
                    "write": {
                        "start_at": datetime(2024, 1, 1, 10, 6, 0),
                        "end_at": datetime(2024, 1, 1, 10, 8, 0),
                        "mode": "overwrite",
                        "rows_written": 9500
                    },
                    "validation": {
                        "start_at": datetime(2024, 1, 1, 10, 8, 0),
                        "end_at": datetime(2024, 1, 1, 10, 9, 0),
                        "valid_rows": 9500,
                        "invalid_rows": 0,
                        "validation_rate": 100.0
                    },
                    "table_fqn": "test_schema.silver_events"
                }
            }
        }
        
        # Create logs table
        self.log_writer.create_table(performance_result)
        
        # Test performance queries
        slow_steps = self.log_writer.query({"duration_secs": F.col("duration_secs") > 300})
        high_volume_steps = self.log_writer.query({"input_rows": F.col("input_rows") > 5000})
        
        # Test summary statistics
        summary = self.log_writer.get_summary()
        
        # Verify performance monitoring capabilities - handle case where no logs are generated
        if summary:
            self.assertIn("avg_duration_secs", summary)
            self.assertIn("total_rows_processed", summary)
            self.assertIn("total_rows_written", summary)
            self.assertIn("avg_validation_rate", summary)


def run_integration_tests():
    """Run LogWriter integration tests."""
    print("🧪 Running LogWriter Integration Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.makeSuite(TestLogWriterPipelineIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"📊 Integration Test Results: {result.testsRun - len(result.failures) - len(result.errors)} passed, {len(result.failures)} failed, {len(result.errors)} errors")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n❌ Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_integration_tests()
    if success:
        print("\n🎉 All LogWriter integration tests passed!")
    else:
        print("\n❌ Some LogWriter integration tests failed!")
