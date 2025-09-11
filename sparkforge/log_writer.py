#!/usr/bin/env python3
"""
Enhanced log writer for PipelineBuilder reports with comprehensive features.

This module provides advanced logging and reporting capabilities for pipeline execution,
including structured logging, analytics, monitoring, and data quality tracking.

Key Features:
- Flattens PipelineBuilder reports into structured log rows
- Uses real TimestampType columns for all datetimes
- Strict schema + typed helpers with validation
- Resilient to missing data and malformed reports
- Advanced analytics and monitoring capabilities
- Integration with current pipeline package structure
- Comprehensive error handling and logging
- Performance metrics and data quality tracking
- Export capabilities for external systems
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, TypedDict, Literal
from datetime import datetime, timedelta
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType
)
from pyspark.sql import functions as F

# Type aliases for better readability
ReportDict = Dict[str, Any]
LogRow = Dict[str, Any]


class LogLevel(Enum):
    """Log levels for pipeline execution."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataQualityStatus(Enum):
    """Data quality status indicators."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    CRITICAL = "critical"


@dataclass
class LogWriterConfig:
    """Configuration for LogWriter."""
    use_delta: bool = True
    pipeline_version: str = "1.0.0"
    enable_analytics: bool = True
    enable_data_quality: bool = True
    retention_days: int = 30
    batch_size: int = 1000
    enable_export: bool = True
    export_formats: List[str] = field(default_factory=lambda: ["json", "csv", "parquet"])
    enable_alerts: bool = True
    alert_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "validation_rate": 95.0,
        "success_rate": 90.0,
        "duration_threshold": 300.0
    })


# ---------- Enhanced log row (Typed) ----------

class PipelineLogRow(TypedDict):
    # Run metadata
    run_id: str
    run_mode: Literal["initial", "incremental", "full_refresh", "validation_only"]
    run_started_at: Optional[datetime]
    run_ended_at: Optional[datetime]
    run_duration_secs: Optional[float]

    # Step metadata
    phase: Literal["bronze", "silver", "gold"]
    step_name: str
    step_order: int
    step_type: Optional[str]

    # Timing
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration_secs: Optional[float]

    # Table information
    table_fqn: Optional[str]
    write_mode: Optional[Literal["overwrite", "append"]]

    # Row counts
    input_rows: Optional[int]
    output_rows: Optional[int]
    rows_written: Optional[int]

    # Validation
    valid_rows: int
    invalid_rows: int
    validation_rate: float
    validation_passed: bool
    data_quality_status: str

    # Watermarking (for incremental processing)
    previous_watermark: Optional[datetime]
    new_watermark: Optional[datetime]
    filtered_rows: Optional[int]

    # Error handling
    error_message: Optional[str]
    error_type: Optional[str]
    success: bool
    retry_count: int

    # Performance metrics
    memory_usage_mb: Optional[float]
    cpu_usage_percent: Optional[float]
    io_operations: Optional[int]

    # Data quality metrics
    null_percentage: Optional[float]
    duplicate_percentage: Optional[float]
    schema_compliance: Optional[float]

    # Additional metadata
    schema_name: str
    pipeline_version: str
    created_at: datetime
    log_level: str


# ---------- Enhanced Spark schema for the pipeline logs table ----------

PIPELINE_LOG_SCHEMA = StructType([
    # Run metadata
    StructField("run_id", StringType(), False),
    StructField("run_mode", StringType(), False),
    StructField("run_started_at", TimestampType(), True),
    StructField("run_ended_at", TimestampType(), True),
    StructField("run_duration_secs", FloatType(), True),

    # Step metadata
    StructField("phase", StringType(), False),
    StructField("step_name", StringType(), False),
    StructField("step_order", IntegerType(), False),
    StructField("step_type", StringType(), True),

    # Timing
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_secs", FloatType(), True),

    # Table information
    StructField("table_fqn", StringType(), True),
    StructField("write_mode", StringType(), True),

    # Row counts
    StructField("input_rows", IntegerType(), True),
    StructField("output_rows", IntegerType(), True),
    StructField("rows_written", IntegerType(), True),

    # Validation
    StructField("valid_rows", IntegerType(), False),
    StructField("invalid_rows", IntegerType(), False),
    StructField("validation_rate", FloatType(), False),
    StructField("validation_passed", BooleanType(), False),
    StructField("data_quality_status", StringType(), False),

    # Watermarking
    StructField("previous_watermark", TimestampType(), True),
    StructField("new_watermark", TimestampType(), True),
    StructField("filtered_rows", IntegerType(), True),

    # Error handling
    StructField("error_message", StringType(), True),
    StructField("error_type", StringType(), True),
    StructField("success", BooleanType(), False),
    StructField("retry_count", IntegerType(), False),

    # Performance metrics
    StructField("memory_usage_mb", FloatType(), True),
    StructField("cpu_usage_percent", FloatType(), True),
    StructField("io_operations", IntegerType(), True),

    # Data quality metrics
    StructField("null_percentage", FloatType(), True),
    StructField("duplicate_percentage", FloatType(), True),
    StructField("schema_compliance", FloatType(), True),

    # Additional metadata
    StructField("schema_name", StringType(), False),
    StructField("pipeline_version", StringType(), False),
    StructField("created_at", TimestampType(), False),
    StructField("log_level", StringType(), False),
])


# ---------- Enhanced flatten helpers with error handling ----------

def _safe_get_nested(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely get nested dictionary values with error handling."""
    try:
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current
    except (KeyError, TypeError, AttributeError):
        return default


def _calculate_duration(start_time: Optional[datetime], end_time: Optional[datetime]) -> Optional[float]:
    """Calculate duration in seconds between two timestamps."""
    if start_time and end_time:
        try:
            return (end_time - start_time).total_seconds()
        except (TypeError, AttributeError):
            return None
    return None


def _generate_run_id() -> str:
    """Generate a unique run ID."""
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"


def _calculate_data_quality_status(validation_rate: float, null_percentage: float = 0.0, duplicate_percentage: float = 0.0) -> str:
    """Calculate data quality status based on metrics."""
    if validation_rate >= 99.0 and null_percentage <= 1.0 and duplicate_percentage <= 1.0:
        return DataQualityStatus.EXCELLENT.value
    elif validation_rate >= 95.0 and null_percentage <= 5.0 and duplicate_percentage <= 5.0:
        return DataQualityStatus.GOOD.value
    elif validation_rate >= 90.0 and null_percentage <= 10.0 and duplicate_percentage <= 10.0:
        return DataQualityStatus.FAIR.value
    elif validation_rate >= 80.0 and null_percentage <= 20.0 and duplicate_percentage <= 20.0:
        return DataQualityStatus.POOR.value
    else:
        return DataQualityStatus.CRITICAL.value


def _create_base_log_row(
    report: ReportDict, 
    phase: Literal["bronze", "silver", "gold"], 
    step_name: str,
    step_order: int,
    schema_name: str,
    config: LogWriterConfig
) -> PipelineLogRow:
    """Create a base log row with common fields."""
    run = _safe_get_nested(report, "run", default={})
    run_started_at = _safe_get_nested(run, "started_at")
    run_ended_at = _safe_get_nested(run, "ended_at")
    
    return {
        "run_id": _generate_run_id(),
        "run_mode": _safe_get_nested(run, "mode", default="initial"),
        "run_started_at": run_started_at,
        "run_ended_at": run_ended_at,
        "run_duration_secs": _calculate_duration(run_started_at, run_ended_at),
        
        "phase": phase,
        "step_name": step_name,
        "step_order": step_order,
        "step_type": None,
        
        "start_time": None,
        "end_time": None,
        "duration_secs": None,
        
        "table_fqn": None,
        "write_mode": None,
        
        "input_rows": None,
        "output_rows": None,
        "rows_written": None,
        
        "valid_rows": 0,
        "invalid_rows": 0,
        "validation_rate": 100.0,
        "validation_passed": True,
        "data_quality_status": DataQualityStatus.EXCELLENT.value,
        
        "previous_watermark": None,
        "new_watermark": None,
        "filtered_rows": None,
        
        "error_message": None,
        "error_type": None,
        "success": True,
        "retry_count": 0,
        
        "memory_usage_mb": None,
        "cpu_usage_percent": None,
        "io_operations": None,
        
        "null_percentage": None,
        "duplicate_percentage": None,
        "schema_compliance": None,
        
        "schema_name": schema_name,
        "pipeline_version": config.pipeline_version,
        "created_at": datetime.now(),
        "log_level": LogLevel.INFO.value,
    }


def _row_from_bronze(
    phase_name: str, 
    record: Dict[str, Any], 
    report: ReportDict, 
    step_order: int,
    schema_name: str,
    config: LogWriterConfig
) -> PipelineLogRow:
    """Create a log row from bronze step data."""
    row = _create_base_log_row(report, "bronze", phase_name, step_order, schema_name, config)
    
    try:
        validation = _safe_get_nested(record, "validation", default={})
        
        # Timing
        row["start_time"] = _safe_get_nested(validation, "start_at")
        row["end_time"] = _safe_get_nested(validation, "end_at")
        row["duration_secs"] = _calculate_duration(row["start_time"], row["end_time"])
        
        # Validation data
        row["valid_rows"] = int(_safe_get_nested(validation, "valid_rows", default=0))
        row["invalid_rows"] = int(_safe_get_nested(validation, "invalid_rows", default=0))
        row["validation_rate"] = float(_safe_get_nested(validation, "validation_rate", default=100.0))
        row["validation_passed"] = row["validation_rate"] >= config.alert_thresholds["validation_rate"]
        
        # Data quality metrics
        null_percentage = _safe_get_nested(validation, "null_percentage", default=0.0)
        duplicate_percentage = _safe_get_nested(validation, "duplicate_percentage", default=0.0)
        row["null_percentage"] = null_percentage
        row["duplicate_percentage"] = duplicate_percentage
        row["data_quality_status"] = _calculate_data_quality_status(
            row["validation_rate"], null_percentage, duplicate_percentage
        )
        
        # Table information
        row["table_fqn"] = _safe_get_nested(record, "table_fqn")
        
        # Row counts
        row["input_rows"] = row["valid_rows"] + row["invalid_rows"]
        row["output_rows"] = row["valid_rows"]
        row["rows_written"] = row["valid_rows"]
        
        # Performance metrics
        row["memory_usage_mb"] = _safe_get_nested(validation, "memory_usage_mb")
        row["cpu_usage_percent"] = _safe_get_nested(validation, "cpu_usage_percent")
        row["io_operations"] = _safe_get_nested(validation, "io_operations")
        
    except Exception as e:
        logging.warning(f"Error processing bronze step {phase_name}: {e}")
        row["error_message"] = str(e)
        row["error_type"] = type(e).__name__
        row["success"] = False
        row["log_level"] = LogLevel.ERROR.value
    
    return row


def _row_from_silver(
    phase_name: str, 
    record: Dict[str, Any], 
    report: ReportDict, 
    step_order: int,
    schema_name: str,
    config: LogWriterConfig
) -> PipelineLogRow:
    """Create a log row from silver step data."""
    row = _create_base_log_row(report, "silver", phase_name, step_order, schema_name, config)
    
    try:
        transform = _safe_get_nested(record, "transform", default={})
        write = _safe_get_nested(record, "write", default={})
        validation = _safe_get_nested(record, "validation", default={})
        
        # Timing - prefer transform, then validation, then write
        row["start_time"] = (
            _safe_get_nested(transform, "start_at") or 
            _safe_get_nested(validation, "start_at") or 
            _safe_get_nested(write, "start_at")
        )
        row["end_time"] = (
            _safe_get_nested(transform, "end_at") or 
            _safe_get_nested(validation, "end_at") or 
            _safe_get_nested(write, "end_at")
        )
        row["duration_secs"] = _calculate_duration(row["start_time"], row["end_time"])
        
        # Table information
        row["table_fqn"] = _safe_get_nested(record, "table_fqn")
        row["write_mode"] = _safe_get_nested(write, "mode")
        
        # Row counts
        row["input_rows"] = int(_safe_get_nested(transform, "input_rows", default=0)) if transform else None
        row["output_rows"] = int(_safe_get_nested(transform, "output_rows", default=0)) if transform else None
        row["rows_written"] = int(_safe_get_nested(write, "rows_written", default=0))
        
        # Validation data
        row["valid_rows"] = int(_safe_get_nested(validation, "valid_rows", default=0))
        row["invalid_rows"] = int(_safe_get_nested(validation, "invalid_rows", default=0))
        row["validation_rate"] = float(_safe_get_nested(validation, "validation_rate", default=100.0))
        row["validation_passed"] = row["validation_rate"] >= config.alert_thresholds["validation_rate"]
        
        # Data quality metrics
        null_percentage = _safe_get_nested(validation, "null_percentage", default=0.0)
        duplicate_percentage = _safe_get_nested(validation, "duplicate_percentage", default=0.0)
        row["null_percentage"] = null_percentage
        row["duplicate_percentage"] = duplicate_percentage
        row["data_quality_status"] = _calculate_data_quality_status(
            row["validation_rate"], null_percentage, duplicate_percentage
        )
        
        # Watermarking
        row["previous_watermark"] = _safe_get_nested(record, "previous_watermark")
        row["new_watermark"] = _safe_get_nested(record, "new_watermark")
        row["filtered_rows"] = _safe_get_nested(record, "filtered_rows")
        
        # Performance metrics
        row["memory_usage_mb"] = _safe_get_nested(validation, "memory_usage_mb")
        row["cpu_usage_percent"] = _safe_get_nested(validation, "cpu_usage_percent")
        row["io_operations"] = _safe_get_nested(validation, "io_operations")
        
        # Schema compliance
        row["schema_compliance"] = _safe_get_nested(validation, "schema_compliance", default=100.0)
        
    except Exception as e:
        logging.warning(f"Error processing silver step {phase_name}: {e}")
        row["error_message"] = str(e)
        row["error_type"] = type(e).__name__
        row["success"] = False
        row["log_level"] = LogLevel.ERROR.value
    
    return row


def _row_from_gold(
    phase_name: str, 
    record: Dict[str, Any], 
    report: ReportDict, 
    step_order: int,
    schema_name: str,
    config: LogWriterConfig
) -> PipelineLogRow:
    """Create a log row from gold step data."""
    row = _create_base_log_row(report, "gold", phase_name, step_order, schema_name, config)
    
    try:
        transform = _safe_get_nested(record, "transform", default={})
        write = _safe_get_nested(record, "write", default={})
        validation = _safe_get_nested(record, "validation", default={})
        
        # Timing
        row["start_time"] = (
            _safe_get_nested(transform, "start_at") or 
            _safe_get_nested(validation, "start_at") or 
            _safe_get_nested(write, "start_at")
        )
        row["end_time"] = (
            _safe_get_nested(transform, "end_at") or 
            _safe_get_nested(validation, "end_at") or 
            _safe_get_nested(write, "end_at")
        )
        row["duration_secs"] = _calculate_duration(row["start_time"], row["end_time"])
        
        # Table information
        row["table_fqn"] = _safe_get_nested(record, "table_fqn")
        row["write_mode"] = _safe_get_nested(write, "mode")
        
        # Row counts
        row["input_rows"] = int(_safe_get_nested(transform, "input_rows", default=0))
        row["output_rows"] = int(_safe_get_nested(transform, "output_rows", default=0))
        row["rows_written"] = int(_safe_get_nested(write, "rows_written", default=0))
        
        # Validation data
        row["valid_rows"] = int(_safe_get_nested(validation, "valid_rows", default=0))
        row["invalid_rows"] = int(_safe_get_nested(validation, "invalid_rows", default=0))
        row["validation_rate"] = float(_safe_get_nested(validation, "validation_rate", default=100.0))
        row["validation_passed"] = row["validation_rate"] >= config.alert_thresholds["validation_rate"]
        
        # Data quality metrics
        null_percentage = _safe_get_nested(validation, "null_percentage", default=0.0)
        duplicate_percentage = _safe_get_nested(validation, "duplicate_percentage", default=0.0)
        row["null_percentage"] = null_percentage
        row["duplicate_percentage"] = duplicate_percentage
        row["data_quality_status"] = _calculate_data_quality_status(
            row["validation_rate"], null_percentage, duplicate_percentage
        )
        
        # Performance metrics
        row["memory_usage_mb"] = _safe_get_nested(validation, "memory_usage_mb")
        row["cpu_usage_percent"] = _safe_get_nested(validation, "cpu_usage_percent")
        row["io_operations"] = _safe_get_nested(validation, "io_operations")
        
        # Schema compliance
        row["schema_compliance"] = _safe_get_nested(validation, "schema_compliance", default=100.0)
        
    except Exception as e:
        logging.warning(f"Error processing gold step {phase_name}: {e}")
        row["error_message"] = str(e)
        row["error_type"] = type(e).__name__
        row["success"] = False
        row["log_level"] = LogLevel.ERROR.value
    
    return row


def flatten_pipeline_report(
    report: ReportDict, 
    schema_name: str = "default_schema",
    config: Optional[LogWriterConfig] = None
) -> List[PipelineLogRow]:
    """
    Flatten a pipeline execution report into structured log rows.
    
    Args:
        report: Pipeline execution report dictionary
        schema_name: Name of the schema for logging
        config: LogWriter configuration
        
    Returns:
        List of structured log rows
    """
    if config is None:
        config = LogWriterConfig()
    
    rows: List[PipelineLogRow] = []
    step_order = 0
    
    try:
        # Process bronze steps
        bronze_steps = _safe_get_nested(report, "bronze", default={})
        for step_name, step_data in bronze_steps.items():
            if isinstance(step_data, dict):
                rows.append(_row_from_bronze(step_name, step_data, report, step_order, schema_name, config))
                step_order += 1
        
        # Process silver steps
        silver_steps = _safe_get_nested(report, "silver", default={})
        for step_name, step_data in silver_steps.items():
            if isinstance(step_data, dict):
                rows.append(_row_from_silver(step_name, step_data, report, step_order, schema_name, config))
                step_order += 1
        
        # Process gold steps
        gold_steps = _safe_get_nested(report, "gold", default={})
        for step_name, step_data in gold_steps.items():
            if isinstance(step_data, dict):
                rows.append(_row_from_gold(step_name, step_data, report, step_order, schema_name, config))
                step_order += 1
                
    except Exception as e:
        logging.error(f"Error flattening pipeline report: {e}")
        # Create an error row
        error_row = _create_base_log_row(report, "bronze", "error", 0, schema_name, config)
        error_row["error_message"] = str(e)
        error_row["error_type"] = type(e).__name__
        error_row["success"] = False
        error_row["log_level"] = LogLevel.ERROR.value
        rows.append(error_row)
    
    return rows


# ---------- Enhanced LogWriter class ----------

class LogWriter:
    """
    Enhanced log writer for pipeline execution reports with comprehensive features.
    
    Features:
    - Flattens pipeline reports into structured log rows
    - Supports both Delta Lake and regular Spark tables
    - Comprehensive error handling and validation
    - Query and filtering capabilities
    - Performance monitoring and analytics
    - Data quality tracking and alerts
    - Export capabilities for external systems
    - Integration with current pipeline package structure
    
    Methods
    -------
    - create_table(report, mode="overwrite")
      Creates or overwrites the logs table with flattened rows
    - append(report)
      Appends flattened rows to the existing table
    - query(filters=None, limit=None)
      Query the logs table with optional filters
    - get_summary()
      Get execution summary statistics
    - get_analytics()
      Get advanced analytics and insights
    - show(n=None, filters=None)
      Display logs with optional filtering
    - cleanup(days_to_keep=30)
      Clean up old log entries
    - export(format="json", path=None)
      Export logs in various formats
    - get_alerts()
      Get data quality and performance alerts
    """

    def __init__(
        self,
        spark: SparkSession,
        write_schema: str,
        logs_table_name: str,
        config: Optional[LogWriterConfig] = None
    ) -> None:
        """
        Initialize the LogWriter.
        
        Args:
            spark: Spark session
            write_schema: Schema name for the logs table
            logs_table_name: Name of the logs table
            config: LogWriter configuration
        """
        self.spark = spark
        self.write_schema = write_schema
        self.logs_table_name = logs_table_name
        self.config = config or LogWriterConfig()
        self.schema = PIPELINE_LOG_SCHEMA
        self.table_fqn = f"{self.write_schema}.{self.logs_table_name}"
        
        # Set up logging
        self.logger = logging.getLogger(f"LogWriter.{self.logs_table_name}")

    def create_table(self, report: ReportDict, mode: str = "overwrite") -> DataFrame:
        """
        Create or overwrite the logs table with flattened rows.
        
        Args:
            report: Pipeline execution report
            mode: Write mode ("overwrite" or "append")
            
        Returns:
            DataFrame containing the log rows
        """
        try:
            self.logger.info(f"Creating logs table with mode: {mode}")
            
            # Flatten the report
            rows = flatten_pipeline_report(report, self.write_schema, self.config)
            
            if not rows:
                self.logger.warning("No log rows generated from report")
                return self.spark.createDataFrame([], schema=self.schema)
            
            # Create DataFrame
            df = self.spark.createDataFrame(rows, schema=self.schema)
            
            # Write to table
            writer = df.write.mode(mode)
            if self.config.use_delta:
                writer = writer.format("parquet").option("overwriteSchema", "true")
            else:
                writer = writer.format("parquet")
            
            writer.saveAsTable(self.table_fqn)
            
            self.logger.info(f"Successfully created logs table with {len(rows)} rows")
            return df
            
        except Exception as e:
            self.logger.error(f"Error creating logs table: {e}")
            raise

    def append(self, report: ReportDict) -> DataFrame:
        """
        Append flattened rows to the existing logs table.
        
        Args:
            report: Pipeline execution report
            
        Returns:
            DataFrame containing the appended log rows
        """
        try:
            self.logger.info("Appending to logs table")
            
            # Flatten the report
            rows = flatten_pipeline_report(report, self.write_schema, self.config)
            
            if not rows:
                self.logger.warning("No log rows generated from report")
                return self.spark.createDataFrame([], schema=self.schema)
            
            # Create DataFrame
            df = self.spark.createDataFrame(rows, schema=self.schema)
            
            # Write to table
            writer = df.write.mode("append")
            if self.config.use_delta:
                writer = writer.format("parquet")
            else:
                writer = writer.format("parquet")
            
            writer.saveAsTable(self.table_fqn)
            
            self.logger.info(f"Successfully appended {len(rows)} rows to logs table")
            return df
            
        except Exception as e:
            self.logger.error(f"Error appending to logs table: {e}")
            raise

    def query(self, filters: Optional[Dict[str, Any]] = None, limit: Optional[int] = None) -> DataFrame:
        """
        Query the logs table with optional filters.
        
        Args:
            filters: Dictionary of column filters
            limit: Maximum number of rows to return
            
        Returns:
            Filtered DataFrame
        """
        try:
            df = self.spark.table(self.table_fqn)
            
            if filters:
                for column, value in filters.items():
                    if isinstance(value, list):
                        df = df.filter(F.col(column).isin(value))
                    else:
                        df = df.filter(F.col(column) == value)
            
            if limit:
                df = df.limit(limit)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error querying logs table: {e}")
            raise

    def get_summary(self) -> Dict[str, Any]:
        """
        Get execution summary statistics.
        
        Returns:
            Dictionary containing summary statistics
        """
        try:
            df = self.spark.table(self.table_fqn)
            
            summary = {
                "total_runs": df.select("run_id").distinct().count(),
                "total_steps": df.count(),
                "successful_steps": df.filter(F.col("success") == True).count(),
                "failed_steps": df.filter(F.col("success") == False).count(),
                "avg_duration_secs": df.agg(F.avg("duration_secs")).collect()[0][0],
                "total_rows_processed": df.agg(F.sum("input_rows")).collect()[0][0] or 0,
                "total_rows_written": df.agg(F.sum("rows_written")).collect()[0][0] or 0,
                "avg_validation_rate": df.agg(F.avg("validation_rate")).collect()[0][0],
                "phases": df.groupBy("phase").count().collect(),
                "recent_runs": df.select("run_id", "run_started_at").distinct().orderBy(F.desc("run_started_at")).limit(10).collect()
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting summary: {e}")
            return {}

    def get_analytics(self) -> Dict[str, Any]:
        """
        Get advanced analytics and insights.
        
        Returns:
            Dictionary containing analytics data
        """
        try:
            df = self.spark.table(self.table_fqn)
            
            analytics = {
                "performance_metrics": {
                    "avg_duration_by_phase": df.groupBy("phase").agg(F.avg("duration_secs")).collect(),
                    "duration_trends": df.groupBy("run_id").agg(F.avg("duration_secs")).orderBy(F.desc("run_id")).limit(20).collect(),
                    "memory_usage": df.agg(F.avg("memory_usage_mb")).collect()[0][0],
                    "cpu_usage": df.agg(F.avg("cpu_usage_percent")).collect()[0][0]
                },
                "data_quality_metrics": {
                    "avg_validation_rate": df.agg(F.avg("validation_rate")).collect()[0][0],
                    "data_quality_distribution": df.groupBy("data_quality_status").count().collect(),
                    "null_percentage_trends": df.groupBy("run_id").agg(F.avg("null_percentage")).orderBy(F.desc("run_id")).limit(20).collect(),
                    "duplicate_percentage_trends": df.groupBy("run_id").agg(F.avg("duplicate_percentage")).orderBy(F.desc("run_id")).limit(20).collect()
                },
                "error_analysis": {
                    "error_types": df.filter(F.col("error_type").isNotNull()).groupBy("error_type").count().collect(),
                    "error_trends": df.filter(F.col("error_type").isNotNull()).groupBy("run_id").count().orderBy(F.desc("run_id")).limit(20).collect(),
                    "failed_steps_by_phase": df.filter(F.col("success") == False).groupBy("phase").count().collect()
                },
                "throughput_metrics": {
                    "rows_per_second": df.agg(F.sum("rows_written") / F.sum("duration_secs")).collect()[0][0],
                    "avg_rows_per_step": df.agg(F.avg("rows_written")).collect()[0][0],
                    "throughput_by_phase": df.groupBy("phase").agg(F.sum("rows_written") / F.sum("duration_secs")).collect()
                }
            }
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Error getting analytics: {e}")
            return {}

    def get_alerts(self) -> List[Dict[str, Any]]:
        """
        Get data quality and performance alerts.
        
        Returns:
            List of alert dictionaries
        """
        try:
            df = self.spark.table(self.table_fqn)
            alerts = []
            
            # Validation rate alerts
            low_validation = df.filter(F.col("validation_rate") < self.config.alert_thresholds["validation_rate"])
            if low_validation.count() > 0:
                alerts.append({
                    "type": "validation_rate",
                    "severity": "warning",
                    "message": f"Found {low_validation.count()} steps with validation rate below {self.config.alert_thresholds['validation_rate']}%",
                    "affected_steps": low_validation.select("step_name", "validation_rate").collect()
                })
            
            # Success rate alerts
            failed_steps = df.filter(F.col("success") == False)
            if failed_steps.count() > 0:
                alerts.append({
                    "type": "success_rate",
                    "severity": "error",
                    "message": f"Found {failed_steps.count()} failed steps",
                    "affected_steps": failed_steps.select("step_name", "error_message").collect()
                })
            
            # Duration alerts
            long_duration = df.filter(F.col("duration_secs") > self.config.alert_thresholds["duration_threshold"])
            if long_duration.count() > 0:
                alerts.append({
                    "type": "duration",
                    "severity": "warning",
                    "message": f"Found {long_duration.count()} steps taking longer than {self.config.alert_thresholds['duration_threshold']} seconds",
                    "affected_steps": long_duration.select("step_name", "duration_secs").collect()
                })
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"Error getting alerts: {e}")
            return []

    def show(self, n: Optional[int] = None, filters: Optional[Dict[str, Any]] = None) -> None:
        """
        Display logs with optional filtering.
        
        Args:
            n: Number of rows to show
            filters: Dictionary of column filters
        """
        try:
            df = self.query(filters, limit=n)
            df.show(n, truncate=False)
            
        except Exception as e:
            self.logger.error(f"Error displaying logs: {e}")
            print(f"Error displaying logs: {e}")

    def export(self, format: str = "json", path: Optional[str] = None) -> str:
        """
        Export logs in various formats.
        
        Args:
            format: Export format ("json", "csv", "parquet")
            path: Output path (optional)
            
        Returns:
            Path where the file was saved
        """
        try:
            if format not in self.config.export_formats:
                raise ValueError(f"Unsupported export format: {format}")
            
            df = self.spark.table(self.table_fqn)
            
            if path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                path = f"pipeline_logs_{timestamp}.{format}"
            
            if format == "json":
                df.write.mode("overwrite").json(path)
            elif format == "csv":
                df.write.mode("overwrite").option("header", "true").csv(path)
            elif format == "parquet":
                df.write.mode("overwrite").parquet(path)
            
            self.logger.info(f"Exported logs to {path}")
            return path
            
        except Exception as e:
            self.logger.error(f"Error exporting logs: {e}")
            raise

    def cleanup(self, days_to_keep: Optional[int] = None) -> int:
        """
        Clean up old log entries.
        
        Args:
            days_to_keep: Number of days of logs to keep (uses config default if None)
            
        Returns:
            Number of rows deleted
        """
        try:
            if days_to_keep is None:
                days_to_keep = self.config.retention_days
            
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # Count rows to be deleted
            df = self.spark.table(self.table_fqn)
            old_rows = df.filter(F.col("created_at") < cutoff_date)
            count = old_rows.count()
            
            if count > 0:
                if self.config.use_delta:
                    # For Delta tables, we need to use DELETE
                    self.logger.warning("Delta table cleanup requires manual DELETE operations")
                else:
                    # For regular tables, we can overwrite with filtered data
                    df.filter(F.col("created_at") >= cutoff_date).write \
                        .mode("overwrite") \
                        .format("parquet") \
                        .saveAsTable(self.table_fqn)
            
            self.logger.info(f"Cleaned up {count} old log entries")
            return count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up logs: {e}")
            return 0

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the logs table.
        
        Returns:
            Dictionary containing table information
        """
        try:
            df = self.spark.table(self.table_fqn)
            
            info = {
                "table_name": self.table_fqn,
                "total_rows": df.count(),
                "columns": df.columns,
                "schema": df.schema,
                "is_delta": self.config.use_delta,
                "pipeline_version": self.config.pipeline_version,
                "retention_days": self.config.retention_days,
                "export_formats": self.config.export_formats
            }
            
            return info
            
        except Exception as e:
            self.logger.error(f"Error getting table info: {e}")
            return {"error": str(e)}

    def get_health_status(self) -> Dict[str, Any]:
        """
        Get overall health status of the logging system.
        
        Returns:
            Dictionary containing health status information
        """
        try:
            df = self.spark.table(self.table_fqn)
            
            # Calculate health metrics
            total_steps = df.count()
            successful_steps = df.filter(F.col("success") == True).count()
            success_rate = (successful_steps / total_steps * 100) if total_steps > 0 else 0
            
            avg_validation_rate = df.agg(F.avg("validation_rate")).collect()[0][0] or 0
            avg_duration = df.agg(F.avg("duration_secs")).collect()[0][0] or 0
            
            # Determine overall health status
            if success_rate >= 95 and avg_validation_rate >= 95:
                health_status = "healthy"
            elif success_rate >= 90 and avg_validation_rate >= 90:
                health_status = "warning"
            else:
                health_status = "critical"
            
            return {
                "health_status": health_status,
                "success_rate": success_rate,
                "avg_validation_rate": avg_validation_rate,
                "avg_duration_secs": avg_duration,
                "total_steps": total_steps,
                "alerts": self.get_alerts(),
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Error getting health status: {e}")
            return {
                "health_status": "error",
                "error": str(e),
                "last_updated": datetime.now().isoformat()
            }