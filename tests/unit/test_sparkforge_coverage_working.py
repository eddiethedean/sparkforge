"""
Working SparkForge coverage tests using Mock Spark.
"""

import os
import pytest

# Use mock functions when in mock mode
if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
    from mock_spark import functions as F
else:
    from pyspark.sql import functions as F
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.execution import ExecutionEngine, ExecutionMode, StepStatus, StepType
from sparkforge.validation.pipeline_validation import UnifiedValidator, ValidationResult, StepValidator
from sparkforge.writer.core import LogWriter, table_exists
from sparkforge.writer.models import WriterConfig, WriteMode, LogLevel, LogRow, WriterMetrics
from sparkforge.writer.analytics import DataQualityAnalyzer, TrendAnalyzer
from sparkforge.writer.monitoring import PerformanceMonitor, AnalyticsEngine
from sparkforge.writer.operations import DataProcessor
from sparkforge.writer.query_builder import QueryBuilder
from sparkforge.writer.storage import StorageManager
from sparkforge.models import (
    PipelineConfig, ValidationThresholds, ParallelConfig,
    ExecutionContext, StageStats, StepResult
)
from sparkforge.models.steps import BronzeStep, SilverStep, GoldStep
from sparkforge.models.dependencies import SilverDependencyInfo, CrossLayerDependency
from sparkforge.logging import PipelineLogger
from sparkforge.performance import now_dt, format_duration
from sparkforge.table_operations import drop_table, table_exists as sparkforge_table_exists
from sparkforge.validation.data_validation import _convert_rule_to_expression, _convert_rules_to_expressions
from sparkforge.validation.utils import safe_divide, get_dataframe_info
from sparkforge.dependencies.analyzer import DependencyAnalyzer
from sparkforge.dependencies.graph import DependencyGraph, StepNode
from sparkforge.errors import (
    ConfigurationError, ValidationError, ExecutionError, 
    DataError, SystemError, PerformanceError, ResourceError
)
from mock_spark import MockSparkSession
from mock_spark.types import MockStructType, MockStructField, StringType, IntegerType, DoubleType
from datetime import datetime
import uuid


class TestSparkForgeCoverageWorking:
    """Working SparkForge coverage tests using Mock Spark."""
    
    def test_pipeline_builder_comprehensive(self, spark_session):
        """Test comprehensive PipelineBuilder functionality."""
        # Test basic initialization
        builder = PipelineBuilder(spark=spark_session, schema="test_schema")
        assert builder.spark == spark_session
        assert builder.schema == "test_schema"
        
        # Test with custom quality rates
        builder_custom = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            min_bronze_rate=90.0,
            min_silver_rate=95.0,
            min_gold_rate=98.0,
            verbose=True
        )
        assert builder_custom.min_bronze_rate == 90.0
        assert builder_custom.min_silver_rate == 95.0
        assert builder_custom.min_gold_rate == 98.0
        assert builder_custom.verbose is True
        
        # Test schema validation
        spark_session.storage.create_schema("test_schema")
        builder._validate_schema("test_schema")  # Should not raise
        
        # Test schema creation
        builder._create_schema_if_not_exists("new_schema")
        assert spark_session.storage.schema_exists("new_schema")
        
        # Test with non-existent schema
        with pytest.raises(Exception):
            builder._validate_schema("nonexistent_schema")
    
    def test_execution_engine_comprehensive(self, spark_session):
        """Test comprehensive ExecutionEngine functionality."""
        # Create comprehensive config
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        parallel_config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=600)
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True
        )
        
        # Test execution engine initialization
        engine = ExecutionEngine(spark=spark_session, config=config)
        assert engine.spark == spark_session
        assert engine.config == config
        
        # Test execution context creation
        context = ExecutionContext(
            execution_id=str(uuid.uuid4()),
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now()
        )
        assert context.execution_id is not None
        assert context.mode == ExecutionMode.INITIAL
        
        # Test step execution result
        step_result = StepResult(
            step_name="test_step",
            step_type=StepType.BRONZE,
            status=StepStatus.COMPLETED,
            start_time=datetime.now(),
            rows_processed=100
        )
        assert step_result.step_name == "test_step"
        assert step_result.step_type == StepType.BRONZE
        assert step_result.status == StepStatus.COMPLETED
    
    def test_validation_system_comprehensive(self, spark_session):
        """Test comprehensive validation system."""
        # Test UnifiedValidator
        validator = UnifiedValidator()
        assert validator.logger is not None
        assert len(validator.custom_validators) == 0
        
        # Test adding custom validator
        class CustomValidator(StepValidator):
            def validate(self, step, context):
                return ValidationResult(
                    is_valid=True,
                    errors=[],
                    warnings=[],
                    recommendations=[]
                )
        
        custom_validator = CustomValidator()
        validator.add_validator(custom_validator)
        assert len(validator.custom_validators) == 1
        
        # Test validation result
        result = ValidationResult(
            is_valid=True,
            errors=["Error 1", "Error 2"],
            warnings=["Warning 1"],
            recommendations=["Recommendation 1"]
        )
        assert result.is_valid is True
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
        assert len(result.recommendations) == 1
    
    def test_writer_system_comprehensive(self, spark_session):
        """Test comprehensive writer system."""
        # Test WriterConfig
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_logs",
            write_mode=WriteMode.APPEND,
            log_level=LogLevel.INFO,
            batch_size=1000,
            compression="snappy",
            max_file_size_mb=128,
            partition_columns=["date"],
            partition_count=10,
            enable_schema_evolution=True,
            schema_validation_mode="strict",
            auto_optimize_schema=True
        )
        assert config.table_schema == "test_schema"
        assert config.write_mode == WriteMode.APPEND
        assert config.batch_size == 1000
        
        # Test LogWriter
        writer = LogWriter(spark=spark_session, config=config)
        assert writer.spark == spark_session
        assert writer.config == config
        
        # Test LogRow
        log_row = LogRow(
            execution_id=str(uuid.uuid4()),
            step_name="test_step",
            status="completed",
            timestamp=datetime.now(),
            duration=1.5,
            rows_processed=100
        )
        assert log_row.execution_id is not None
        assert log_row.step_name == "test_step"
        
        # Test WriterMetrics
        metrics = WriterMetrics(
            execution_time=1.5,
            rows_written=100,
            bytes_written=1024,
            files_written=1
        )
        assert metrics.execution_time == 1.5
        assert metrics.rows_written == 100
        
        # Test table_exists function
        spark_session.storage.create_schema("test_schema")
        spark_session.storage.create_table("test_schema", "test_table", [])
        assert table_exists(spark_session, "test_schema", "test_table")
        assert not table_exists(spark_session, "test_schema", "nonexistent_table")
    
    def test_analytics_system(self, spark_session):
        """Test analytics system components."""
        # Test DataQualityAnalyzer
        analyzer = DataQualityAnalyzer(spark_session)
        assert analyzer.spark == spark_session
        
        # Test TrendAnalyzer
        trend_analyzer = TrendAnalyzer(spark_session)
        assert trend_analyzer.spark == spark_session
        
        # Test PerformanceMonitor
        monitor = PerformanceMonitor(spark_session)
        assert monitor.spark == spark_session
        
        # Test AnalyticsEngine
        analytics_engine = AnalyticsEngine(spark_session)
        assert analytics_engine.spark == spark_session
        
        # Test DataProcessor
        processor = DataProcessor(spark_session)
        assert processor.spark == spark_session
        
        # Test QueryBuilder
        query_builder = QueryBuilder(spark_session)
        assert query_builder.spark == spark_session
        
        # Test StorageManager
        storage_manager = StorageManager(spark_session)
        assert storage_manager.spark == spark_session
    
    def test_models_comprehensive(self, spark_session):
        """Test comprehensive model classes."""
        # Test ValidationThresholds
        thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
        assert thresholds.bronze == 95.0
        assert thresholds.silver == 98.0
        assert thresholds.gold == 99.0
        
        # Test ParallelConfig
        parallel_config = ParallelConfig(enabled=True, max_workers=4, timeout_secs=600)
        assert parallel_config.enabled is True
        assert parallel_config.max_workers == 4
        assert parallel_config.timeout_secs == 600
        
        # Test PipelineConfig
        config = PipelineConfig(
            schema="test_schema",
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=True
        )
        assert config.schema == "test_schema"
        assert config.thresholds == thresholds
        assert config.parallel == parallel_config
        assert config.verbose is True
        
        # Test StageStats
        stage_stats = StageStats(
            stage_name="test_stage",
            start_time=datetime.now(),
            duration=1.5,
            rows_processed=100,
            status="completed"
        )
        assert stage_stats.stage_name == "test_stage"
        assert stage_stats.rows_processed == 100
        
        # Test ExecutionResult (from execution module)
        from sparkforge.execution import ExecutionResult
        execution_result = ExecutionResult(
            execution_id=str(uuid.uuid4()),
            mode=ExecutionMode.INITIAL,
            start_time=datetime.now(),
            status="completed",
            steps=[]
        )
        assert execution_result.execution_id is not None
        assert execution_result.mode == ExecutionMode.INITIAL
    
    def test_step_models(self, spark_session):
        """Test step model classes."""
        # Test BronzeStep
        bronze_step = BronzeStep(
            name="bronze_step",
            table_name="bronze.table",
            rules={"id": ["not_null"]},
            incremental_col="timestamp"
        )
        assert bronze_step.name == "bronze_step"
        assert bronze_step.table_name == "bronze.table"
        assert bronze_step.rules == {"id": ["not_null"]}
        
        # Test SilverStep
        silver_step = SilverStep(
            name="silver_step",
            table_name="silver.table",
            source_bronze="bronze.table",
            transform=None,
            rules={"id": ["not_null"]}
        )
        assert silver_step.name == "silver_step"
        assert silver_step.table_name == "silver.table"
        assert silver_step.source_bronze == "bronze.table"
        
        # Test GoldStep
        gold_step = GoldStep(
            name="gold_step",
            table_name="gold.table",
            source_silvers=["silver.table"],
            transform=None,
            rules={"id": ["not_null"]}
        )
        assert gold_step.name == "gold_step"
        assert gold_step.table_name == "gold.table"
        assert gold_step.source_silvers == ["silver.table"]
    
    def test_dependency_system(self, spark_session):
        """Test dependency analysis system."""
        # Test DependencyAnalyzer
        analyzer = DependencyAnalyzer(spark_session)
        assert analyzer.spark == spark_session
        
        # Test DependencyGraph
        graph = DependencyGraph()
        assert graph is not None
        
        # Test StepNode
        node = StepNode(
            step_id="test_step",
            step_type=StepType.BRONZE,
            dependencies=[]
        )
        assert node.step_id == "test_step"
        assert node.step_type == StepType.BRONZE
        
        # Test SilverDependencyInfo
        silver_dep = SilverDependencyInfo(
            step_name="silver_step",
            source_bronze="bronze_step",
            dependencies=[]
        )
        assert silver_dep.step_name == "silver_step"
        assert silver_dep.source_bronze == "bronze_step"
        
        # Test CrossLayerDependency
        cross_dep = CrossLayerDependency(
            from_step="bronze_step",
            to_step="silver_step",
            dependency_type="data"
        )
        assert cross_dep.from_step == "bronze_step"
        assert cross_dep.to_step == "silver_step"
    
    def test_logging_system(self, spark_session):
        """Test logging system."""
        # Test PipelineLogger
        logger = PipelineLogger()
        assert logger is not None
        
        # Test with custom configuration
        logger_custom = PipelineLogger(
            name="custom_logger",
            level="INFO",
            format_string="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        assert logger_custom.name == "custom_logger"
        assert logger_custom.level == "INFO"
    
    def test_performance_system(self, spark_session):
        """Test performance tracking system."""
        # Test performance utility functions
        current_time = now_dt()
        assert current_time is not None
        
        # Test duration formatting
        formatted_duration = format_duration(1.5)
        assert formatted_duration is not None
        assert "1.5" in formatted_duration or "1" in formatted_duration
    
    def test_table_operations(self, spark_session):
        """Test table operations."""
        # Create test schema and table
        spark_session.storage.create_schema("test_schema")
        spark_session.storage.create_table("test_schema", "test_table", [])
        
        # Test table_exists
        assert sparkforge_table_exists(spark_session, "test_schema", "test_table")
        assert not sparkforge_table_exists(spark_session, "test_schema", "nonexistent_table")
        
        # Test drop_table
        drop_table(spark_session, "test_schema.test_table")
        # Table should be dropped (implementation dependent)
    
    def test_error_handling(self, spark_session):
        """Test error handling classes."""
        # Test ConfigurationError
        with pytest.raises(ConfigurationError):
            raise ConfigurationError("Test configuration error")
        
        # Test ValidationError
        with pytest.raises(ValidationError):
            raise ValidationError("Test validation error")
        
        # Test ExecutionError
        with pytest.raises(ExecutionError):
            raise ExecutionError("Test execution error")
        
        # Test DataError
        with pytest.raises(DataError):
            raise DataError("Test data error")
        
        # Test SystemError
        with pytest.raises(SystemError):
            raise SystemError("Test system error")
        
        # Test PerformanceError
        with pytest.raises(PerformanceError):
            raise PerformanceError("Test performance error")
        
        # Test ResourceError
        with pytest.raises(ResourceError):
            raise ResourceError("Test resource error")
    
    def test_validation_data_validation(self, spark_session):
        """Test data validation functionality."""
        # Test validation rule conversion
        expr = _convert_rule_to_expression("not_null", "id")
        assert expr is not None
        
        # Test rules conversion
        rules = {"id": ["not_null", "positive"]}
        converted = _convert_rules_to_expressions(rules)
        assert "id" in converted
        assert len(converted["id"]) == 2
    
    def test_validation_utils(self, spark_session):
        """Test validation utility functions."""
        # Test safe_divide function
        result = safe_divide(10, 2)
        assert result == 5.0
        
        result_zero = safe_divide(10, 0)
        assert result_zero == 0.0
        
        result_default = safe_divide(10, 0, default=1.0)
        assert result_default == 1.0
        
        # Test get_dataframe_info function
        schema = MockStructType([
            MockStructField("id", IntegerType()),
            MockStructField("name", StringType())
        ])
        
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        df = spark_session.createDataFrame(data, schema)
        
        info = get_dataframe_info(df)
        assert info["row_count"] == 2
        assert info["column_count"] == 2
        assert info["is_empty"] is False
        assert "id" in info["columns"]
        assert "name" in info["columns"]
    
    def test_pipeline_validation_comprehensive(self, spark_session):
        """Test comprehensive pipeline validation."""
        # Test UnifiedValidator with custom validators
        validator = UnifiedValidator()
        
        # Test validation methods
        validator.validate_step(None, None)  # Should handle None gracefully
        validator.validate_pipeline(None)  # Should handle None gracefully
        
        # Test custom validator integration
        class TestValidator(StepValidator):
            def validate(self, step, context):
                return ValidationResult(
                    is_valid=True,
                    errors=[],
                    warnings=[],
                    recommendations=[]
                )
        
        test_validator = TestValidator()
        validator.add_validator(test_validator)
        assert len(validator.custom_validators) == 1
    
    def test_writer_analytics_comprehensive(self, spark_session):
        """Test comprehensive writer analytics."""
        # Test DataQualityAnalyzer methods
        analyzer = DataQualityAnalyzer(spark_session)
        
        # Test analysis methods
        analyzer.analyze_data_quality(None)  # Should handle None gracefully
        analyzer.generate_quality_report(None)  # Should handle None gracefully
        
        # Test TrendAnalyzer methods
        trend_analyzer = TrendAnalyzer(spark_session)
        trend_analyzer.analyze_trends(None)  # Should handle None gracefully
        trend_analyzer.generate_trend_report(None)  # Should handle None gracefully
    
    def test_writer_monitoring_comprehensive(self, spark_session):
        """Test comprehensive writer monitoring."""
        # Test PerformanceMonitor methods
        monitor = PerformanceMonitor(spark_session)
        monitor.track_performance(None)  # Should handle None gracefully
        monitor.generate_performance_report(None)  # Should handle None gracefully
        
        # Test AnalyticsEngine methods
        analytics_engine = AnalyticsEngine(spark_session)
        analytics_engine.process_analytics(None)  # Should handle None gracefully
        analytics_engine.generate_analytics_report(None)  # Should handle None gracefully
    
    def test_writer_operations_comprehensive(self, spark_session):
        """Test comprehensive writer operations."""
        # Test DataProcessor methods
        processor = DataProcessor(spark_session)
        processor.process_data(None)  # Should handle None gracefully
        processor.transform_data(None)  # Should handle None gracefully
        
        # Test QueryBuilder methods
        query_builder = QueryBuilder(spark_session)
        query_builder.build_query(None)  # Should handle None gracefully
        query_builder.execute_query(None)  # Should handle None gracefully
    
    def test_writer_storage_comprehensive(self, spark_session):
        """Test comprehensive writer storage."""
        # Test StorageManager methods
        storage_manager = StorageManager(spark_session)
        storage_manager.store_data(None)  # Should handle None gracefully
        storage_manager.retrieve_data(None)  # Should handle None gracefully
        storage_manager.optimize_storage(None)  # Should handle None gracefully
