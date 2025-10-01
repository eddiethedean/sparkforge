"""
Unit tests for pipeline builder using Mock Spark.
"""

import pytest
from sparkforge.pipeline.builder import PipelineBuilder
from sparkforge.models.steps import BronzeStep, SilverStep, GoldStep
from sparkforge.models.enums import PipelinePhase
from mock_spark.errors import AnalysisException, IllegalArgumentException


class TestPipelineBuilder:
    """Test PipelineBuilder with Mock Spark."""
    
    def test_pipeline_builder_initialization(self, mock_spark_session):
        """Test pipeline builder initialization."""
        builder = PipelineBuilder(mock_spark_session)
        assert builder.spark == mock_spark_session
        assert builder.pipeline is None
    
    def test_create_pipeline(self, mock_spark_session):
        """Test creating a pipeline."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        assert isinstance(pipeline, Pipeline)
        assert pipeline.name == "test_pipeline"
        assert pipeline.description == "Test pipeline"
        assert builder.pipeline == pipeline
    
    def test_add_bronze_step(self, mock_spark_session):
        """Test adding a bronze step."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        step = builder.add_bronze_step("bronze_step", "bronze.raw_data")
        
        assert isinstance(step, Step)
        assert step.name == "bronze_step"
        assert step.step_type == StepType.SOURCE
        assert step.data_layer == DataLayer.BRONZE
        assert step.table_name == "bronze.raw_data"
        assert step in pipeline.steps
    
    def test_add_silver_step(self, mock_spark_session):
        """Test adding a silver step."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        step = builder.add_silver_step("silver_step", "silver.processed_data")
        
        assert isinstance(step, Step)
        assert step.name == "silver_step"
        assert step.step_type == StepType.TRANSFORM
        assert step.data_layer == DataLayer.SILVER
        assert step.table_name == "silver.processed_data"
        assert step in pipeline.steps
    
    def test_add_gold_step(self, mock_spark_session):
        """Test adding a gold step."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        step = builder.add_gold_step("gold_step", "gold.aggregated_data")
        
        assert isinstance(step, Step)
        assert step.name == "gold_step"
        assert step.step_type == StepType.TRANSFORM
        assert step.data_layer == DataLayer.GOLD
        assert step.table_name == "gold.aggregated_data"
        assert step in pipeline.steps
    
    def test_validate_schema_exists(self, mock_spark_session):
        """Test schema validation when schema exists."""
        # Create a schema first
        mock_spark_session.storage.create_schema("test_schema")
        
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # This should not raise an exception
        builder._validate_schema("test_schema")
    
    def test_validate_schema_not_exists(self, mock_spark_session):
        """Test schema validation when schema doesn't exist."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # This should raise an exception
        with pytest.raises(AnalysisException):
            builder._validate_schema("nonexistent_schema")
    
    def test_create_schema_if_not_exists(self, mock_spark_session):
        """Test creating schema if it doesn't exist."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # Create schema
        builder._create_schema_if_not_exists("new_schema")
        
        # Verify schema was created
        assert mock_spark_session.storage.schema_exists("new_schema")
    
    def test_create_schema_already_exists(self, mock_spark_session):
        """Test creating schema when it already exists."""
        # Create schema first
        mock_spark_session.storage.create_schema("existing_schema")
        
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # This should not raise an exception
        builder._create_schema_if_not_exists("existing_schema")
        
        # Verify schema still exists
        assert mock_spark_session.storage.schema_exists("existing_schema")
    
    def test_add_step_with_invalid_parameters(self, mock_spark_session):
        """Test adding step with invalid parameters."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # Test with invalid table name
        with pytest.raises(IllegalArgumentException):
            builder.add_bronze_step("", "bronze.raw_data")  # Empty name
        
        with pytest.raises(IllegalArgumentException):
            builder.add_bronze_step("bronze_step", "")  # Empty table name
    
    def test_pipeline_validation(self, mock_spark_session):
        """Test pipeline validation."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # Add steps
        builder.add_bronze_step("bronze_step", "bronze.raw_data")
        builder.add_silver_step("silver_step", "silver.processed_data")
        builder.add_gold_step("gold_step", "gold.aggregated_data")
        
        # Validate pipeline
        validation_result = builder.validate_pipeline()
        assert validation_result is not None
        assert validation_result.passed
    
    def test_pipeline_validation_empty(self, mock_spark_session):
        """Test validation of empty pipeline."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # Validate empty pipeline
        validation_result = builder.validate_pipeline()
        assert validation_result is not None
        assert not validation_result.passed  # Empty pipeline should fail validation
    
    def test_get_pipeline(self, mock_spark_session):
        """Test getting the pipeline."""
        builder = PipelineBuilder(mock_spark_session)
        
        # Initially no pipeline
        assert builder.get_pipeline() is None
        
        # Create pipeline
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        assert builder.get_pipeline() == pipeline
    
    def test_reset_pipeline(self, mock_spark_session):
        """Test resetting the pipeline."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        builder.add_bronze_step("bronze_step", "bronze.raw_data")
        
        # Reset pipeline
        builder.reset_pipeline()
        assert builder.pipeline is None
    
    def test_pipeline_with_multiple_steps(self, mock_spark_session):
        """Test pipeline with multiple steps of different types."""
        builder = PipelineBuilder(mock_spark_session)
        pipeline = builder.create_pipeline("test_pipeline", "Test pipeline")
        
        # Add multiple steps
        bronze_step = builder.add_bronze_step("bronze_step", "bronze.raw_data")
        silver_step = builder.add_silver_step("silver_step", "silver.processed_data")
        gold_step = builder.add_gold_step("gold_step", "gold.aggregated_data")
        
        # Verify all steps are in pipeline
        assert len(pipeline.steps) == 3
        assert bronze_step in pipeline.steps
        assert silver_step in pipeline.steps
        assert gold_step in pipeline.steps
        
        # Verify step order
        assert pipeline.steps[0] == bronze_step
        assert pipeline.steps[1] == silver_step
        assert pipeline.steps[2] == gold_step
