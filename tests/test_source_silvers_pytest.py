"""
Pytest tests for source_silvers=None functionality.
"""

import pytest
from pyspark.sql import functions as F
from sparkforge.models import GoldStep


class TestSourceSilversNone:
    """Test that source_silvers=None works correctly."""

    @pytest.mark.spark
    def test_goldstep_creation_with_none(self, spark_session):
        """Test that GoldStep can be created with source_silvers=None."""
        def dummy_transform(spark, silvers):
            return spark.createDataFrame([], ["test"])
        
        gold_step = GoldStep(
            name="test_gold",
            transform=dummy_transform,
            rules={"test": [F.col("test").isNotNull()]},
            table_name="test_gold",
            source_silvers=None  # This should be allowed
        )
        
        assert gold_step.source_silvers is None
        gold_step.validate()  # Should not raise an exception

    @pytest.mark.spark
    def test_source_silvers_logic_none(self, spark_session):
        """Test that source_silvers=None uses all available silvers."""
        def dummy_transform(spark, silvers):
            return spark.createDataFrame([], ["test"])
        
        gold_step = GoldStep(
            name="test_gold",
            transform=dummy_transform,
            rules={"test": [F.col("test").isNotNull()]},
            table_name="test_gold",
            source_silvers=None
        )
        
        # Simulate silver_results (what the execution engine returns)
        silver_results = {
            "silver_events": {
                "table_fqn": "test_schema.silver_events",
                "transform": {"input_rows": 3, "output_rows": 3},
                "validation": {"valid_rows": 3, "invalid_rows": 0},
                "write": {"rows_written": 3},
                "skipped": False
            },
            "silver_users": {
                "table_fqn": "test_schema.silver_users", 
                "transform": {"input_rows": 3, "output_rows": 3},
                "validation": {"valid_rows": 3, "invalid_rows": 0},
                "write": {"rows_written": 3},
                "skipped": False
            }
        }
        
        # Test with source_silvers=None (should use all silvers)
        required_silvers = gold_step.source_silvers or list(silver_results.keys())
        assert set(required_silvers) == set(silver_results.keys())

    @pytest.mark.spark
    def test_source_silvers_logic_specific(self, spark_session):
        """Test that source_silvers with specific list works correctly."""
        def dummy_transform(spark, silvers):
            return spark.createDataFrame([], ["test"])
        
        gold_step_specific = GoldStep(
            name="test_gold_specific",
            transform=dummy_transform,
            rules={"test": [F.col("test").isNotNull()]},
            table_name="test_gold_specific",
            source_silvers=["silver_events"]  # Specific list
        )
        
        # Simulate silver_results
        silver_results = {
            "silver_events": {"table_fqn": "test_schema.silver_events"},
            "silver_users": {"table_fqn": "test_schema.silver_users"}
        }
        
        required_silvers_specific = gold_step_specific.source_silvers or list(silver_results.keys())
        assert required_silvers_specific == ["silver_events"]

    @pytest.mark.parametrize("source_silvers", [
        None,  # None should be valid
        [],    # Empty list should be valid
        ["silver1"],  # List with one item should be valid
        ["silver1", "silver2"],  # List with multiple items should be valid
    ])
    @pytest.mark.spark
    def test_goldstep_validation_valid_cases(self, spark_session, source_silvers):
        """Test that GoldStep validation works with valid source_silvers values."""
        def dummy_transform(spark, silvers):
            return spark.createDataFrame([], ["test"])
        
        gold_step = GoldStep(
            name="test_gold",
            transform=dummy_transform,
            rules={"test": [F.col("test").isNotNull()]},
            table_name="test_gold",
            source_silvers=source_silvers
        )
        
        gold_step.validate()  # Should not raise an exception

    @pytest.mark.parametrize("source_silvers", [
        "not_a_list",  # String should be invalid
        123,           # Number should be invalid
        {"not": "list"},  # Dict should be invalid
    ])
    @pytest.mark.spark
    def test_goldstep_validation_invalid_cases(self, spark_session, source_silvers):
        """Test that GoldStep validation fails with invalid source_silvers values."""
        def dummy_transform(spark, silvers):
            return spark.createDataFrame([], ["test"])
        
        with pytest.raises(Exception, match="Source silvers must be a list or None"):
            gold_step = GoldStep(
                name="test_gold_invalid",
                transform=dummy_transform,
                rules={"test": [F.col("test").isNotNull()]},
                table_name="test_gold_invalid",
                source_silvers=source_silvers
            )
            gold_step.validate()
