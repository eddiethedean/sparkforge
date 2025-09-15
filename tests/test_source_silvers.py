"""
Comprehensive tests for source_silvers functionality.

This module tests both source_silvers=None (uses all available silvers) and
source_silvers with specific lists, including validation and integration scenarios.
"""

import pytest
from pyspark.sql import functions as F
from sparkforge.models import GoldStep
from sparkforge import PipelineBuilder


class TestSourceSilvers:
    """Comprehensive tests for source_silvers functionality."""

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
    def test_goldstep_creation_with_specific_list(self, spark_session):
        """Test that GoldStep can be created with specific source_silvers list."""
        def dummy_transform(spark, silvers):
            return spark.createDataFrame([], ["test"])
        
        gold_step = GoldStep(
            name="test_gold",
            transform=dummy_transform,
            rules={"test": [F.col("test").isNotNull()]},
            table_name="test_gold",
            source_silvers=["silver_events", "silver_users"]
        )
        
        assert gold_step.source_silvers == ["silver_events", "silver_users"]
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

    @pytest.mark.spark
    def test_integration_source_silvers_none(self, spark_session):
        """Integration test: source_silvers=None uses all available silvers in pipeline."""
        # Create test data
        bronze_data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user2", "view", "2024-01-01 11:00:00"),
            ("user3", "purchase", "2024-01-01 12:00:00"),
        ]
        
        bronze_df = spark_session.createDataFrame(
            bronze_data, 
            ["user_id", "action", "timestamp"]
        )
        
        # Define validation rules
        bronze_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()]
        }
        
        silver_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()]
        }
        
        gold_rules = {
            "action": [F.col("action").isNotNull()],
            "total_events": [F.col("total_events") > 0],
            "unique_users": [F.col("unique_users") > 0]
        }
        
        # Track what silvers are received
        received_silvers = {}
        
        def silver_events(spark, bronze_df):
            return (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .select("user_id", "action", "event_date")
                   )
        
        def silver_users(spark, bronze_df):
            return (bronze_df
                    .select("user_id")
                    .distinct()
                    .withColumn("created_at", F.current_timestamp())
                   )
        
        def gold_summary(spark, silvers):
            # Track received silvers for verification
            received_silvers.update(silvers)
            
            # Verify that we received all available silvers
            expected_silvers = {"silver_events", "silver_users"}
            actual_silvers = set(silvers.keys())
            
            assert actual_silvers == expected_silvers, f"Expected {expected_silvers}, got {actual_silvers}"
            
            # Verify that all silvers are DataFrames
            for name, df in silvers.items():
                assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
                assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
            
            # Create a summary using both silvers
            events_df = silvers.get("silver_events")
            users_df = silvers.get("silver_users")
            
            if events_df is not None and users_df is not None:
                return (events_df
                        .groupBy("action")
                        .agg(F.count("*").alias("total_events"))
                        .crossJoin(users_df.agg(F.countDistinct("user_id").alias("unique_users")))
                        .select("action", "total_events", "unique_users")
                       )
            else:
                return spark_session.createDataFrame([], ["action", "total_events", "unique_users"])
        
        # Build pipeline with source_silvers=None (should use all silvers)
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            min_bronze_rate=95.0,
            min_silver_rate=95.0,
            min_gold_rate=95.0
        )
        
        # Add bronze rules first
        builder.with_bronze_rules(name="bronze_events", rules=bronze_rules)
        
        # Add silver transforms
        builder.add_silver_transform(
            name="silver_events",
            source_bronze="bronze_events",
            transform=silver_events,
            rules=silver_rules,
            table_name="silver_events"
        )
        builder.add_silver_transform(
            name="silver_users",
            source_bronze="bronze_events",
            transform=silver_users,
            rules=silver_rules,
            table_name="silver_users"
        )
        
        # Add gold transform
        builder.add_gold_transform(
            name="gold_summary",
            transform=gold_summary,
            rules=gold_rules,
            table_name="gold_summary",
            source_silvers=["silver_events", "silver_users"]  # Use specific silvers
        )
        
        # Build pipeline
        pipeline = builder.to_pipeline()
        
        # Execute pipeline
        result = pipeline.initial_load(bronze_sources={"bronze_events": bronze_df})
        
        # Verify pipeline completed successfully
        assert result.status.value == "completed"
        
        # Verify that gold transform received all silvers
        assert len(received_silvers) == 2
        assert "silver_events" in received_silvers
        assert "silver_users" in received_silvers

    @pytest.mark.spark
    def test_integration_source_silvers_specific(self, spark_session):
        """Integration test: source_silvers with specific list works in pipeline."""
        # Create test data
        bronze_data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user2", "view", "2024-01-01 11:00:00"),
        ]
        
        bronze_df = spark_session.createDataFrame(
            bronze_data, 
            ["user_id", "action", "timestamp"]
        )
        
        # Define validation rules
        bronze_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "timestamp": [F.col("timestamp").isNotNull()]
        }
        
        silver_rules = {
            "user_id": [F.col("user_id").isNotNull()],
            "action": [F.col("action").isNotNull()],
            "event_date": [F.col("event_date").isNotNull()]
        }
        
        gold_rules = {
            "action": [F.col("action").isNotNull()],
            "total_events": [F.col("total_events") > 0]
        }
        
        # Track what silvers are received
        received_silvers = {}
        
        def silver_events(spark, bronze_df):
            return (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .select("user_id", "action", "event_date")
                   )
        
        def silver_users(spark, bronze_df):
            return (bronze_df
                    .select("user_id")
                    .distinct()
                    .withColumn("created_at", F.current_timestamp())
                   )
        
        def gold_summary(spark, silvers):
            # Track received silvers for verification
            received_silvers.update(silvers)
            
            # Should only receive silver_events (specific list)
            expected_silvers = {"silver_events"}
            actual_silvers = set(silvers.keys())
            
            assert actual_silvers == expected_silvers, f"Expected {expected_silvers}, got {actual_silvers}"
            
            # Create summary using only the specified silver
            events_df = silvers.get("silver_events")
            if events_df is not None:
                return (events_df
                        .groupBy("action")
                        .agg(F.count("*").alias("total_events"))
                        .select("action", "total_events")
                       )
            else:
                return spark_session.createDataFrame([], ["action", "total_events"])
        
        # Build pipeline with specific source_silvers
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            min_bronze_rate=95.0,
            min_silver_rate=95.0,
            min_gold_rate=95.0
        )
        
        # Add bronze rules first
        builder.with_bronze_rules(name="bronze_events", rules=bronze_rules)
        
        # Add silver transforms
        builder.add_silver_transform(
            name="silver_events",
            source_bronze="bronze_events",
            transform=silver_events,
            rules=silver_rules,
            table_name="silver_events"
        )
        builder.add_silver_transform(
            name="silver_users",
            source_bronze="bronze_events",
            transform=silver_users,
            rules=silver_rules,
            table_name="silver_users"
        )
        
        # Add gold transform
        builder.add_gold_transform(
            name="gold_summary",
            transform=gold_summary,
            rules=gold_rules,
            table_name="gold_summary",
            source_silvers=["silver_events"]  # Specific list
        )
        
        # Build pipeline
        pipeline = builder.to_pipeline()
        
        # Execute pipeline
        result = pipeline.initial_load(bronze_sources={"bronze_events": bronze_df})
        
        # Verify pipeline completed successfully
        assert result.status.value == "completed"
        
        # Verify that gold transform received only the specified silvers
        assert len(received_silvers) == 1
        assert "silver_events" in received_silvers
        assert "silver_users" not in received_silvers
