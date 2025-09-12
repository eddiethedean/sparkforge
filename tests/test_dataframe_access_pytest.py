"""
Pytest tests for DataFrame access fix.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class TestDataFrameAccess:
    """Test that Gold transforms receive actual DataFrames, not metadata dictionaries."""

    @pytest.mark.spark
    def test_gold_transform_dataframe_access(self, spark_session, sample_bronze_data, sample_bronze_rules, sample_silver_rules, sample_gold_rules):
        """Test that Gold transforms receive actual DataFrames."""
        from sparkforge import PipelineBuilder
        
        # Track what silvers are received
        received_silvers = {}
        
        def silver_transform(spark, bronze_df):
            return (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .select("user_id", "action", "event_date")
                   )
        
        def gold_transform(spark, silvers):
            # Verify that we receive actual DataFrames
            for name, df in silvers.items():
                assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
                assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
                received_silvers[name] = type(df)
            
            # Use the DataFrame methods
            events_df = silvers.get("silver_events")
            if events_df is not None:
                w = Window.partitionBy("action").orderBy("event_date")
                return (events_df
                        .withColumn("rn", F.row_number().over(w))
                        .filter(F.col("rn") == 1)
                        .select("action", "event_date")
                       )
            else:
                return spark.createDataFrame([], ["action", "event_date"])
        
        # Build pipeline
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="test_bronze",
                rules=sample_bronze_rules,
                incremental_col="timestamp"
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="test_bronze",
                transform=silver_transform,
                rules=sample_silver_rules,
                table_name="silver_events",
                watermark_col="event_date"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=gold_transform,
                rules=sample_gold_rules,
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
        )
        
        # Execute pipeline
        runner = pipeline.to_pipeline()
        
        # This should not raise an AttributeError
        report = runner.initial_load(bronze_sources={"test_bronze": sample_bronze_data})
        
        # Verify results
        assert report.status.name == "COMPLETED"
        assert "silver_events" in received_silvers
        assert received_silvers["silver_events"].__name__ == "DataFrame"

    @pytest.mark.spark
    def test_silver_transform_dataframe_access(self, spark_session, sample_bronze_data, sample_bronze_rules, sample_silver_rules):
        """Test that Silver transforms receive actual DataFrames from Bronze."""
        from sparkforge import PipelineBuilder
        
        # Track what bronze DataFrames are received
        received_bronze = {}
        
        def silver_transform(spark, bronze_df):
            # Verify that we receive actual DataFrames
            assert hasattr(bronze_df, 'withColumn'), f"Expected DataFrame, got {type(bronze_df)}"
            assert hasattr(bronze_df, 'count'), f"Expected DataFrame, got {type(bronze_df)}"
            received_bronze["bronze_df"] = type(bronze_df)
            
            return (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .select("user_id", "action", "event_date")
                   )
        
        # Build pipeline
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="test_bronze",
                rules=sample_bronze_rules,
                incremental_col="timestamp"
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="test_bronze",
                transform=silver_transform,
                rules=sample_silver_rules,
                table_name="silver_events",
                watermark_col="event_date"
            )
        )
        
        # Execute pipeline
        runner = pipeline.to_pipeline()
        
        report = runner.initial_load(bronze_sources={"test_bronze": sample_bronze_data})
        
        # Verify results
        assert report.status.name == "COMPLETED"
        assert "bronze_df" in received_bronze
        assert received_bronze["bronze_df"].__name__ == "DataFrame"

    @pytest.mark.spark
    def test_dataframe_type_validation(self, spark_session, sample_bronze_data, sample_bronze_rules, sample_silver_rules, sample_gold_rules):
        """Test that type annotations match actual data structures."""
        from sparkforge import PipelineBuilder
        
        def silver_transform(spark, bronze_df):
            return (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .select("user_id", "action", "event_date")
                   )
        
        def gold_transform(spark, silvers):
            # Verify that silvers is a dict of DataFrames
            assert isinstance(silvers, dict), f"Expected dict, got {type(silvers)}"
            
            for name, df in silvers.items():
                assert hasattr(df, 'withColumn'), f"Expected DataFrame for {name}, got {type(df)}"
                assert hasattr(df, 'count'), f"Expected DataFrame for {name}, got {type(df)}"
            
            # Return a simple result with all columns needed for validation
            events_df = silvers.get("silver_events")
            if events_df is not None:
                return events_df.select("action", "event_date").distinct()
            else:
                return spark.createDataFrame([], ["action", "event_date"])
        
        # Build pipeline
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="test_bronze",
                rules=sample_bronze_rules,
                incremental_col="timestamp"
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="test_bronze",
                transform=silver_transform,
                rules=sample_silver_rules,
                table_name="silver_events",
                watermark_col="event_date"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=gold_transform,
                rules=sample_gold_rules,
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
        )
        
        # Execute pipeline
        runner = pipeline.to_pipeline()
        
        report = runner.initial_load(bronze_sources={"test_bronze": sample_bronze_data})
        
        # Verify results
        assert report.status.name == "COMPLETED"

    @pytest.mark.spark
    def test_dataframe_method_access_validation(self, spark_session, sample_bronze_data, sample_bronze_rules, sample_silver_rules, sample_gold_rules):
        """Test that various DataFrame methods work on inputs."""
        from sparkforge import PipelineBuilder
        
        def silver_transform(spark, bronze_df):
            # Test various DataFrame methods
            assert hasattr(bronze_df, 'withColumn'), "DataFrame should have withColumn method"
            assert hasattr(bronze_df, 'select'), "DataFrame should have select method"
            assert hasattr(bronze_df, 'filter'), "DataFrame should have filter method"
            assert hasattr(bronze_df, 'count'), "DataFrame should have count method"
            assert hasattr(bronze_df, 'distinct'), "DataFrame should have distinct method"
            
            return (bronze_df
                    .withColumn("event_date", F.to_date("timestamp"))
                    .select("user_id", "action", "event_date")
                   )
        
        def gold_transform(spark, silvers):
            # Test various DataFrame methods on silvers
            for name, df in silvers.items():
                assert hasattr(df, 'withColumn'), f"DataFrame {name} should have withColumn method"
                assert hasattr(df, 'select'), f"DataFrame {name} should have select method"
                assert hasattr(df, 'filter'), f"DataFrame {name} should have filter method"
                assert hasattr(df, 'count'), f"DataFrame {name} should have count method"
                assert hasattr(df, 'distinct'), f"DataFrame {name} should have distinct method"
            
            # Use the methods with all columns needed for validation
            events_df = silvers.get("silver_events")
            if events_df is not None:
                return (events_df
                        .select("action", "event_date")
                        .distinct()
                        .filter(F.col("action").isNotNull())
                       )
            else:
                return spark.createDataFrame([], ["action", "event_date"])
        
        # Build pipeline
        builder = PipelineBuilder(
            spark=spark_session,
            schema="test_schema",
            verbose=False
        )
        
        pipeline = (builder
            .with_bronze_rules(
                name="test_bronze",
                rules=sample_bronze_rules,
                incremental_col="timestamp"
            )
            .add_silver_transform(
                name="silver_events",
                source_bronze="test_bronze",
                transform=silver_transform,
                rules=sample_silver_rules,
                table_name="silver_events",
                watermark_col="event_date"
            )
            .add_gold_transform(
                name="gold_summary",
                transform=gold_transform,
                rules=sample_gold_rules,
                table_name="gold_summary",
                source_silvers=["silver_events"]
            )
        )
        
        # Execute pipeline
        runner = pipeline.to_pipeline()
        
        report = runner.initial_load(bronze_sources={"test_bronze": sample_bronze_data})
        
        # Verify results
        assert report.status.name == "COMPLETED"
