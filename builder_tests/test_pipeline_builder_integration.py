"""
Test pipeline builder integration with realistic scenarios.

This module tests the PipelineBuilder API with simple, focused test cases
that verify the correct usage patterns and integration between components.
"""

from mock_spark import functions as F


class TestPipelineBuilderIntegration:
    """Test PipelineBuilder integration with realistic scenarios."""

    def test_simple_bronze_silver_gold_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test basic 3-layer pipeline: Bronze -> Silver -> Gold."""
        builder = pipeline_builder

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
        )

        # Add silver step
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
                "value": [F.col("value").isNotNull()],
            },
            table_name="clean_events",
        )

        # Add gold step
        builder.add_gold_transform(
            name="event_summary",
            transform=lambda spark, silvers: silvers["clean_events"],
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            table_name="event_summary",
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 3  # bronze + silver + gold

        # Verify pipeline execution metrics
        assert report.metrics.successful_steps == 3  # All steps successful
        assert report.metrics.failed_steps == 0  # No failed steps
        assert report.metrics.total_rows_processed == 15  # 5 bronze + 5 silver + 5 gold

    def test_pipeline_with_incremental_load(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with incremental load."""
        builder = pipeline_builder

        # Add bronze step with incremental column
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
            incremental_col="timestamp",
        )

        # Add silver step
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        # Build pipeline
        pipeline = builder.to_pipeline()

        # Run initial load
        initial_report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )
        assert initial_report.success is True

        # Create incremental data
        from datetime import datetime

        incremental_data = spark_session.createDataFrame(
            [
                {
                    "id": 6,
                    "name": "event6",
                    "timestamp": datetime(2024, 1, 2, 10, 0, 0),
                    "value": 600,
                }
            ],
            ["id", "name", "timestamp", "value"],
        )

        # Run incremental load
        incremental_report = pipeline.run_incremental(
            bronze_sources={"events": incremental_data}
        )
        assert incremental_report.success is True

        # Verify incremental data was added
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        assert clean_events_df.count() == 6  # 5 initial + 1 incremental

    def test_multiple_bronze_sources(
        self,
        spark_session,
        pipeline_builder,
        test_schema,
        simple_events_data,
        simple_users_data,
    ):
        """Test pipeline with multiple bronze sources."""
        builder = pipeline_builder

        # Add multiple bronze steps
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        builder.with_bronze_rules(
            name="users",
            rules={
                "user_id": [F.col("user_id").isNotNull()],
                "username": [F.col("username").isNotNull()],
            },
        )

        # Add silver step that uses both bronze sources
        builder.add_silver_transform(
            name="enriched_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="enriched_events",
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data, "users": simple_users_data}
        )

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 3  # 2 bronze + 1 silver

        # Verify tables were created
        enriched_events_df = spark_session.table(f"{test_schema}.enriched_events")
        assert enriched_events_df.count() == 5  # All events enriched
        assert "enriched" in enriched_events_df.columns

    def test_multiple_silver_transforms(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with multiple silver transforms from same bronze."""
        builder = pipeline_builder

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        # Add multiple silver steps
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="processed_events",
        )

        # Add gold step that uses both silver tables
        builder.add_gold_transform(
            name="event_analytics",
            transform=lambda spark, silvers: (
                silvers["clean_events"]
                .union(silvers["processed_events"])
                .groupBy("id")
                .count()
            ),
            rules={
                "id": [F.col("id").isNotNull()],
                "count": [F.col("count") > 0],
            },
            table_name="event_analytics",
            source_silvers=["clean_events", "processed_events"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data}
        )

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 4  # 1 bronze + 2 silver + 1 gold

        # Verify all tables were created
        clean_events_df = spark_session.table(f"{test_schema}.clean_events")
        processed_events_df = spark_session.table(f"{test_schema}.processed_events")
        analytics_df = spark_session.table(f"{test_schema}.event_analytics")

        assert clean_events_df.count() == 5
        assert processed_events_df.count() == 5
        assert analytics_df.count() == 5  # Union of both silver tables

    def test_pipeline_validation(
        self, spark_session, pipeline_builder, simple_events_data
    ):
        """Test pipeline validation functionality."""
        builder = pipeline_builder

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        # Add silver step
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select(
                "id", "name", "timestamp", "value"
            ),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        # Validate pipeline
        errors = builder.validate_pipeline()
        assert len(errors) == 0  # Should have no validation errors

        # Build pipeline
        pipeline = builder.to_pipeline()
        assert pipeline is not None

        # Test that pipeline has expected methods
        assert hasattr(pipeline, "run_initial_load")
        assert hasattr(pipeline, "run_incremental")
        assert hasattr(pipeline, "run_pipeline")
