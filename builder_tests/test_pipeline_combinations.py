"""
Test various pipeline layer combinations.

This module tests different realistic combinations of Bronze/Silver/Gold layers
to ensure the pipeline system works correctly across different scenarios.
"""

from mock_spark import functions as F


class TestPipelineCombinations:
    """Test different pipeline layer combinations."""

    def test_bronze_only_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with only bronze layer (data validation only)."""
        builder = pipeline_builder

        # Add only bronze step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
                "timestamp": [F.col("timestamp").isNotNull()],
            },
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 1  # Only bronze step
        assert report.metrics.successful_steps == 1
        assert report.metrics.failed_steps == 0

    def test_bronze_to_silver_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with bronze and silver layers."""
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 2  # bronze + silver
        assert report.metrics.successful_steps == 2
        assert report.metrics.failed_steps == 0

    def test_bronze_to_gold_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with bronze and gold layers (skipping silver)."""
        builder = pipeline_builder

        # Add bronze step
        builder.with_bronze_rules(
            name="events",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
            },
        )

        # Add gold step that directly uses bronze data
        builder.add_gold_transform(
            name="event_summary",
            transform=lambda spark, silvers: (
                # Note: This is unusual but possible - gold using bronze directly
                # In practice, you'd typically have a silver step first
                spark.table(f"{test_schema}.events")
            ),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="event_summary",
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 2  # bronze + gold
        assert report.metrics.successful_steps == 2
        assert report.metrics.failed_steps == 0

    def test_full_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test complete pipeline with bronze, silver, and gold layers."""
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        # Add gold step
        builder.add_gold_transform(
            name="event_analytics",
            transform=lambda spark, silvers: silvers["clean_events"],
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="event_analytics",
            source_silvers=["clean_events"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 3  # bronze + silver + gold
        assert report.metrics.successful_steps == 3
        assert report.metrics.failed_steps == 0

    def test_multiple_bronze_steps_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data, simple_users_data
    ):
        """Test pipeline with multiple bronze steps."""
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

        # Add silver step that uses one bronze source
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(
            bronze_sources={"events": simple_events_data, "users": simple_users_data}
        )

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 3  # 2 bronze + 1 silver
        assert report.metrics.successful_steps == 3
        assert report.metrics.failed_steps == 0

    def test_multiple_silver_steps_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with multiple silver steps from same bronze."""
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        builder.add_silver_transform(
            name="processed_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="processed_events",
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 3  # 1 bronze + 2 silver
        assert report.metrics.successful_steps == 3
        assert report.metrics.failed_steps == 0

    def test_multiple_gold_steps_pipeline(
        self, spark_session, pipeline_builder, test_schema, simple_events_data
    ):
        """Test pipeline with multiple gold steps."""
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
            transform=lambda spark, df, silvers: df.select("id", "name", "timestamp", "value"),
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="clean_events",
        )

        # Add multiple gold steps
        builder.add_gold_transform(
            name="event_summary",
            transform=lambda spark, silvers: silvers["clean_events"],
            rules={
                "id": [F.col("id").isNotNull()],
            },
            table_name="event_summary",
            source_silvers=["clean_events"],
        )

        builder.add_gold_transform(
            name="event_analytics",
            transform=lambda spark, silvers: silvers["clean_events"],
            rules={
                "name": [F.col("name").isNotNull()],
            },
            table_name="event_analytics",
            source_silvers=["clean_events"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        report = pipeline.run_initial_load(bronze_sources={"events": simple_events_data})

        # Verify pipeline execution
        assert report.success is True
        assert report.metrics.total_steps == 4  # 1 bronze + 1 silver + 2 gold
        assert report.metrics.successful_steps == 4
        assert report.metrics.failed_steps == 0
