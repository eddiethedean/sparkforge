"""
Tests for multi-schema support functionality.

This module tests the ability to read from and write to different schemas
in SparkForge pipelines, enabling cross-schema data flows.
"""

from unittest.mock import patch

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from sparkforge.errors import StepError
from sparkforge.pipeline.builder import PipelineBuilder


class TestMultiSchemaSupport:
    """Test multi-schema support functionality."""

    @pytest.fixture(autouse=True)
    def setup_test(self, spark_session):
        """Set up test fixtures."""
        self.spark = spark_session
        self.builder = PipelineBuilder(spark=self.spark, schema="default_schema")

        # Create test data
        self.test_data = [
            ("user1", "click", "2024-01-01 10:00:00"),
            ("user2", "view", "2024-01-01 11:00:00"),
            ("user3", "purchase", "2024-01-01 12:00:00"),
        ]

        self.test_schema = StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

        self.test_df = self.spark.createDataFrame(self.test_data, self.test_schema)

    def test_bronze_rules_with_schema(self):
        """Test with_bronze_rules with custom schema."""
        # Create the schema first
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS raw_data")

        # Test with custom schema
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
            schema="raw_data",
        )

        bronze_step = self.builder.bronze_steps["events"]
        assert bronze_step.schema == "raw_data"
        assert bronze_step.name == "events"
        assert bronze_step.incremental_col == "timestamp"

    def test_bronze_rules_without_schema(self):
        """Test with_bronze_rules without schema (uses default)."""
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        bronze_step = self.builder.bronze_steps["events"]
        assert bronze_step.schema is None  # Should use builder's default schema

    def test_silver_rules_with_schema(self):
        """Test with_silver_rules with custom schema."""
        # Create the schema first
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS staging")

        self.builder.with_silver_rules(
            name="existing_clean_events",
            table_name="clean_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            watermark_col="timestamp",
            schema="staging",
        )

        silver_step = self.builder.silver_steps["existing_clean_events"]
        assert silver_step.schema == "staging"
        assert silver_step.table_name == "clean_events"
        assert silver_step.existing is True

    def test_silver_transform_with_schema(self):
        """Test add_silver_transform with custom schema."""
        # Create the schema first
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS processing")

        # Add bronze step first
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def clean_events(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_events",
            transform=clean_events,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events",
            watermark_col="timestamp",
            schema="processing",
        )

        silver_step = self.builder.silver_steps["clean_events"]
        assert silver_step.schema == "processing"
        assert silver_step.table_name == "clean_events"
        assert silver_step.source_bronze == "events"

    def test_gold_transform_with_schema(self):
        """Test add_gold_transform with custom schema."""
        # Create the schema first
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

        # Add bronze and silver steps first
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def clean_events(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_events",
            transform=clean_events,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events",
            watermark_col="timestamp",
        )

        def daily_metrics(spark, silvers):
            return silvers["clean_events"].groupBy("user_id").count()

        self.builder.add_gold_transform(
            name="daily_metrics",
            transform=daily_metrics,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="daily_metrics",
            schema="analytics",
        )

        gold_step = self.builder.gold_steps["daily_metrics"]
        assert gold_step.schema == "analytics"
        assert gold_step.table_name == "daily_metrics"
        assert gold_step.source_silvers == ["clean_events"]

    def test_schema_validation_success(self):
        """Test successful schema validation."""
        with patch.object(self.builder, "_validate_schema") as mock_validate:
            self.builder.with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                schema="valid_schema",
            )
            mock_validate.assert_called_once_with("valid_schema")

    def test_schema_validation_failure(self):
        """Test schema validation failure."""
        with patch.object(self.builder, "_validate_schema") as mock_validate:
            mock_validate.side_effect = StepError(
                "Schema 'invalid_schema' does not exist",
                context={"step_name": "schema_validation", "step_type": "validation"},
            )

            with pytest.raises(
                StepError, match="Schema 'invalid_schema' does not exist"
            ):
                self.builder.with_bronze_rules(
                    name="events",
                    rules={"user_id": [F.col("user_id").isNotNull()]},
                    schema="invalid_schema",
                )

    def test_get_effective_schema(self):
        """Test _get_effective_schema helper method."""
        # Test with custom schema
        effective_schema = self.builder._get_effective_schema("custom_schema")
        assert effective_schema == "custom_schema"

        # Test with None (should use default)
        effective_schema = self.builder._get_effective_schema(None)
        assert effective_schema == "default_schema"

    def test_schema_creation(self):
        """Test schema creation functionality."""
        with patch.object(self.builder.spark, "sql") as mock_sql:
            self.builder._create_schema_if_not_exists("new_schema")
            mock_sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS new_schema")

    def test_schema_creation_failure(self):
        """Test schema creation failure handling."""
        with patch.object(self.builder.spark, "sql") as mock_sql:
            mock_sql.side_effect = Exception("Permission denied")

            with pytest.raises(StepError, match="Failed to create schema 'new_schema'"):
                self.builder._create_schema_if_not_exists("new_schema")

    def test_cross_schema_pipeline(self):
        """Test a complete cross-schema pipeline."""
        # Create all schemas first
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS raw_data")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS processing")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

        # Bronze: Read from raw_data schema
        self.builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
            schema="raw_data",
        )

        # Silver: Write to processing schema
        def clean_events(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_events",
            transform=clean_events,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events",
            watermark_col="timestamp",
            schema="processing",
        )

        # Gold: Write to analytics schema
        def daily_metrics(spark, silvers):
            return silvers["clean_events"].groupBy("user_id").count()

        self.builder.add_gold_transform(
            name="daily_metrics",
            transform=daily_metrics,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="daily_metrics",
            schema="analytics",
        )

        # Verify all steps have correct schemas
        assert self.builder.bronze_steps["raw_events"].schema == "raw_data"
        assert self.builder.silver_steps["clean_events"].schema == "processing"
        assert self.builder.gold_steps["daily_metrics"].schema == "analytics"

    def test_mixed_schema_usage(self):
        """Test mixing schemas and default schema in same pipeline."""
        # Create the schema first
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS raw_data")
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS analytics")

        # Bronze: Use custom schema
        self.builder.with_bronze_rules(
            name="raw_events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            schema="raw_data",
        )

        # Silver: Use default schema (no schema parameter)
        def clean_events(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_events",
            transform=clean_events,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events",
        )

        # Gold: Use custom schema
        def daily_metrics(spark, silvers):
            return silvers["clean_events"].groupBy("user_id").count()

        self.builder.add_gold_transform(
            name="daily_metrics",
            transform=daily_metrics,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="daily_metrics",
            schema="analytics",
        )

        # Verify schemas
        assert self.builder.bronze_steps["raw_events"].schema == "raw_data"
        assert self.builder.silver_steps["clean_events"].schema is None  # Uses default
        assert self.builder.gold_steps["daily_metrics"].schema == "analytics"

    def test_schema_validation_integration(self):
        """Test schema validation integration with step creation."""
        with patch.object(self.builder, "_validate_schema") as mock_validate:
            # Test bronze step
            self.builder.with_bronze_rules(
                name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                schema="test_schema",
            )
            mock_validate.assert_called_with("test_schema")

            # Test silver step
            self.builder.with_silver_rules(
                name="existing_events",
                table_name="events",
                rules={"user_id": [F.col("user_id").isNotNull()]},
                schema="test_schema2",
            )
            mock_validate.assert_called_with("test_schema2")

            # Test add_silver_transform
            def clean_events(spark, bronze_df, prior_silvers):
                return bronze_df

            self.builder.add_silver_transform(
                name="clean_events",
                transform=clean_events,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name="clean_events",
                schema="test_schema3",
            )
            mock_validate.assert_called_with("test_schema3")

            # Test add_gold_transform
            def daily_metrics(spark, silvers):
                return silvers["clean_events"]

            self.builder.add_gold_transform(
                name="daily_metrics",
                transform=daily_metrics,
                rules={"user_id": [F.col("user_id").isNotNull()]},
                table_name="daily_metrics",
                schema="test_schema4",
            )
            mock_validate.assert_called_with("test_schema4")

    def test_backward_compatibility(self):
        """Test that existing code without schema parameters still works."""
        # This should work exactly as before
        self.builder.with_bronze_rules(
            name="events",
            rules={"user_id": [F.col("user_id").isNotNull()]},
            incremental_col="timestamp",
        )

        def clean_events(spark, bronze_df, prior_silvers):
            return bronze_df.filter(F.col("user_id").isNotNull())

        self.builder.add_silver_transform(
            name="clean_events",
            transform=clean_events,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="clean_events",
        )

        def daily_metrics(spark, silvers):
            return silvers["clean_events"].groupBy("user_id").count()

        self.builder.add_gold_transform(
            name="daily_metrics",
            transform=daily_metrics,
            rules={"user_id": [F.col("user_id").isNotNull()]},
            table_name="daily_metrics",
        )

        # All steps should have None schema (use default)
        assert self.builder.bronze_steps["events"].schema is None
        assert self.builder.silver_steps["clean_events"].schema is None
        assert self.builder.gold_steps["daily_metrics"].schema is None
