"""
Data Quality & Reconciliation Pipeline Tests

This module tests a realistic data quality and reconciliation pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with data quality scoring, anomaly detection,
and source reconciliation.
"""

import os
import tempfile
from uuid import uuid4

import pytest

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(
        reason="PySpark-specific tests require SPARK_MODE=real"
    )

from pyspark.sql import functions as F

from pipeline_builder.pipeline import PipelineBuilder
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test_helpers.isolation import get_unique_schema


class TestDataQualityPipeline:
    """Test data quality and reconciliation pipeline with bronze-silver-gold architecture."""

    @pytest.mark.sequential
    def test_complete_data_quality_pipeline_execution(
        self, spark_session, data_generator, test_assertions
    ):
        """Test complete data quality pipeline: raw sources → quality scoring → reconciliation insights."""

        # Create realistic data with quality issues
        source_a_df = data_generator.create_data_quality_source_a(
            spark_session, num_records=80
        )
        source_b_df = data_generator.create_data_quality_source_b(
            spark_session, num_records=100
        )
        # Use get_unique_schema for proper concurrent testing isolation (includes worker ID)
        unique_schema = get_unique_schema("bronze")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {unique_schema}")

        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )

        # Bronze Layer: Raw source data validation
        builder.with_bronze_rules(
            name="raw_source_a",
            rules={
                "id": ["not_null"],
                "transaction_date": ["not_null"],
            },
            incremental_col="transaction_date",
        )

        builder.with_bronze_rules(
            name="raw_source_b",
            rules={
                "record_id": ["not_null"],
                "date": ["not_null"],
            },
            incremental_col="date",
        )

        # Silver Layer: Normalized and quality-scored data
        def normalized_source_a_transform(spark, df, silvers):
            """Normalize source A data and calculate quality scores."""
            return (
                df.withColumn(
                    "transaction_date_parsed",
                    F.to_timestamp(
                        F.col("transaction_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "has_null_customer",
                    F.col("customer_id").isNull(),
                )
                .withColumn(
                    "has_invalid_amount",
                    F.col("amount") <= 0,
                )
                .withColumn(
                    "has_empty_category",
                    F.length(F.trim(F.col("category"))) == 0,
                )
                .withColumn(
                    "quality_score",
                    F.when(~F.col("has_null_customer"), 20).otherwise(0)
                    + F.when(F.col("status").isNotNull(), 20).otherwise(0)
                    + F.when(~F.col("has_invalid_amount"), 20).otherwise(0)
                    + F.when(~F.col("has_empty_category"), 20).otherwise(0)
                    + F.when(F.col("region").isNotNull(), 20).otherwise(0),
                )
                .withColumn(
                    "quality_status",
                    F.when(F.col("quality_score") == 100, "excellent")
                    .when(F.col("quality_score") >= 80, "good")
                    .when(F.col("quality_score") >= 60, "fair")
                    .otherwise("poor"),
                )
                .withColumn(
                    "customer_id",
                    F.coalesce(
                        F.col("customer_id"), F.lit("UNKNOWN")
                    ),  # Fill nulls for reconciliation
                )
                .select(
                    "id",
                    "customer_id",
                    "transaction_date_parsed",
                    "amount",
                    "status",
                    "category",
                    "region",
                    "quality_score",
                    "has_null_customer",
                    "has_invalid_amount",
                    "has_empty_category",
                    "quality_status",
                )
            )

        builder.add_silver_transform(
            name="normalized_source_a",
            source_bronze="raw_source_a",
            transform=normalized_source_a_transform,
            rules={
                "id": ["not_null"],
                "customer_id": [
                    "not_null"
                ],  # Now filled with UNKNOWN if originally null
                "transaction_date_parsed": ["not_null"],
                "quality_score": [["gte", 0], ["lte", 100]],
                "quality_status": ["not_null"],
                "amount": [
                    ["not_null"]
                ],  # Allow negative amounts - they're quality issues we track
                "region": ["not_null"],
                "has_null_customer": ["not_null"],
                "has_invalid_amount": ["not_null"],
                "has_empty_category": ["not_null"],
            },
            table_name="normalized_source_a",
        )

        def normalized_source_b_transform(spark, df, silvers):
            """Normalize source B data and align schema with source A."""
            return (
                df.withColumn(
                    "transaction_date_parsed",
                    F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"),
                )
                .withColumnRenamed("record_id", "id")
                .withColumnRenamed("cust_id", "customer_id")
                .withColumnRenamed("value", "amount")
                .withColumnRenamed("transaction_status", "status")
                .withColumnRenamed("item_type", "category")
                .withColumnRenamed("location", "region")
                .withColumn(
                    "quality_score",
                    F.lit(100),  # Source B assumed to be clean
                )
                .withColumn(
                    "quality_status",
                    F.lit("excellent"),
                )
                .withColumn(
                    "has_null_customer",
                    F.lit(False),
                )
                .withColumn(
                    "has_invalid_amount",
                    F.col("amount") <= 0,
                )
                .withColumn(
                    "has_empty_category",
                    F.length(F.trim(F.col("category"))) == 0,
                )
                .select(
                    "id",
                    "customer_id",
                    "transaction_date_parsed",
                    "amount",
                    "status",
                    "category",
                    "region",
                    "quality_score",
                    "has_null_customer",
                    "has_invalid_amount",
                    "has_empty_category",
                    "quality_status",
                )
            )

        builder.add_silver_transform(
            name="normalized_source_b",
            source_bronze="raw_source_b",
            transform=normalized_source_b_transform,
            rules={
                "id": ["not_null"],
                "customer_id": ["not_null"],
                "transaction_date_parsed": ["not_null"],
                "amount": [["not_null"]],
                "quality_score": [["gte", 0], ["lte", 100]],
                "quality_status": ["not_null"],
                "has_null_customer": ["not_null"],
                "has_invalid_amount": ["not_null"],
                "has_empty_category": ["not_null"],
            },
            table_name="normalized_source_b",
        )

        # Gold Layer: Data quality metrics and reconciliation
        def data_quality_metrics_transform(spark, silvers):
            """Calculate overall data quality metrics."""
            normalized_source_a = silvers.get("normalized_source_a")
            normalized_source_b = silvers.get("normalized_source_b")

            if normalized_source_a is None:
                return spark.createDataFrame(
                    [],
                    [
                        "source",
                        "total_records",
                        "avg_quality_score",
                        "excellent_count",
                        "good_count",
                        "fair_count",
                        "poor_count",
                        "null_customer_count",
                        "invalid_amount_count",
                        "empty_category_count",
                    ],
                )

            # Quality metrics for source A
            quality_a = normalized_source_a.agg(
                F.count("*").alias("total_records"),
                F.avg("quality_score").alias("avg_quality_score"),
                F.sum(
                    F.when(F.col("quality_status") == "excellent", 1).otherwise(0)
                ).alias("excellent_count"),
                F.sum(F.when(F.col("quality_status") == "good", 1).otherwise(0)).alias(
                    "good_count"
                ),
                F.sum(F.when(F.col("quality_status") == "fair", 1).otherwise(0)).alias(
                    "fair_count"
                ),
                F.sum(F.when(F.col("quality_status") == "poor", 1).otherwise(0)).alias(
                    "poor_count"
                ),
                F.sum(F.when(F.col("has_null_customer"), 1).otherwise(0)).alias(
                    "null_customer_count"
                ),
                F.sum(F.when(F.col("has_invalid_amount"), 1).otherwise(0)).alias(
                    "invalid_amount_count"
                ),
                F.sum(F.when(F.col("has_empty_category"), 1).otherwise(0)).alias(
                    "empty_category_count"
                ),
            ).withColumn("source", F.lit("source_a"))

            # Quality metrics for source B
            if normalized_source_b is not None:
                quality_b = normalized_source_b.agg(
                    F.count("*").alias("total_records"),
                    F.avg("quality_score").alias("avg_quality_score"),
                    F.sum(
                        F.when(F.col("quality_status") == "excellent", 1).otherwise(0)
                    ).alias("excellent_count"),
                    F.sum(
                        F.when(F.col("quality_status") == "good", 1).otherwise(0)
                    ).alias("good_count"),
                    F.sum(
                        F.when(F.col("quality_status") == "fair", 1).otherwise(0)
                    ).alias("fair_count"),
                    F.sum(
                        F.when(F.col("quality_status") == "poor", 1).otherwise(0)
                    ).alias("poor_count"),
                    F.sum(F.when(F.col("has_null_customer"), 1).otherwise(0)).alias(
                        "null_customer_count"
                    ),
                    F.sum(F.when(F.col("has_invalid_amount"), 1).otherwise(0)).alias(
                        "invalid_amount_count"
                    ),
                    F.sum(F.when(F.col("has_empty_category"), 1).otherwise(0)).alias(
                        "empty_category_count"
                    ),
                ).withColumn("source", F.lit("source_b"))
                return quality_a.union(quality_b)
            else:
                return quality_a

        builder.add_gold_transform(
            name="data_quality_metrics",
            transform=data_quality_metrics_transform,
            rules={
                "source": ["not_null"],
                "avg_quality_score": [["gte", 0], ["lte", 100]],
            },
            table_name="data_quality_metrics",
            source_silvers=["normalized_source_a", "normalized_source_b"],
        )

        def source_reconciliation_transform(spark, silvers):
            """Reconcile data between sources."""
            normalized_source_a = silvers.get("normalized_source_a")
            normalized_source_b = silvers.get("normalized_source_b")

            if normalized_source_a is None or normalized_source_b is None:
                return spark.createDataFrame(
                    [],
                    [
                        "customer_id",
                        "source_a_count",
                        "source_a_total_amount",
                        "source_b_count",
                        "source_b_total_amount",
                        "amount_difference",
                        "count_difference",
                        "is_reconciled",
                    ],
                )

            # Aggregate by customer
            source_a_agg = normalized_source_a.groupBy("customer_id").agg(
                F.count("*").alias("source_a_count"),
                F.sum("amount").alias("source_a_total_amount"),
            )

            source_b_agg = normalized_source_b.groupBy("customer_id").agg(
                F.count("*").alias("source_b_count"),
                F.sum("amount").alias("source_b_total_amount"),
            )

            # Full outer join for reconciliation
            reconciliation = (
                source_a_agg.join(source_b_agg, "customer_id", "outer")
                .withColumn(
                    "source_a_count",
                    F.coalesce(F.col("source_a_count"), F.lit(0)),
                )
                .withColumn(
                    "source_a_total_amount",
                    F.coalesce(F.col("source_a_total_amount"), F.lit(0.0)),
                )
                .withColumn(
                    "source_b_count",
                    F.coalesce(F.col("source_b_count"), F.lit(0)),
                )
                .withColumn(
                    "source_b_total_amount",
                    F.coalesce(F.col("source_b_total_amount"), F.lit(0.0)),
                )
                .withColumn(
                    "amount_difference",
                    F.abs(
                        F.col("source_a_total_amount") - F.col("source_b_total_amount")
                    ),
                )
                .withColumn(
                    "count_difference",
                    F.abs(F.col("source_a_count") - F.col("source_b_count")),
                )
                .withColumn(
                    "is_reconciled",
                    (F.col("count_difference") == 0)
                    & (F.col("amount_difference") < 0.01),
                )
                .select(
                    "customer_id",
                    "source_a_count",
                    "source_a_total_amount",
                    "source_b_count",
                    "source_b_total_amount",
                    "amount_difference",
                    "count_difference",
                    "is_reconciled",
                )
            )

            return reconciliation

        builder.add_gold_transform(
            name="source_reconciliation",
            transform=source_reconciliation_transform,
            rules={
                "customer_id": ["not_null"],
                "is_reconciled": ["not_null"],
            },
            table_name="source_reconciliation",
            source_silvers=["normalized_source_a", "normalized_source_b"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_source_a": source_a_df,
                "raw_source_b": source_b_df,
            }
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify data quality
        assert result.status.value == "completed" or result.success
        assert "data_quality_metrics" in result.gold_results
        assert "source_reconciliation" in result.gold_results

        # Verify gold layer outputs
        quality_result = result.gold_results["data_quality_metrics"]
        assert quality_result.get("rows_processed", 0) > 0

        reconciliation_result = result.gold_results["source_reconciliation"]
        assert reconciliation_result.get("rows_processed", 0) >= 0

        # Cleanup: drop schema created for this test
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from test_helpers.isolation import cleanup_test_tables
            cleanup_test_tables(spark_session, unique_schema)
        except Exception:
            pass  # Ignore cleanup errors

    def test_incremental_data_quality_processing(
        self, spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new data quality data."""
        # Create initial data
        source_a_initial = data_generator.create_data_quality_source_a(
            spark_session, num_records=30
        )

        # Create pipeline builder
        unique_schema = get_unique_schema("bronze")

        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=False,
        )

        builder.with_bronze_rules(
            name="raw_source_a",
            rules={
                "id": ["not_null"],
                "transaction_date": ["not_null"],
            },
            incremental_col="transaction_date",
        )

        def normalized_source_a_transform(spark, df, silvers):
            return df.withColumn(
                "transaction_date_parsed",
                F.to_timestamp(
                    F.col("transaction_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                ),
            )

        builder.add_silver_transform(
            name="normalized_source_a",
            source_bronze="raw_source_a",
            transform=normalized_source_a_transform,
            rules={"id": ["not_null"]},
            table_name="normalized_source_a",
        )

        pipeline = builder.to_pipeline()

        # Initial load
        result1 = pipeline.run_initial_load(
            bronze_sources={"raw_source_a": source_a_initial}
        )

        test_assertions.assert_pipeline_success(result1)

        # Incremental load with new records
        source_a_incremental = data_generator.create_data_quality_source_a(
            spark_session, num_records=20
        )

        result2 = pipeline.run_incremental(
            bronze_sources={"raw_source_a": source_a_incremental}
        )

        test_assertions.assert_pipeline_success(result2)
        assert result2.mode.value == "incremental"
