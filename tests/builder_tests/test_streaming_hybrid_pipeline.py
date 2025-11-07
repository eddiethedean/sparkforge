"""
Streaming/Batch Hybrid Pipeline Tests

This module tests a realistic streaming/batch hybrid pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with incremental streaming updates
and batch backfill capabilities.
"""

import os

import pytest
from mock_spark import functions as F

from pipeline_builder.pipeline import PipelineBuilder


class TestStreamingHybridPipeline:
    """Test streaming/batch hybrid pipeline with bronze-silver-gold architecture."""

    @pytest.mark.skipif(
        os.environ.get("SPARK_MODE", "mock").lower() == "mock",
        reason="Polars backend still fails complex datetime validation in this pipeline (mock-spark follow-up).",
    )
    def test_complete_streaming_hybrid_pipeline_execution(
        self, mock_spark_session, data_generator, test_assertions
    ):
        """Test complete streaming/batch hybrid pipeline: batch history + streaming events → unified analytics."""

        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")

        # Create realistic data - batch historical data + streaming events
        batch_history_df = data_generator.create_streaming_batch_history(
            mock_spark_session, num_records=100
        )
        streaming_events_df = data_generator.create_streaming_batch_events(
            mock_spark_session, num_events=80
        )

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=mock_spark_session,
            functions=F,
            schema="bronze",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )

        # Bronze Layer: Raw batch and streaming data validation
        builder.with_bronze_rules(
            name="raw_batch_history",
            rules={
                "event_id": ["not_null"],
                "user_id": ["not_null"],
                "event_timestamp": ["not_null"],
                "event_type": ["not_null"],
            },
            incremental_col="event_timestamp",
        )

        builder.with_bronze_rules(
            name="raw_streaming_events",
            rules={
                "event_id": ["not_null"],
                "user_id": ["not_null"],
                "event_timestamp": ["not_null"],
                "event_type": ["not_null"],
            },
            incremental_col="event_timestamp",
        )

        # Silver Layer: Unified event processing
        def unified_batch_transform(spark, df, silvers):
            """Unify batch events into common schema."""
            return (
                df.withColumn(
                    "event_timestamp_parsed",
                    F.to_timestamp(
                        F.regexp_replace(F.col("event_timestamp"), r"\.\d+", ""),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                )
                .withColumn(
                    "hour_of_day",
                    F.hour(F.col("event_timestamp_parsed")),
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek(F.col("event_timestamp_parsed")),
                )
                .withColumn(
                    "is_purchase",
                    F.when(F.col("event_type") == "purchase", True).otherwise(False),
                )
                .withColumn(
                    "is_mobile",
                    F.when(F.col("device") == "mobile", True).otherwise(False),
                )
                .withColumn(
                    "has_amount",
                    F.col("amount").isNotNull(),
                )
                .withColumn(
                    "event_value",
                    F.coalesce(F.col("amount"), F.lit(0.0)),
                )
                .withColumn(
                    "data_source",
                    F.coalesce(F.col("source"), F.lit("batch")),
                )
                .select(
                    "event_id",
                    "user_id",
                    "event_timestamp_parsed",
                    "hour_of_day",
                    "day_of_week",
                    "event_type",
                    "product_id",
                    "session_id",
                    "event_value",
                    "device",
                    "is_purchase",
                    "is_mobile",
                    "has_amount",
                    "data_source",
                )
            )

        def unified_streaming_transform(spark, df, silvers):
            """Unify streaming events into common schema."""
            return (
                df.withColumn(
                    "event_timestamp_parsed",
                    F.to_timestamp(
                        F.regexp_replace(F.col("event_timestamp"), r"\.\d+", ""),
                        "yyyy-MM-dd'T'HH:mm:ss",
                    ),
                )
                .withColumn(
                    "hour_of_day",
                    F.hour(F.col("event_timestamp_parsed")),
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek(F.col("event_timestamp_parsed")),
                )
                .withColumn(
                    "is_purchase",
                    F.when(F.col("event_type") == "purchase", True).otherwise(False),
                )
                .withColumn(
                    "is_mobile",
                    F.when(F.col("device") == "mobile", True).otherwise(False),
                )
                .withColumn(
                    "has_amount",
                    F.col("amount").isNotNull(),
                )
                .withColumn(
                    "event_value",
                    F.coalesce(F.col("amount"), F.lit(0.0)),
                )
                .withColumn(
                    "data_source",
                    F.lit("streaming"),
                )
                .select(
                    "event_id",
                    "user_id",
                    "event_timestamp_parsed",
                    "hour_of_day",
                    "day_of_week",
                    "event_type",
                    "product_id",
                    "session_id",
                    "event_value",
                    "device",
                    "is_purchase",
                    "is_mobile",
                    "has_amount",
                    "data_source",
                )
            )

        builder.add_silver_transform(
            name="unified_batch_events",
            source_bronze="raw_batch_history",
            transform=unified_batch_transform,
            rules={
                "event_id": ["not_null"],
                "user_id": ["not_null"],
                "event_timestamp_parsed": ["not_null"],
                "event_type": ["not_null"],
                "product_id": ["not_null"],
                "session_id": ["not_null"],
                "data_source": ["not_null"],
                "is_purchase": ["not_null"],
                "is_mobile": ["not_null"],
                "event_value": [["gte", 0]],
            },
            table_name="unified_batch_events",
        )

        builder.add_silver_transform(
            name="unified_streaming_events",
            source_bronze="raw_streaming_events",
            transform=unified_streaming_transform,
            rules={
                "event_id": ["not_null"],
                "user_id": ["not_null"],
                "event_timestamp_parsed": ["not_null"],
                "event_type": ["not_null"],
                "product_id": ["not_null"],
                "session_id": ["not_null"],
                "data_source": ["not_null"],
                "is_purchase": ["not_null"],
                "is_mobile": ["not_null"],
                "event_value": [["gte", 0]],
            },
            table_name="unified_streaming_events",
        )

        # Gold Layer: Unified analytics combining batch and streaming
        def unified_analytics_transform(spark, silvers):
            """Create unified analytics from batch and streaming sources."""
            unified_batch = silvers.get("unified_batch_events")
            unified_streaming = silvers.get("unified_streaming_events")

            if unified_batch is None and unified_streaming is None:
                return spark.createDataFrame(
                    [],
                    [
                        "metric_date",
                        "total_events",
                        "batch_events",
                        "streaming_events",
                        "total_users",
                        "total_purchases",
                        "total_revenue",
                        "mobile_pct",
                    ],
                )

            # Process batch and streaming separately then combine
            batch_metrics = None
            if unified_batch is not None:
                batch_metrics = (
                    unified_batch.withColumn(
                        "metric_date",
                        F.to_date(F.col("event_timestamp_parsed")),
                    )
                    .groupBy("metric_date")
                    .agg(
                        F.count("*").alias("batch_events"),
                        F.countDistinct("user_id").alias("batch_users"),
                        F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias(
                            "batch_purchases"
                        ),
                        F.sum("event_value").alias("batch_revenue"),
                    )
                )

            streaming_metrics = None
            if unified_streaming is not None:
                streaming_metrics = (
                    unified_streaming.withColumn(
                        "metric_date",
                        F.to_date(F.col("event_timestamp_parsed")),
                    )
                    .groupBy("metric_date")
                    .agg(
                        F.count("*").alias("streaming_events"),
                        F.countDistinct("user_id").alias("streaming_users"),
                        F.sum(F.when(F.col("is_purchase"), 1).otherwise(0)).alias(
                            "streaming_purchases"
                        ),
                        F.sum("event_value").alias("streaming_revenue"),
                        F.sum(F.when(F.col("is_mobile"), 1).otherwise(0)).alias(
                            "mobile_events"
                        ),
                    )
                )

            # Combine metrics
            if batch_metrics is not None and streaming_metrics is not None:
                analytics = (
                    batch_metrics.join(streaming_metrics, "metric_date", "full")
                    .withColumn(
                        "total_events",
                        F.coalesce(F.col("batch_events"), F.lit(0))
                        + F.coalesce(F.col("streaming_events"), F.lit(0)),
                    )
                    .withColumn(
                        "total_users",
                        F.coalesce(F.col("batch_users"), F.lit(0))
                        + F.coalesce(F.col("streaming_users"), F.lit(0)),
                    )
                    .withColumn(
                        "total_purchases",
                        F.coalesce(F.col("batch_purchases"), F.lit(0))
                        + F.coalesce(F.col("streaming_purchases"), F.lit(0)),
                    )
                    .withColumn(
                        "total_revenue",
                        F.coalesce(F.col("batch_revenue"), F.lit(0.0))
                        + F.coalesce(F.col("streaming_revenue"), F.lit(0.0)),
                    )
                    .withColumn(
                        "mobile_pct",
                        F.when(
                            F.col("total_events") > 0,
                            (
                                F.coalesce(F.col("mobile_events"), F.lit(0))
                                / F.col("total_events")
                            )
                            * 100,
                        ).otherwise(0),
                    )
                    .withColumn(
                        "batch_events",
                        F.coalesce(F.col("batch_events"), F.lit(0)),
                    )
                    .withColumn(
                        "streaming_events",
                        F.coalesce(F.col("streaming_events"), F.lit(0)),
                    )
                    .select(
                        "metric_date",
                        "total_events",
                        "batch_events",
                        "streaming_events",
                        "total_users",
                        "total_purchases",
                        "total_revenue",
                        "mobile_pct",
                    )
                )
            elif batch_metrics is not None:
                analytics = (
                    batch_metrics.withColumn("total_events", F.col("batch_events"))
                    .withColumn("streaming_events", F.lit(0))
                    .withColumn("total_users", F.col("batch_users"))
                    .withColumn("total_purchases", F.col("batch_purchases"))
                    .withColumn("total_revenue", F.col("batch_revenue"))
                    .withColumn("mobile_pct", F.lit(0))
                    .select(
                        "metric_date",
                        "total_events",
                        "batch_events",
                        "streaming_events",
                        "total_users",
                        "total_purchases",
                        "total_revenue",
                        "mobile_pct",
                    )
                )
            elif streaming_metrics is not None:
                analytics = (
                    streaming_metrics.withColumn(
                        "total_events", F.col("streaming_events")
                    )
                    .withColumn("batch_events", F.lit(0))
                    .withColumn("total_users", F.col("streaming_users"))
                    .withColumn("total_purchases", F.col("streaming_purchases"))
                    .withColumn("total_revenue", F.col("streaming_revenue"))
                    .withColumn(
                        "mobile_pct",
                        F.when(
                            F.col("total_events") > 0,
                            (
                                F.coalesce(F.col("mobile_events"), F.lit(0))
                                / F.col("total_events")
                            )
                            * 100,
                        ).otherwise(0),
                    )
                    .select(
                        "metric_date",
                        "total_events",
                        "batch_events",
                        "streaming_events",
                        "total_users",
                        "total_purchases",
                        "total_revenue",
                        "mobile_pct",
                    )
                )
            else:
                return spark.createDataFrame(
                    [],
                    [
                        "metric_date",
                        "total_events",
                        "batch_events",
                        "streaming_events",
                        "total_users",
                        "total_purchases",
                        "total_revenue",
                        "mobile_pct",
                    ],
                )

            return analytics

        builder.add_gold_transform(
            name="unified_analytics",
            transform=unified_analytics_transform,
            rules={
                "metric_date": ["not_null"],
                "total_events": [["gte", 0]],
            },
            table_name="unified_analytics",
            source_silvers=["unified_batch_events", "unified_streaming_events"],
        )

        def real_time_sessions_transform(spark, silvers):
            """Create real-time session analytics from streaming events."""
            unified_streaming = silvers.get("unified_streaming_events")

            if unified_streaming is None:
                return spark.createDataFrame(
                    [],
                    [
                        "session_id",
                        "user_id",
                        "session_start",
                        "session_end",
                        "event_count",
                        "has_purchase",
                        "total_value",
                        "duration_minutes",
                    ],
                )

            # Session-level aggregations
            sessions = (
                unified_streaming.groupBy("session_id", "user_id")
                .agg(
                    F.min("event_timestamp_parsed").alias("session_start"),
                    F.max("event_timestamp_parsed").alias("session_end"),
                    F.count("*").alias("event_count"),
                    F.max(F.when(F.col("is_purchase"), 1).otherwise(0)).alias(
                        "has_purchase"
                    ),
                    F.sum("event_value").alias("total_value"),
                )
                .withColumn(
                    "duration_minutes",
                    (
                        F.unix_timestamp(F.col("session_end"))
                        - F.unix_timestamp(F.col("session_start"))
                    )
                    / 60.0,
                )
                .select(
                    "session_id",
                    "user_id",
                    "session_start",
                    "session_end",
                    "event_count",
                    "has_purchase",
                    "total_value",
                    "duration_minutes",
                )
            )

            return sessions

        builder.add_gold_transform(
            name="real_time_sessions",
            transform=real_time_sessions_transform,
            rules={
                "session_id": ["not_null"],
                "user_id": ["not_null"],
                "event_count": [["gte", 0]],
            },
            table_name="real_time_sessions",
            source_silvers=["unified_streaming_events"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_batch_history": batch_history_df,
                "raw_streaming_events": streaming_events_df,
            }
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify data quality
        assert result.status.value == "completed" or result.success
        assert "unified_analytics" in result.gold_results
        assert "real_time_sessions" in result.gold_results

        # Verify gold layer outputs
        analytics_result = result.gold_results["unified_analytics"]
        assert analytics_result.get("rows_processed", 0) >= 0

        sessions_result = result.gold_results["real_time_sessions"]
        assert sessions_result.get("rows_processed", 0) >= 0

    def test_incremental_streaming_processing(
        self, mock_spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new streaming events."""
        # Create initial batch data
        batch_initial = data_generator.create_streaming_batch_history(
            mock_spark_session, num_records=50
        )

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=mock_spark_session,
            schema="bronze",
            functions=F,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=False,
        )

        builder.with_bronze_rules(
            name="raw_batch_history",
            rules={
                "event_id": ["not_null"],
                "user_id": ["not_null"],
            },
            incremental_col="event_timestamp",
        )

        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")

        def unified_events_transform(spark, df, silvers):
            return (
                df.withColumn(
                    "event_timestamp_clean",
                    F.regexp_replace(F.col("event_timestamp"), r"\.\d+", ""),
                )
                .withColumn(
                    "event_timestamp_parsed",
                    F.to_timestamp(
                        F.col("event_timestamp_clean"), "yyyy-MM-dd'T'HH:mm:ss"
                    ),
                )
                .drop("event_timestamp_clean")
            )

        builder.add_silver_transform(
            name="unified_batch_events",
            source_bronze="raw_batch_history",
            transform=unified_events_transform,
            rules={"event_id": ["not_null"]},
            table_name="unified_batch_events",
        )

        pipeline = builder.to_pipeline()

        # Initial load
        result1 = pipeline.run_initial_load(
            bronze_sources={"raw_batch_history": batch_initial}
        )

        test_assertions.assert_pipeline_success(result1)

        # Incremental load with new batch records (simulating late-arriving data)
        batch_incremental = data_generator.create_streaming_batch_history(
            mock_spark_session, num_records=30
        )

        result2 = pipeline.run_incremental(
            bronze_sources={"raw_batch_history": batch_incremental}
        )

        test_assertions.assert_pipeline_success(result2)
        assert result2.mode.value == "incremental"
