"""
Marketing Analytics Pipeline Tests

This module tests a realistic marketing analytics pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with ad impressions, clicks,
conversions, and campaign performance metrics.
"""

import os

import pytest

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(reason="PySpark-specific tests require SPARK_MODE=real")

from pyspark.sql import functions as F

from pipeline_builder.pipeline import PipelineBuilder


class TestMarketingPipeline:
    """Test marketing analytics pipeline with bronze-silver-gold architecture."""

    def test_complete_marketing_pipeline_execution(
        self, spark_session, data_generator, test_assertions
    ):
        """Test complete marketing pipeline: impressions → clicks → conversions → campaign insights."""

        # Create realistic marketing data
        impressions_df = data_generator.create_marketing_impressions(
            spark_session, num_impressions=150
        )
        clicks_df = data_generator.create_marketing_clicks(spark_session, num_clicks=60)
        conversions_df = data_generator.create_marketing_conversions(
            spark_session, num_conversions=40
        )

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="bronze",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )

        # Bronze Layer: Raw marketing data validation
        builder.with_bronze_rules(
            name="raw_impressions",
            rules={
                "impression_id": ["not_null"],
                "campaign_id": ["not_null"],
                "customer_id": ["not_null"],
                "impression_date": ["not_null"],
            },
            incremental_col="impression_date",
        )

        builder.with_bronze_rules(
            name="raw_clicks",
            rules={
                "click_id": ["not_null"],
                "impression_id": ["not_null"],
                "customer_id": ["not_null"],
                "click_date": ["not_null"],
            },
            incremental_col="click_date",
        )

        builder.with_bronze_rules(
            name="raw_conversions",
            rules={
                "conversion_id": ["not_null"],
                "customer_id": ["not_null"],
                "conversion_date": ["not_null"],
                "conversion_value": [["gte", 0]],
            },
            incremental_col="conversion_date",
        )

        # Silver Layer: Processed marketing data
        def processed_impressions_transform(spark, df, silvers):
            """Process and enrich impression data."""
            return (
                df.withColumn(
                    "impression_date_parsed",
                    F.to_timestamp(
                        F.col("impression_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "hour_of_day",
                    F.hour(F.col("impression_date_parsed")),
                )
                .withColumn(
                    "day_of_week",
                    F.dayofweek(F.col("impression_date_parsed")),
                )
                .withColumn(
                    "is_mobile",
                    F.when(F.col("device_type") == "mobile", True).otherwise(False),
                )
                .select(
                    "impression_id",
                    "campaign_id",
                    "customer_id",
                    "impression_date_parsed",
                    "hour_of_day",
                    "day_of_week",
                    "channel",
                    "ad_id",
                    "cost_per_impression",
                    "device_type",
                    "is_mobile",
                )
            )

        builder.add_silver_transform(
            name="processed_impressions",
            source_bronze="raw_impressions",
            transform=processed_impressions_transform,
            rules={
                "impression_id": ["not_null"],
                "impression_date_parsed": ["not_null"],
                "campaign_id": ["not_null"],
                "customer_id": ["not_null"],
                "channel": ["not_null"],
                "cost_per_impression": [["gte", 0]],
                "hour_of_day": ["not_null"],
                "device_type": ["not_null"],
            },
            table_name="processed_impressions",
        )

        def processed_clicks_transform(spark, df, silvers):
            """Process and enrich click data."""
            return (
                df.withColumn(
                    "click_date_parsed",
                    F.to_timestamp(
                        F.col("click_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "time_to_click_minutes",
                    F.col("time_to_click_seconds") / 60.0,
                )
                .withColumn(
                    "is_quick_click",
                    F.when(F.col("time_to_click_seconds") < 60, True).otherwise(False),
                )
                .select(
                    "click_id",
                    "impression_id",
                    "customer_id",
                    "click_date_parsed",
                    "channel",
                    "time_to_click_seconds",
                    "time_to_click_minutes",
                    "is_quick_click",
                )
            )

        builder.add_silver_transform(
            name="processed_clicks",
            source_bronze="raw_clicks",
            transform=processed_clicks_transform,
            rules={
                "click_id": ["not_null"],
                "impression_id": ["not_null"],
                "customer_id": ["not_null"],
                "click_date_parsed": ["not_null"],
                "channel": ["not_null"],
                "is_quick_click": ["not_null"],
            },
            table_name="processed_clicks",
        )

        def processed_conversions_transform(spark, df, silvers):
            """Process and enrich conversion data."""
            return (
                df.withColumn(
                    "conversion_date_parsed",
                    F.to_timestamp(
                        F.col("conversion_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "is_purchase",
                    F.when(F.col("conversion_type") == "purchase", True).otherwise(
                        False
                    ),
                )
                .withColumn(
                    "is_high_value",
                    F.when(F.col("conversion_value") >= 100, True).otherwise(False),
                )
                .select(
                    "conversion_id",
                    "customer_id",
                    "click_id",
                    "conversion_date_parsed",
                    "conversion_type",
                    "conversion_value",
                    "channel",
                    "is_purchase",
                    "is_high_value",
                )
            )

        builder.add_silver_transform(
            name="processed_conversions",
            source_bronze="raw_conversions",
            transform=processed_conversions_transform,
            rules={
                "conversion_id": ["not_null"],
                "customer_id": ["not_null"],
                "click_id": ["not_null"],
                "conversion_date_parsed": ["not_null"],
                "conversion_value": [["gte", 0]],
                "channel": ["not_null"],
                "is_purchase": ["not_null"],
            },
            table_name="processed_conversions",
        )

        # Gold Layer: Campaign performance metrics
        def campaign_performance_transform(spark, silvers):
            """Calculate campaign performance metrics."""
            processed_impressions = silvers.get("processed_impressions")
            processed_clicks = silvers.get("processed_clicks")
            processed_conversions = silvers.get("processed_conversions")

            if processed_impressions is None:
                return spark.createDataFrame(
                    [],
                    [
                        "campaign_id",
                        "channel",
                        "total_impressions",
                        "total_clicks",
                        "total_conversions",
                        "click_through_rate",
                        "conversion_rate",
                        "total_cost",
                        "total_conversion_value",
                        "roi",
                        "cost_per_conversion",
                    ],
                )

            # Aggregations by campaign and channel
            impression_metrics = processed_impressions.groupBy(
                "campaign_id", "channel"
            ).agg(
                F.count("impression_id").alias("total_impressions"),
                F.sum("cost_per_impression").alias("total_cost"),
            )

            # Need to join clicks with impressions to get campaign_id
            clicks_with_campaign = processed_clicks.join(
                processed_impressions.select("impression_id", "campaign_id"),
                "impression_id",
                "left",
            )

            click_metrics = clicks_with_campaign.groupBy("campaign_id", "channel").agg(
                F.count("click_id").alias("total_clicks")
            )

            # Need to join conversions with clicks and impressions to get campaign_id
            conversions_with_campaign = (
                processed_conversions.join(
                    processed_clicks.select(
                        "click_id", "impression_id", "channel"
                    ).withColumnRenamed("channel", "conv_channel"),
                    "click_id",
                    "left",
                )
                .join(
                    processed_impressions.select("impression_id", "campaign_id"),
                    "impression_id",
                    "left",
                )
                .withColumn(
                    "channel", F.coalesce(F.col("channel"), F.col("conv_channel"))
                )
            )

            conversion_metrics = conversions_with_campaign.groupBy(
                "campaign_id", "channel"
            ).agg(
                F.count("conversion_id").alias("total_conversions"),
                F.sum("conversion_value").alias("total_conversion_value"),
            )

            # Join all metrics
            campaign_metrics = (
                impression_metrics.join(
                    click_metrics, ["campaign_id", "channel"], "left"
                )
                .join(conversion_metrics, ["campaign_id", "channel"], "left")
                .withColumn(
                    "click_through_rate",
                    F.when(
                        F.col("total_impressions") > 0,
                        (
                            F.coalesce(F.col("total_clicks"), F.lit(0))
                            / F.col("total_impressions")
                        )
                        * 100,
                    ).otherwise(0),
                )
                .withColumn(
                    "conversion_rate",
                    F.when(
                        F.coalesce(F.col("total_clicks"), F.lit(0)) > 0,
                        (
                            F.coalesce(F.col("total_conversions"), F.lit(0))
                            / F.col("total_clicks")
                        )
                        * 100,
                    ).otherwise(0),
                )
                .withColumn(
                    "cost_per_conversion",
                    F.when(
                        F.coalesce(F.col("total_conversions"), F.lit(0)) > 0,
                        F.col("total_cost") / F.col("total_conversions"),
                    ).otherwise(None),
                )
                .withColumn(
                    "roi",
                    F.when(
                        F.col("total_cost") > 0,
                        (
                            (
                                F.coalesce(F.col("total_conversion_value"), F.lit(0))
                                - F.col("total_cost")
                            )
                            / F.col("total_cost")
                        )
                        * 100,
                    ).otherwise(None),
                )
                .select(
                    "campaign_id",
                    "channel",
                    "total_impressions",
                    "total_clicks",
                    "total_conversions",
                    "click_through_rate",
                    "conversion_rate",
                    "total_cost",
                    "total_conversion_value",
                    "roi",
                    "cost_per_conversion",
                )
            )

            return campaign_metrics

        builder.add_gold_transform(
            name="campaign_performance",
            transform=campaign_performance_transform,
            rules={
                "campaign_id": ["not_null"],
                "channel": ["not_null"],
                "click_through_rate": [["gte", 0]],
            },
            table_name="campaign_performance",
            source_silvers=[
                "processed_impressions",
                "processed_clicks",
                "processed_conversions",
            ],
        )

        def customer_journey_transform(spark, silvers):
            """Calculate customer journey and attribution metrics."""
            processed_impressions = silvers.get("processed_impressions")
            processed_clicks = silvers.get("processed_clicks")
            processed_conversions = silvers.get("processed_conversions")

            if processed_impressions is None or processed_conversions is None:
                return spark.createDataFrame(
                    [],
                    [
                        "customer_id",
                        "total_touchpoints",
                        "channels_engaged",
                        "days_to_conversion",
                        "total_conversion_value",
                        "first_touch_channel",
                        "last_touch_channel",
                    ],
                )

            # Calculate touchpoint counts per customer
            touchpoint_counts = processed_impressions.groupBy("customer_id").agg(
                F.count("impression_id").alias("total_impressions"),
                F.countDistinct("channel").alias("channels_impressions"),
            )

            click_counts = processed_clicks.groupBy("customer_id").agg(
                F.count("click_id").alias("total_clicks"),
                F.countDistinct("channel").alias("channels_clicks"),
            )

            # Conversion journey - join conversions with impressions on customer_id
            conversion_with_impressions = processed_conversions.select(
                "customer_id", "conversion_date_parsed", "conversion_value"
            ).join(
                processed_impressions.select(
                    "customer_id", "impression_date_parsed", "channel"
                ),
                "customer_id",
                "inner",
            )

            # Join with clicks
            conversion_journey = (
                conversion_with_impressions.join(
                    processed_clicks.select("customer_id", "channel").withColumnRenamed(
                        "channel", "click_channel"
                    ),
                    "customer_id",
                    "left",
                )
                .withColumn(
                    "channel", F.coalesce(F.col("channel"), F.col("click_channel"))
                )
                .drop("click_channel")
                .groupBy("customer_id")
                .agg(
                    F.min("impression_date_parsed").alias("first_touch_date"),
                    F.max("conversion_date_parsed").alias("conversion_date"),
                    F.first("channel").alias("first_touch_channel"),
                    F.last("channel").alias("last_touch_channel"),
                    F.sum("conversion_value").alias("total_conversion_value"),
                )
            )

            # Combine metrics
            journey = (
                touchpoint_counts.join(click_counts, "customer_id", "outer")
                .join(conversion_journey, "customer_id", "inner")
                .withColumn(
                    "total_touchpoints",
                    F.coalesce(F.col("total_impressions"), F.lit(0))
                    + F.coalesce(F.col("total_clicks"), F.lit(0)),
                )
                .withColumn(
                    "channels_engaged",
                    F.greatest(
                        F.coalesce(F.col("channels_impressions"), F.lit(0)),
                        F.coalesce(F.col("channels_clicks"), F.lit(0)),
                    ),
                )
                .withColumn(
                    "days_to_conversion",
                    F.datediff(F.col("conversion_date"), F.col("first_touch_date")),
                )
                .select(
                    "customer_id",
                    "total_touchpoints",
                    "channels_engaged",
                    "days_to_conversion",
                    "total_conversion_value",
                    "first_touch_channel",
                    "last_touch_channel",
                )
            )

            return journey

        builder.add_gold_transform(
            name="customer_journey",
            transform=customer_journey_transform,
            rules={
                "customer_id": ["not_null"],
                "total_touchpoints": [["gte", 0]],
            },
            table_name="customer_journey",
            source_silvers=[
                "processed_impressions",
                "processed_clicks",
                "processed_conversions",
            ],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_impressions": impressions_df,
                "raw_clicks": clicks_df,
                "raw_conversions": conversions_df,
            }
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify data quality
        assert result.status.value == "completed" or result.success
        assert "campaign_performance" in result.gold_results
        assert "customer_journey" in result.gold_results

        # Verify gold layer outputs
        campaign_result = result.gold_results["campaign_performance"]
        assert campaign_result.get("rows_processed", 0) > 0

        journey_result = result.gold_results["customer_journey"]
        assert journey_result.get("rows_processed", 0) >= 0

    def test_incremental_marketing_processing(
        self, spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new marketing data."""
        # Create initial data
        impressions_initial = data_generator.create_marketing_impressions(
            spark_session, num_impressions=50
        )

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=spark_session,
            schema="bronze",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=False,
        )

        builder.with_bronze_rules(
            name="raw_impressions",
            rules={
                "impression_id": ["not_null"],
                "campaign_id": ["not_null"],
            },
            incremental_col="impression_date",
        )

        def processed_impressions_transform(spark, df, silvers):
            return df.withColumn(
                "impression_date_parsed",
                F.to_timestamp(
                    F.col("impression_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                ),
            )

        builder.add_silver_transform(
            name="processed_impressions",
            source_bronze="raw_impressions",
            transform=processed_impressions_transform,
            rules={"impression_id": ["not_null"]},
            table_name="processed_impressions",
        )

        pipeline = builder.to_pipeline()

        # Initial load
        result1 = pipeline.run_initial_load(
            bronze_sources={"raw_impressions": impressions_initial}
        )

        test_assertions.assert_pipeline_success(result1)

        # Incremental load with new impressions
        impressions_incremental = data_generator.create_marketing_impressions(
            spark_session, num_impressions=30
        )

        result2 = pipeline.run_incremental(
            bronze_sources={"raw_impressions": impressions_incremental}
        )

        test_assertions.assert_pipeline_success(result2)
        assert result2.mode.value == "incremental"
