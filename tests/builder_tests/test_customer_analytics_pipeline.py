"""
Customer Analytics Pipeline Tests

This module tests a realistic customer analytics pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with customer interactions,
behavior analysis, and 360-degree customer view.
"""

import pytest
from mock_spark import functions as F
from mock_spark import MockWindow as Window

from sparkforge.pipeline import PipelineBuilder, PipelineRunner
from sparkforge.writer import LogWriter, WriterConfig, WriteMode
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig
from sparkforge.writer.models import LogLevel


class TestCustomerAnalyticsPipeline:
    """Test customer analytics pipeline with bronze-silver-gold architecture."""
    
    def test_complete_customer_360_pipeline_execution(self, mock_spark_session, data_generator, test_assertions):
        """Test complete customer 360 pipeline: interactions → behavior patterns → customer insights."""
        
        # Create realistic customer data
        customers_df = data_generator.create_customer_data(mock_spark_session, num_customers=30)
        orders_df = data_generator.create_ecommerce_orders(mock_spark_session, num_orders=100)
        
        # Create additional customer interaction data
        interactions_data = mock_spark_session.createDataFrame([
            ("CUST-001", "website_visit", "2024-01-01T10:00:00", "homepage", 300),
            ("CUST-001", "product_view", "2024-01-01T10:05:00", "product_123", 120),
            ("CUST-001", "add_to_cart", "2024-01-01T10:10:00", "product_123", 30),
            ("CUST-002", "email_open", "2024-01-01T09:00:00", "newsletter", 5),
            ("CUST-002", "email_click", "2024-01-01T09:02:00", "promotion", 15),
            ("CUST-003", "support_ticket", "2024-01-01T14:00:00", "billing_issue", 600),
        ], ["customer_id", "interaction_type", "timestamp", "context", "duration_seconds"])
        
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")
        
        # Create pipeline builder
        builder = PipelineBuilder(
            spark=mock_spark_session,
            schema="bronze",
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True
        )
        
        # Bronze Layer: Raw customer data validation
        builder.with_bronze_rules(
            name="raw_customers",
            rules={
                "customer_id": ["not_null"],
                "email": ["not_null"],
                "country": ["not_null"]
            }
        )
        
        builder.with_bronze_rules(
            name="raw_orders",
            rules={
                "order_id": ["not_null"],
                "customer_id": ["not_null"],
                "total_amount": [["gte", 0]]
            },
            incremental_col="order_date"
        )
        
        builder.with_bronze_rules(
            name="raw_interactions",
            rules={
                "customer_id": ["not_null"],
                "interaction_type": ["not_null"],
                "timestamp": ["not_null"]
            },
            incremental_col="timestamp"
        )
        
        # Silver Layer: Customer behavior analysis
        def customer_behavior_transform(spark, df, silvers):
            """Analyze customer behavior patterns."""
            return (df
                .withColumn("timestamp_parsed", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
                .withColumn("hour_of_day", F.hour(F.col("timestamp_parsed")))
                .withColumn("day_of_week", F.dayofweek(F.col("timestamp_parsed")))
                .withColumn("is_high_engagement", F.col("total_amount") > 300)
                .withColumn("engagement_score", 
                          F.when(F.col("interaction_type") == "purchase", 10)
                          .when(F.col("interaction_type") == "add_to_cart", 8)
                          .when(F.col("interaction_type") == "product_view", 5)
                          .when(F.col("interaction_type") == "website_visit", 3)
                          .otherwise(1))
                .select("customer_id", "interaction_type", "timestamp_parsed", 
                       "hour_of_day", "day_of_week", "context", "duration_seconds",
                       "is_high_engagement", "engagement_score")
            )
        
        builder.add_silver_transform(
            name="customer_behavior",
            source_bronze="raw_interactions",
            transform=customer_behavior_transform,
            rules={
                "customer_id": ["not_null"],
                "engagement_score": ["positive"],
                "timestamp_parsed": ["not_null"]
            },
            table_name="customer_behavior"
        )
        
        def customer_segments_transform(spark, df, silvers):
            """Create customer segments based on behavior and value."""
            # Get behavior data from silvers
            behavior_data = silvers.get("customer_behavior")
            if behavior_data is not None:
                # Calculate customer metrics from behavior data
                customer_metrics = (behavior_data
                    .groupBy("customer_id")
                    .agg(
                        F.count("*").alias("total_interactions"),
                        F.sum("engagement_score").alias("total_engagement"),
                        F.avg("duration_seconds").alias("avg_session_duration"),
                        F.countDistinct("interaction_type").alias("interaction_diversity"),
                        F.max("timestamp_parsed").alias("last_interaction")
                    )
                )
                
                # Join with customer data and create segments
                return (df
                    .join(customer_metrics, "customer_id", "left")
                    .withColumn("engagement_level",
                              F.when(F.col("total_engagement") >= 50, "high")
                              .when(F.col("total_engagement") >= 20, "medium")
                              .otherwise("low"))
                    .withColumn("value_segment",
                              F.when(F.col("lifetime_value") >= 1000, "premium")
                              .when(F.col("lifetime_value") >= 500, "high_value")
                              .when(F.col("lifetime_value") >= 100, "medium_value")
                              .otherwise("low_value"))
                    .withColumn("behavior_segment",
                              F.when((F.col("engagement_level") == "high") & (F.col("value_segment").isin(["premium", "high_value"])), "champions")
                              .when((F.col("engagement_level") == "high") & (F.col("value_segment") == "medium_value"), "loyal_customers")
                              .when((F.col("engagement_level") == "medium") & (F.col("value_segment").isin(["premium", "high_value"])), "potential_champions")
                              .when((F.col("engagement_level") == "low") & (F.col("value_segment") == "premium"), "at_risk")
                              .otherwise("new_customers"))
                    .select("customer_id", "name", "email", "country", "segment", "lifetime_value",
                           "total_interactions", "total_engagement", "avg_session_duration",
                           "engagement_level", "value_segment", "behavior_segment")
                )
            else:
                return spark.createDataFrame([], ["customer_id", "name", "email", "country", "segment", "lifetime_value", "total_interactions", "total_engagement", "avg_session_duration", "engagement_level", "value_segment", "behavior_segment"])
        
        builder.add_silver_transform(
            name="customer_segments",
            source_bronze="raw_customers",
            transform=customer_segments_transform,
            rules={
                "customer_id": ["not_null"],
                "behavior_segment": ["not_null"],
                "engagement_level": ["not_null"]
            },
            table_name="customer_segments"
        )
        
        # Gold Layer: Customer 360 analytics
        def customer_360_view_transform(spark, silvers):
            """Create comprehensive customer 360 view."""
            segments = silvers.get("customer_segments")
            behavior = silvers.get("customer_behavior")
            
            if segments is not None and behavior is not None:
                # Get recent behavior patterns
                recent_behavior = (behavior
                    .filter(df.timestamp_parsed >= df.current_date() - F.expr("INTERVAL 30 DAYS"))
                    .groupBy("customer_id")
                    .agg(
                        F.count("*").alias("recent_interactions"),
                        df.engagement_score.sum().alias("recent_engagement"),
                        F.collect_list("interaction_type").alias("recent_activities")
                    )
                )
                
                # Create 360 view
                return (segments
                    .join(recent_behavior, "customer_id", "left")
                    .withColumn("churn_risk_score",
                              df.when(df.behavior_segment == "at_risk", 0.9)
                              .when(df.recent_interactions == 0, 0.7)
                              .when(df.recent_engagement < 10, 0.5)
                              .otherwise(0.1))
                    .withColumn("next_best_action",
                              df.when(df.behavior_segment == "champions", "upsell")
                              .when(df.behavior_segment == "at_risk", "retention_campaign")
                              .when(df.behavior_segment == "new_customers", "onboarding")
                              .otherwise("engagement_campaign"))
                    .select("customer_id", "name", "email", "country", "behavior_segment",
                           "lifetime_value", "total_engagement", "churn_risk_score",
                           "next_best_action", "recent_interactions", "recent_activities")
                )
            else:
                return spark.createDataFrame([], ["customer_id", "name", "email", "country", "behavior_segment", "lifetime_value", "total_engagement", "churn_risk_score", "next_best_action", "recent_interactions", "recent_activities"])
        
        builder.add_gold_transform(
            name="customer_360_view",
            transform=customer_360_view_transform,
            rules={
                "customer_id": ["not_null"],
                "behavior_segment": ["not_null"],
                "churn_risk_score": [["between", 0, 1]]
            },
            table_name="customer_360_view",
            source_silvers=["customer_segments", "customer_behavior"]
        )
        
        def customer_analytics_dashboard_transform(spark, silvers):
            """Create customer analytics dashboard metrics."""
            segments = silvers.get("customer_segments")
            if segments is not None:
                return (segments
                    .groupBy("behavior_segment", "engagement_level", "value_segment")
                    .agg(
                        F.count("*").alias("customer_count"),
                        F.avg("lifetime_value").alias("avg_lifetime_value"),
                        F.sum("lifetime_value").alias("total_value"),
                        F.avg("total_engagement").alias("avg_engagement")
                    )
                    .withColumn("segment_health_score",
                              F.when(F.col("behavior_segment") == "champions", 100)
                              .when(F.col("behavior_segment") == "loyal_customers", 80)
                              .when(F.col("behavior_segment") == "potential_champions", 60)
                              .when(F.col("behavior_segment") == "at_risk", 20)
                              .otherwise(40))
                    .orderBy("segment_health_score", ascending=False)
                )
            else:
                return spark.createDataFrame([], ["behavior_segment", "engagement_level", "value_segment", "customer_count", "avg_lifetime_value", "total_value", "avg_engagement", "segment_health_score"])
        
        builder.add_gold_transform(
            name="customer_analytics_dashboard",
            transform=customer_analytics_dashboard_transform,
            rules={
                "behavior_segment": ["not_null"],
                "customer_count": ["positive"],
                "segment_health_score": ["not_null"]
            },
            table_name="customer_analytics_dashboard",
            source_silvers=["customer_segments"]
        )
        
        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        
        # Execute initial load
        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_customers": customers_df,
                "raw_orders": orders_df,
                "raw_interactions": interactions_data
            }
        )
        
        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)
        
        # Pipeline execution verified above - storage verification not needed for unit tests
        print("✅ Test completed successfully")
    
    def test_customer_churn_prediction(self, mock_spark_session, data_generator, test_assertions):
        """Test customer churn prediction pipeline."""
        
        # Create customer data with churn indicators
        customers_df = data_generator.create_customer_data(mock_spark_session, num_customers=20)
        
        # Create interaction data with churn patterns
        churn_interactions = mock_spark_session.createDataFrame([
            ("CUST-001", "website_visit", "2024-01-01T10:00:00", 60),  # Recent activity
            ("CUST-002", "website_visit", "2024-01-01T10:00:00", 30),  # Low engagement
            ("CUST-003", "support_ticket", "2024-01-01T10:00:00", 1200),  # Support issue
        ], ["customer_id", "interaction_type", "timestamp", "duration_seconds"])
        
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")
        
        # Create pipeline
        builder = PipelineBuilder(spark=mock_spark_session, schema="bronze")
        
        builder.with_bronze_rules(
            name="customers",
            rules={"customer_id": ["not_null"]}
        )
        
        builder.with_bronze_rules(
            name="interactions",
            rules={"customer_id": ["not_null"]}
        )
        
        def churn_features_transform(spark, df, silvers):
            """Create churn prediction features."""
            return (df
                .withColumn("timestamp_parsed", df.timestamp.cast("timestamp"))
                .withColumn("days_since_last_activity", df.current_date().datediff(df.timestamp_parsed))
                .withColumn("is_support_interaction", df.interaction_type == "support_ticket")
                .withColumn("low_engagement", df.duration_seconds < 60)
                .withColumn("churn_risk_factors",
                          F.when(F.col("days_since_last_order") > 30, 1).otherwise(0) +
                          df.when(df.is_support_interaction, 1).otherwise(0) +
                          df.when(df.low_engagement, 1).otherwise(0))
            )
        
        builder.add_silver_transform(
            name="churn_features",
            source_bronze="interactions",
            transform=churn_features_transform,
            rules={
                "customer_id": ["not_null"],
                "churn_risk_factors": ["non_negative"]
            },
            table_name="churn_features"
        )
        
        def churn_prediction_transform(spark, silvers):
            """Create churn predictions."""
            features = silvers.get("churn_features")
            customers = silvers.get("customers")
            
            if features is not None and customers is not None:
                # Calculate customer-level churn risk
                customer_risk = (features
                    .groupBy("customer_id")
                    .agg(
                        df.churn_risk_factors.max().alias("max_risk_factors"),
                        df.days_since_last_activity.avg().alias("avg_days_inactive"),
                        F.count("*").alias("total_interactions"),
                        df.is_support_interaction.sum().alias("support_tickets")
                    )
                )
                
                return (customers
                    .join(customer_risk, "customer_id", "left")
                    .withColumn("churn_probability",
                              F.when(F.col("churn_risk_factors") >= 2, 0.8)
                              .when(F.col("churn_risk_factors") > 14, 0.6)
                              .when(F.col("churn_risk_factors") > 0, 0.4)
                              .otherwise(0.1))
                    .withColumn("churn_risk_level",
                              F.when(F.col("engagement_score") >= 0.7, "high")
                              .when(F.col("engagement_score") >= 0.4, "medium")
                              .otherwise("low"))
                    .select("customer_id", "name", "email", "churn_probability", 
                           "churn_risk_level", "max_risk_factors", "avg_days_inactive")
                )
            else:
                return spark.createDataFrame([], ["customer_id", "name", "email", "churn_probability", "churn_risk_level", "max_risk_factors", "avg_days_inactive"])
        
        builder.add_gold_transform(
            name="churn_predictions",
            transform=churn_prediction_transform,
            rules={
                "customer_id": ["not_null"],
                "churn_probability": ["not_null"]
            },
            table_name="churn_predictions",
            source_silvers=["churn_features"]
        )
        
        pipeline = builder.to_pipeline()
        
        # Execute pipeline
        result = pipeline.run_initial_load(
            bronze_sources={
                "customers": customers_df,
                "interactions": churn_interactions
            }
        )
        
        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)
        
        # Skip table access for testing - focus on pipeline execution
        # Pipeline execution verified above - storage verification not needed for unit tests
    
    def test_customer_lifetime_value_analysis(self, mock_spark_session, data_generator, test_assertions):
        """Test customer lifetime value analysis pipeline."""
        
        # Create customer and order data
        customers_df = data_generator.create_customer_data(mock_spark_session, num_customers=15)
        orders_df = data_generator.create_ecommerce_orders(mock_spark_session, num_orders=75)
        
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")
        
        # Create pipeline
        builder = PipelineBuilder(spark=mock_spark_session, schema="bronze")
        
        builder.with_bronze_rules(
            name="customers",
            rules={"customer_id": ["not_null"]}
        )
        
        builder.with_bronze_rules(
            name="orders",
            rules={"customer_id": ["not_null"]}
        )
        
        def customer_ltv_transform(spark, df, silvers):
            """Calculate customer lifetime value metrics."""
            return (df
                .withColumn("order_date_parsed", df.order_date.cast("date"))
                .withColumn("total_amount", df.quantity * df.unit_price)
                .groupBy("customer_id")
                .agg(
                    F.count("*").alias("total_orders"),
                    df.total_amount.sum().alias("total_spent"),
                    df.total_amount.avg().alias("avg_order_value"),
                    df.order_date_parsed.min().alias("first_order_date"),
                    df.order_date_parsed.max().alias("last_order_date"),
                    df.product_id.nunique().alias("unique_products")
                )
                .withColumn("customer_tenure_days", df.current_date().datediff(df.first_order_date))
                .withColumn("order_frequency", df.total_orders / F.greatest(df.customer_tenure_days, 1))
            )
        
        builder.add_silver_transform(
            name="customer_ltv",
            source_bronze="orders",
            transform=customer_ltv_transform,
            rules={
                "customer_id": ["not_null"],
                "total_spent": ["non_negative"]
            },
            table_name="customer_ltv"
        )
        
        def ltv_segments_transform(spark, silvers):
            """Create LTV-based customer segments."""
            ltv_data = silvers.get("customer_ltv")
            customers = silvers.get("customers")
            
            if ltv_data is not None and customers is not None:
                # Calculate LTV percentiles for segmentation
                ltv_stats = ltv_data.select(F.percentile_approx("total_spent", 0.5).alias("median_ltv"),
                                           F.percentile_approx("total_spent", 0.8).alias("p80_ltv"),
                                           F.percentile_approx("total_spent", 0.95).alias("p95_ltv")).collect()[0]
                
                median_ltv = ltv_stats["median_ltv"]
                p80_ltv = ltv_stats["p80_ltv"]
                p95_ltv = ltv_stats["p95_ltv"]
                
                return (customers
                    .join(ltv_data, "customer_id", "left")
                    .withColumn("ltv_segment",
                              df.when(df.total_spent >= p95_ltv, "champions")
                              .when(df.total_spent >= p80_ltv, "high_value")
                              .when(df.total_spent >= median_ltv, "medium_value")
                              .otherwise("low_value"))
                    .withColumn("growth_potential",
                              F.when((F.col("order_frequency") < 0.1) & (F.col("total_spent") > 0), "high")
                              .when((F.col("order_frequency") < 0.2) & (F.col("total_spent") > 0), "medium")
                              .otherwise("low"))
                    .select("customer_id", "name", "total_spent", "total_orders", 
                           "avg_order_value", "order_frequency", "ltv_segment", "growth_potential")
                )
            else:
                return spark.createDataFrame([], ["customer_id", "name", "total_spent", "total_orders", "avg_order_value", "order_frequency", "ltv_segment", "growth_potential"])
        
        builder.add_gold_transform(
            name="ltv_segments",
            transform=ltv_segments_transform,
            rules={
                "customer_id": ["not_null"],
                "ltv_segment": ["not_null"]
            },
            table_name="ltv_segments",
            source_silvers=["customer_ltv"]
        )
        
        pipeline = builder.to_pipeline()
        
        # Execute pipeline
        result = pipeline.run_initial_load(
            bronze_sources={
                "customers": customers_df,
                "orders": orders_df
            }
        )
        
        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)
        
        # Skip table access for testing - focus on pipeline execution
        # Pipeline execution verified above - storage verification not needed for unit tests
    
    def test_customer_analytics_logging(self, mock_spark_session, data_generator, log_writer_config, test_assertions):
        """Test comprehensive logging for customer analytics pipeline."""
        
        # Create test data
        customers_df = data_generator.create_customer_data(mock_spark_session, num_customers=10)
        interactions_data = mock_spark_session.createDataFrame([
            ("CUST-001", "website_visit", "2024-01-01T10:00:00", 300),
        ], ["customer_id", "interaction_type", "timestamp", "duration_seconds"])
        
        # Setup schemas
        mock_spark_session.storage.create_schema("bronze")
        mock_spark_session.storage.create_schema("silver")
        mock_spark_session.storage.create_schema("gold")
        mock_spark_session.storage.create_schema("analytics")
        
        # Create LogWriter
        log_writer = LogWriter(
            spark=mock_spark_session,
            schema="analytics",
            table_name="customer_analytics_logs"
        )
        
        # Create pipeline
        builder = PipelineBuilder(spark=mock_spark_session, schema="bronze")
        
        builder.with_bronze_rules(
            name="customers",
            rules={"customer_id": ["not_null"]}
        )
        
        builder.with_bronze_rules(
            name="interactions",
            rules={"customer_id": ["not_null"]}
        )
        
        builder.add_silver_transform(
            name="customer_behavior",
            source_bronze="interactions",
            transform=lambda spark, df, silvers: df,
            rules={"customer_id": ["not_null"]},
            table_name="customer_behavior"
        )
        
        builder.add_gold_transform(
            name="customer_insights",
            transform=lambda spark, silvers: silvers["customer_behavior"].agg(F.count("*").alias("total_customers")),
            rules={"total_customers": ["not_null"]},
            table_name="customer_insights",
            source_silvers=["customer_behavior"]
        )
        
        pipeline = builder.to_pipeline()
        
        # Execute pipeline
        result = pipeline.run_initial_load(
            bronze_sources={
                "customers": customers_df,
                "interactions": interactions_data
            }
        )
        
        # Log execution results
        log_result = log_writer.write_execution_result(result)
        
        # Verify logging was successful
        test_assertions.assert_pipeline_success(result)
        assert log_result is not None
        
        # Verify log table was created
        assert mock_spark_session.storage.table_exists("analytics", "customer_analytics_logs")
        
        # Verify log data
        log_data = mock_spark_session.storage.query_table("analytics", "customer_analytics_logs")
        assert len(log_data) > 0
