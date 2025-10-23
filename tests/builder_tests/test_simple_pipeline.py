"""
Simple test to verify basic pipeline execution without complex validation.
"""

import pytest
from sparkforge.pipeline import PipelineBuilder, PipelineRunner
from sparkforge.writer import LogWriter, WriterConfig, WriteMode
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig
from sparkforge.writer.models import LogLevel


class TestSimplePipeline:
    """Test simple pipeline execution with mock-spark."""
    
    def test_simple_pipeline_execution(self, mock_spark_session, data_generator, test_assertions):
        """Test simple pipeline without complex validation rules."""
        
        # Create test data
        orders_df = data_generator.create_ecommerce_orders(mock_spark_session, num_orders=10)
        
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
        
        # Bronze Layer: Simple validation
        builder.with_bronze_rules(
            name="raw_orders",
            rules={
                "order_id": ["not_null"]
            }
        )
        
        # Silver Layer: Simple transform
        def clean_orders_transform(spark, df, silvers):
            """Clean order data."""
            return df.select("order_id", "customer_id", "product_id", "quantity", "unit_price")
        
        builder.add_silver_transform(
            name="clean_orders",
            source_bronze="raw_orders",
            transform=clean_orders_transform,
            rules={
                "order_id": ["not_null"]
            },
            table_name="clean_orders"
        )
        
        # Gold Layer: Simple aggregation
        def order_summary_transform(spark, silvers):
            """Create order summary."""
            clean_orders = silvers.get("clean_orders")
            if clean_orders is not None:
                return clean_orders.count()
            else:
                return spark.createDataFrame([], ["count"])
        
        builder.add_gold_transform(
            name="order_summary",
            transform=order_summary_transform,
            rules={"count": ["not_null"]},
            table_name="order_summary",
            source_silvers=["clean_orders"]
        )
        
        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        
        # Execute initial load
        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_orders": orders_df
            }
        )
        
        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)
        
        print("✅ Simple pipeline test passed")
