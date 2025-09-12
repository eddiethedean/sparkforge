"""
Test demonstrating Bronze tables without datetime columns.

This test shows how Bronze tables without incremental_col force
downstream Silver tables to use overwrite mode for full refresh.
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from sparkforge import PipelineBuilder


@pytest.fixture(scope="function")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("BronzeNoDatetimeTest") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    yield spark
    
    # Cleanup after test
    try:
        spark.sql("DROP DATABASE IF EXISTS bronze_no_datetime_test CASCADE")
    except:
        pass
    spark.stop()


@pytest.fixture
def sample_data(spark_session):
    """Create sample data for testing."""
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("device_category", StringType(), True),
        StructField("event_date", StringType(), True)
    ])
    
    data = [
        ("user1", "click", "mobile", "2024-01-01"),
        ("user2", "view", "desktop", "2024-01-01"),
        ("user1", "purchase", "mobile", "2024-01-02"),
        ("user3", "click", "tablet", "2024-01-02"),
        ("user2", "view", "mobile", "2024-01-03"),
    ]
    
    return spark_session.createDataFrame(data, schema)


def test_bronze_no_datetime_full_pipeline(spark_session, sample_data):
    """Test complete pipeline with Bronze table without datetime column."""
    
    # Clean up any existing data
    try:
        spark_session.sql("DROP DATABASE IF EXISTS bronze_no_datetime_test CASCADE")
    except:
        pass
    
    # Create database
    spark_session.sql("CREATE DATABASE bronze_no_datetime_test")
    
    # Create pipeline builder
    builder = PipelineBuilder(spark=spark_session, schema="bronze_no_datetime_test")
    
    # Bronze Layer: Raw data ingestion (NO datetime column)
    builder.with_bronze_rules(
        name="events_no_datetime",
        rules={
            "user_id": ["not_null"],
            "action": ["not_null"],
            "device_category": ["not_null"],
            "event_date": ["not_null"]  # Add event_date to validation rules
        },
        # Note: No incremental_col specified - this forces full refresh
        description="Raw event data without datetime column (full refresh mode)"
    )
    
    # Silver Layer: Data enrichment
    def silver_transform(spark, bronze_df, prior_silvers):
        """Transform bronze data to silver with enrichment."""
        return (bronze_df
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("event_date", F.to_date("event_date"))
        )
    
    builder.add_silver_transform(
        name="enriched_events",
        source_bronze="events_no_datetime",
        transform=silver_transform,
        table_name="enriched_events",
        watermark_col="processed_at",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "processed_at": ["not_null"]},
        description="Enriched event data with processing metadata (full refresh mode)"
    )
    
    # Gold Layer: Business analytics
    def gold_transform(spark, silvers):
        """Gold transform: Device analytics and business metrics."""
        enriched_df = silvers["enriched_events"]
        return (enriched_df
            .groupBy("device_category", "action")
            .agg(
                F.count("*").alias("total_events"),
                F.countDistinct("user_id").alias("unique_users"),
                F.max("processed_at").alias("last_processed")
            )
            .orderBy("device_category", "action")
        )
    
    builder.add_gold_transform(
        name="device_analytics",
        source_silvers=["enriched_events"],
        transform=gold_transform,
        table_name="device_analytics",
        rules={"device_category": ["not_null"], "action": ["not_null"], "total_events": ["not_null"], "unique_users": ["not_null"], "last_processed": ["not_null"]},
        description="Device-based analytics and business metrics"
    )
    
    # Create the pipeline
    pipeline = builder.to_pipeline()
    
    # Create runner and execute
    runner = pipeline
    
    # Run initial load
    result = runner.initial_load(
        bronze_sources={"events_no_datetime": sample_data}
    )
    
    # Verify results
    assert result.status.name == "COMPLETED"
    # Note: total_rows_processed might be 0 due to how metrics are calculated
    # but total_rows_written should reflect the actual data written
    assert result.metrics.total_rows_written >= 5  # At least 5 rows should be written
    
    # Note: Bronze step only validates data, doesn't create tables
    # Tables are created by Silver and Gold steps
    
    # Verify Silver table was created with overwrite mode (no incremental column)
    silver_table = spark_session.table("bronze_no_datetime_test.enriched_events")
    assert silver_table.count() == 5
    assert "processed_at" in silver_table.columns
    
    # Verify Gold table was created
    gold_table = spark_session.table("bronze_no_datetime_test.device_analytics")
    assert gold_table.count() > 0  # Should have aggregated data
    
    # Verify the Gold table has the expected columns
    expected_columns = ["device_category", "action", "total_events", "unique_users", "last_processed"]
    for col in expected_columns:
        assert col in gold_table.columns


def test_bronze_no_datetime_incremental_behavior(spark_session, sample_data):
    """Test that Bronze without datetime forces Silver to use overwrite mode."""
    
    # Clean up any existing data
    try:
        spark_session.sql("DROP DATABASE IF EXISTS bronze_no_datetime_test CASCADE")
    except:
        pass
    
    # Create database
    spark_session.sql("CREATE DATABASE bronze_no_datetime_test")
    
    # Create pipeline builder
    builder = PipelineBuilder(spark=spark_session, schema="bronze_no_datetime_test")
    
    # Bronze Layer: Raw data ingestion (NO datetime column)
    builder.with_bronze_rules(
        name="events_no_datetime",
        rules={
            "user_id": ["not_null"],
            "action": ["not_null"]
        },
        # Note: No incremental_col specified
        description="Raw event data without datetime column"
    )
    
    # Silver Layer: Data enrichment
    def silver_transform(spark, bronze_df, prior_silvers):
        """Transform bronze data to silver with enrichment."""
        return (bronze_df
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("enrichment_flag", F.lit("processed"))
        )
    
    builder.add_silver_transform(
        name="enriched_events",
        source_bronze="events_no_datetime",
        transform=silver_transform,
        table_name="enriched_events",
        watermark_col="processed_at",
        rules={"user_id": ["not_null"], "action": ["not_null"]},
        description="Enriched event data"
    )
    
    # Create the pipeline
    pipeline = builder.to_pipeline()
    runner = pipeline
    
    # Run initial load
    result1 = runner.initial_load(
        bronze_sources={"events_no_datetime": sample_data}
    )
    
    assert result1.status.name == "COMPLETED"
    
    # Create new data for incremental run
    new_data = spark_session.createDataFrame([
        ("user4", "click", "mobile", "2024-01-04"),
        ("user5", "view", "desktop", "2024-01-04"),
    ], sample_data.schema)
    
    # Run incremental load
    result2 = runner.run_incremental(
        bronze_sources={"events_no_datetime": new_data}
    )
    
    assert result2.status.name == "COMPLETED"
    
    # Verify that Silver table was overwritten (not appended)
    # Since Bronze has no incremental column, Silver should use overwrite mode
    silver_table = spark_session.table("bronze_no_datetime_test.enriched_events")
    
    # The table should only contain the new data (2 rows), not the old data (5 rows)
    # This proves that overwrite mode was used instead of append mode
    assert silver_table.count() == 2
    
    # Verify the data is the new data
    user_ids = [row["user_id"] for row in silver_table.select("user_id").collect()]
    assert "user4" in user_ids
    assert "user5" in user_ids
    assert "user1" not in user_ids  # Old data should be gone


def test_bronze_with_datetime_vs_without_datetime(spark_session, sample_data):
    """Compare behavior between Bronze with and without datetime columns."""
    
    # Clean up any existing data
    try:
        spark_session.sql("DROP DATABASE IF EXISTS bronze_comparison_test CASCADE")
    except:
        pass
    
    # Create database
    spark_session.sql("CREATE DATABASE bronze_comparison_test")
    
    # Test 1: Bronze WITH datetime column
    builder1 = PipelineBuilder(spark=spark_session, schema="bronze_comparison_test")
    builder1.with_bronze_rules(
        name="events_with_datetime",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"]},
        incremental_col="event_date",  # Has datetime column
        description="Bronze with datetime"
    )
    
    def silver_transform(spark, bronze_df, prior_silvers):
        return bronze_df.withColumn("processed_at", F.current_timestamp())
    
    builder1.add_silver_transform(
        name="silver_with_datetime",
        source_bronze="events_with_datetime",
        transform=silver_transform,
        table_name="silver_with_datetime",
        watermark_col="processed_at",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"], "processed_at": ["not_null"]}
    )
    
    pipeline1 = builder1.to_pipeline()
    runner1 = pipeline1
    
    # Run initial load
    result1 = runner1.initial_load(
        bronze_sources={"events_with_datetime": sample_data}
    )
    assert result1.status.name == "COMPLETED"
    
    # Test 2: Bronze WITHOUT datetime column
    builder2 = PipelineBuilder(spark=spark_session, schema="bronze_comparison_test")
    builder2.with_bronze_rules(
        name="events_without_datetime",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"]},
        # No incremental_col specified
        description="Bronze without datetime"
    )
    
    builder2.add_silver_transform(
        name="silver_without_datetime",
        source_bronze="events_without_datetime",
        transform=silver_transform,
        table_name="silver_without_datetime",
        watermark_col="processed_at",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"], "processed_at": ["not_null"]}
    )
    
    pipeline2 = builder2.to_pipeline()
    runner2 = pipeline2
    
    # Run initial load
    result2 = runner2.initial_load(
        bronze_sources={"events_without_datetime": sample_data}
    )
    assert result2.status.name == "COMPLETED"
    
    # Verify both Silver tables exist
    silver_with = spark_session.table("bronze_comparison_test.silver_with_datetime")
    silver_without = spark_session.table("bronze_comparison_test.silver_without_datetime")
    
    assert silver_with.count() == 5
    assert silver_without.count() == 5
    
    # Both should have the same data initially (sort to ensure consistent order)
    with_data = silver_with.select("user_id").orderBy("user_id").collect()
    without_data = silver_without.select("user_id").orderBy("user_id").collect()
    assert with_data == without_data


def test_bronze_step_validation():
    """Test BronzeStep model validation for optional incremental_col."""
    from sparkforge.models import BronzeStep, ColumnRules
    
    # Test Bronze step without incremental column
    bronze_no_datetime = BronzeStep(
        name="test_bronze",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"], "processed_at": ["not_null"]},
        incremental_col=None
    )
    
    assert not bronze_no_datetime.has_incremental_capability
    assert bronze_no_datetime.incremental_col is None
    
    # Test Bronze step with incremental column
    bronze_with_datetime = BronzeStep(
        name="test_bronze",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"], "processed_at": ["not_null"]},
        incremental_col="created_at"
    )
    
    assert bronze_with_datetime.has_incremental_capability
    assert bronze_with_datetime.incremental_col == "created_at"
    
    # Test validation
    bronze_no_datetime.validate()  # Should not raise
    bronze_with_datetime.validate()  # Should not raise


def test_pipeline_builder_with_optional_incremental():
    """Test PipelineBuilder with_bronze_rules method with optional incremental_col."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("TestPipelineBuilder") \
        .master("local[*]") \
        .getOrCreate()
    
    builder = PipelineBuilder(spark=spark, schema="test_schema")
    
    # Test without incremental_col
    builder.with_bronze_rules(
        name="test_bronze_no_datetime",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"], "processed_at": ["not_null"]},
        # incremental_col not specified
        description="Test bronze without datetime"
    )
    
    # Verify the bronze step was created correctly
    assert "test_bronze_no_datetime" in builder.bronze_steps
    bronze_step = builder.bronze_steps["test_bronze_no_datetime"]
    assert bronze_step.incremental_col is None
    assert not bronze_step.has_incremental_capability
    
    # Test with incremental_col
    builder.with_bronze_rules(
        name="test_bronze_with_datetime",
        rules={"user_id": ["not_null"], "action": ["not_null"], "device_category": ["not_null"], "event_date": ["not_null"], "processed_at": ["not_null"]},
        incremental_col="created_at",
        description="Test bronze with datetime"
    )
    
    # Verify the bronze step was created correctly
    assert "test_bronze_with_datetime" in builder.bronze_steps
    bronze_step = builder.bronze_steps["test_bronze_with_datetime"]
    assert bronze_step.incremental_col == "created_at"
    assert bronze_step.has_incremental_capability
    
    # Cleanup
    spark.stop()
