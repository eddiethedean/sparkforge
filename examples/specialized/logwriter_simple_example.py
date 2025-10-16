#!/usr/bin/env python3
"""
Simple example demonstrating the new LogWriter API.

This example shows how to use the simplified LogWriter interface
to log pipeline execution results.
"""

from pyspark.sql import SparkSession, functions as F
from sparkforge.pipeline import PipelineBuilder
from sparkforge.writer import LogWriter


def main():
    """Demonstrate the new LogWriter API."""
    print("🚀 LogWriter Simple API Example")
    print("=" * 60)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("LogWriter Example")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    try:
        # ====================================================================
        # Step 1: Create sample data
        # ====================================================================
        print("\n📊 Creating sample data...")
        sample_data = [
            ("user1", "login", 100, "2024-01-15 10:00:00"),
            ("user2", "purchase", 250, "2024-01-15 10:05:00"),
            ("user3", "logout", 50, "2024-01-15 10:10:00"),
            ("user1", "view", 10, "2024-01-15 10:15:00"),
            ("user2", "click", 25, "2024-01-15 10:20:00"),
        ]

        source_df = spark.createDataFrame(
            sample_data, ["user_id", "action", "value", "timestamp"]
        )
        source_df = source_df.withColumn("timestamp", F.to_timestamp("timestamp"))

        print(f"✅ Created sample data with {source_df.count()} rows")

        # ====================================================================
        # Step 2: Build and run pipeline
        # ====================================================================
        print("\n🏗️  Building pipeline...")
        builder = PipelineBuilder(spark, schema="logwriter_example")

        # Add bronze step
        builder.add_bronze_step("events", rules={"user_id": [F.col("user_id").isNotNull()]})

        # Add silver step
        def silver_transform(spark, df, silvers):
            return (
                df.withColumn("event_date", F.to_date("timestamp"))
                .withColumn("processed_at", F.current_timestamp())
                .select("user_id", "action", "value", "event_date", "processed_at")
            )

        builder.add_silver_step(
            name="processed_events",
            source_bronze="events",
            transform=silver_transform,
            rules={"value": [F.col("value") > 0]},
        )

        # Add gold step
        def gold_transform(spark, silvers):
            return silvers["processed_events"].groupBy("event_date").agg(
                F.count("*").alias("event_count"),
                F.sum("value").alias("total_value"),
                F.countDistinct("user_id").alias("unique_users"),
            )

        builder.add_gold_step(
            name="daily_summary",
            source_silvers=["processed_events"],
            transform=gold_transform,
        )

        # Build pipeline
        pipeline = builder.to_pipeline()
        print("✅ Pipeline built successfully")

        # ====================================================================
        # Step 3: Run pipeline and get report
        # ====================================================================
        print("\n🚀 Running pipeline (initial load)...")
        report = pipeline.run_initial_load(bronze_sources={"events": source_df})

        print(f"\n📈 Pipeline Results:")
        print(f"   Status: {report.status.value}")
        print(f"   Total steps: {report.metrics.total_steps}")
        print(f"   Successful steps: {report.metrics.successful_steps}")
        print(f"   Failed steps: {report.metrics.failed_steps}")
        print(f"   Total rows processed: {report.metrics.total_rows_processed:,}")
        print(f"   Total rows written: {report.metrics.total_rows_written:,}")
        print(f"   Duration: {report.duration_seconds:.2f}s")

        # ====================================================================
        # Step 4: Initialize LogWriter with simple API
        # ====================================================================
        print("\n📝 Initializing LogWriter (simple API)...")
        
        # OLD WAY (deprecated):
        # config = WriterConfig(table_schema="logs", table_name="pipeline_execution")
        # writer = LogWriter(spark, config)
        
        # NEW WAY (simple):
        writer = LogWriter(spark, schema="logs", table_name="pipeline_execution")
        
        print("✅ LogWriter initialized")

        # ====================================================================
        # Step 5: Create log table with first report
        # ====================================================================
        print("\n📊 Creating log table with report...")
        result = writer.create_table(report)
        
        print(f"✅ Log table created:")
        print(f"   Table: {result['table_fqn']}")
        print(f"   Rows written: {result['rows_written']}")
        print(f"   Run ID: {result['run_id']}")

        # ====================================================================
        # Step 6: Run pipeline again (incremental) and append
        # ====================================================================
        print("\n🔄 Running incremental pipeline...")
        
        # Create new incremental data
        incremental_data = [
            ("user4", "signup", 500, "2024-01-15 11:00:00"),
            ("user5", "login", 75, "2024-01-15 11:05:00"),
        ]
        
        incremental_df = spark.createDataFrame(
            incremental_data, ["user_id", "action", "value", "timestamp"]
        )
        incremental_df = incremental_df.withColumn(
            "timestamp", F.to_timestamp("timestamp")
        )
        
        report2 = pipeline.run_incremental(bronze_sources={"events": incremental_df})
        
        print(f"\n📈 Incremental Results:")
        print(f"   Status: {report2.status.value}")
        print(f"   Rows processed: {report2.metrics.total_rows_processed:,}")
        print(f"   Rows written: {report2.metrics.total_rows_written:,}")

        # Append to log table
        print("\n📝 Appending report to log table...")
        result2 = writer.append(report2)
        
        print(f"✅ Report appended:")
        print(f"   Rows written: {result2['rows_written']}")
        print(f"   Run ID: {result2['run_id']}")

        # ====================================================================
        # Step 7: Query log table
        # ====================================================================
        print("\n🔍 Querying log table...")
        logs_df = spark.table("logs.pipeline_execution")
        
        print(f"\n📊 Log Table Contents ({logs_df.count()} rows):")
        logs_df.select(
            "run_id",
            "run_mode",
            "pipeline_id",
            "success",
            "rows_processed",
            "rows_written",
            "duration_secs",
        ).show(truncate=False)

        print("\n🎉 Example completed successfully!")
        print("\n💡 Key Takeaways:")
        print("   1. LogWriter now accepts schema and table_name directly")
        print("   2. Use create_table() for first report (overwrites)")
        print("   3. Use append() for subsequent reports")
        print("   4. All reports logged to a single queryable table")

    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        import traceback
        traceback.print_exc()
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

