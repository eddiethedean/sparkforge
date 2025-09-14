#!/usr/bin/env python3
"""
SparkForge Hello World Example

The absolute simplest SparkForge pipeline - just 3 lines of pipeline code!
This demonstrates the Bronze → Silver → Gold flow with minimal complexity.
"""

from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

def main():
    """The simplest possible SparkForge pipeline."""
    
    print("🌍 SparkForge Hello World")
    print("=" * 40)
    
    # Start Spark
    spark = SparkSession.builder \
        .appName("Hello World") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        # Create the simplest possible data
        data = [("Alice", "click"), ("Bob", "view"), ("Alice", "purchase")]
        df = spark.createDataFrame(data, ["user", "action"])
        
        print("📊 Input Data:")
        df.show()
        
        # Build the simplest pipeline (just 3 lines!)
        builder = PipelineBuilder(spark=spark, schema="hello_world")
        
        # Bronze: Just validate user exists
        builder.with_bronze_rules(
            name="events", 
            rules={"user": [F.col("user").isNotNull()]}
        )
        
        # Silver: Filter to only purchases
        builder.add_silver_transform(
            name="purchases",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("action") == "purchase"),
            rules={"action": [F.col("action") == "purchase"]},
            table_name="purchases"
        )
        
        # Gold: Count users who purchased
        builder.add_gold_transform(
            name="user_counts",
            transform=lambda spark, silvers: silvers["purchases"].groupBy("user").count(),
            rules={"user": [F.col("user").isNotNull()]},
            table_name="user_counts",
            source_silvers=["purchases"]
        )
        
        # Run it!
        pipeline = builder.to_pipeline()
        result = pipeline.initial_load(bronze_sources={"events": df})
        
        print(f"\n✅ Pipeline completed: {result.success}")
        print(f"📈 Rows processed: {result.totals['total_rows_written']}")
        
        # Show the results
        print("\n🎯 Final Results (Gold Layer):")
        spark.table("hello_world.user_counts").show()
        
        print("\n🔍 Intermediate Results (Silver Layer):")
        spark.table("hello_world.purchases").show()
        
        print("\n🎉 That's it! You've built a complete Bronze → Silver → Gold pipeline!")
        print("💡 The data flow: Raw events → Filtered purchases → User purchase counts")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
