#!/usr/bin/env python3
"""
Basic usage example for Mock Spark.

This example demonstrates how to use the Mock Spark implementation
for testing SparkForge without a real Spark session.
"""

from mock_spark import MockSparkSession
from mock_spark.types import MockStructType, MockStructField, StringType, IntegerType, DoubleType
from mock_spark.functions import F


def main():
    """Demonstrate basic Mock Spark usage."""
    print("🚀 Mock Spark Basic Usage Example")
    print("=" * 50)
    
    # 1. Create Mock Spark Session
    print("\n1. Creating Mock Spark Session...")
    spark = MockSparkSession("MockSparkApp")
    print(f"✓ Created session: {spark.app_name}")
    
    # 2. Create Schema
    print("\n2. Creating schema...")
    schema = MockStructType([
        MockStructField("id", IntegerType()),
        MockStructField("name", StringType()),
        MockStructField("age", IntegerType()),
        MockStructField("salary", DoubleType())
    ])
    print(f"✓ Created schema with {len(schema)} fields")
    
    # 3. Create Sample Data
    print("\n3. Creating sample data...")
    data = [
        {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
        {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0},
        {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0},
        {"id": 4, "name": "Diana", "age": 28, "salary": 55000.0},
        {"id": 5, "name": "Eve", "age": 32, "salary": 65000.0}
    ]
    print(f"✓ Created {len(data)} sample records")
    
    # 4. Create DataFrame
    print("\n4. Creating DataFrame...")
    df = spark.createDataFrame(data, schema)
    print(f"✓ Created DataFrame with {df.count()} rows and {len(df.columns())} columns")
    
    # 5. Basic Operations
    print("\n5. Basic DataFrame operations...")
    
    # Show schema
    print("\nSchema:")
    df.printSchema()
    
    # Show data
    print("\nData:")
    df.show()
    
    # 6. Filtering
    print("\n6. Filtering operations...")
    
    # Filter by age
    young_employees = df.filter(F.col("age") < 30)
    print(f"✓ Young employees (< 30): {young_employees.count()} rows")
    young_employees.show()
    
    # Filter by salary
    high_earners = df.filter(F.col("salary") > 60000)
    print(f"✓ High earners (> 60k): {high_earners.count()} rows")
    high_earners.show()
    
    # 7. Column Selection
    print("\n7. Column selection...")
    
    # Select specific columns
    names_and_ages = df.select("name", "age")
    print(f"✓ Selected name and age: {len(names_and_ages.columns())} columns")
    names_and_ages.show()
    
    # 8. Aggregations
    print("\n8. Aggregation operations...")
    
    # Group by age range and count
    age_groups = df.groupBy("age").count()
    print("✓ Age groups:")
    age_groups.show()
    
    # Average salary by age group
    avg_salary = df.groupBy("age").avg("salary")
    print("✓ Average salary by age:")
    avg_salary.show()
    
    # 9. Storage Operations
    print("\n9. Storage operations...")
    
    # Create schema
    spark.storage.create_schema("hr")
    print("✓ Created 'hr' schema")
    
    # Save DataFrame as table
    df.write().format("parquet").mode("overwrite").saveAsTable("hr.employees")
    print("✓ Saved DataFrame as 'hr.employees' table")
    
    # Query table
    table_df = spark.table("hr.employees")
    print(f"✓ Loaded table with {table_df.count()} rows")
    
    # 10. Error Handling
    print("\n10. Error handling demonstration...")
    
    try:
        # Try to select nonexistent column
        df.select("nonexistent_column")
    except Exception as e:
        print(f"✓ Caught expected error: {type(e).__name__}: {e}")
    
    try:
        # Try to access nonexistent table
        spark.table("nonexistent.table")
    except Exception as e:
        print(f"✓ Caught expected error: {type(e).__name__}: {e}")
    
    # 11. Cleanup
    print("\n11. Cleanup...")
    spark.stop()
    print("✓ Stopped Mock Spark session")
    
    print("\n🎉 Mock Spark example completed successfully!")
    print("\nThis demonstrates that Mock Spark can be used to test")
    print("SparkForge pipelines without requiring a real Spark cluster.")


if __name__ == "__main__":
    main()
