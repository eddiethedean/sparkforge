Hello World Example
===================

The absolute simplest SparkForge pipeline - just 3 lines of pipeline code! This demonstrates the Bronze → Silver → Gold flow with minimal complexity.

What You'll Learn
-----------------

- How to create a simple Bronze → Silver → Gold pipeline
- Basic data validation and transformation
- Step-by-step execution and debugging

Prerequisites
-------------

- Python 3.8+ with SparkForge installed
- Basic understanding of Python

Setup and Imports
-----------------

.. code-block:: python

   # Import required libraries
   from sparkforge import PipelineBuilder
   from pyspark.sql import SparkSession, functions as F

   # Initialize Spark
   spark = SparkSession.builder \
       .appName("Hello World") \
       .master("local[*]") \
       .getOrCreate()

   print("✅ Spark session created successfully!")
   print(f"📊 Spark version: {spark.version}")

Create Sample Data
------------------

Let's start with some simple data to understand the pipeline flow:

.. code-block:: python

   # Create the simplest possible data
   data = [("Alice", "click"), ("Bob", "view"), ("Alice", "purchase")]
   df = spark.createDataFrame(data, ["user", "action"])

   print("📊 Input Data:")
   df.show()

Build the Pipeline
------------------

Now let's build a simple Bronze → Silver → Gold pipeline:

.. code-block:: python

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

Execute the Pipeline
--------------------

Now let's run our pipeline and see the results!

.. code-block:: python

   # Run it!
   pipeline = builder.to_pipeline()
   result = pipeline.initial_load(bronze_sources={"events": df})

   print(f"\n✅ Pipeline completed: {result.success}")
   print(f"📈 Rows processed: {result.totals['total_rows_written']}")

Explore the Results
-------------------

Let's see what our pipeline created at each layer:

.. code-block:: python

   # Show all created tables
   print("\n📋 Created Tables:")
   spark.sql("SHOW TABLES IN hello_world").show()

   # Bronze Layer Results
   print("\n🥉 Bronze Layer - Raw Data:")
   spark.table("hello_world.events").show()

   # Silver Layer Results
   print("\n🥈 Silver Layer - Cleaned Data (Purchases Only):")
   spark.table("hello_world.purchases").show()

   # Gold Layer Results
   print("\n🥇 Gold Layer - Business Analytics:")
   spark.table("hello_world.user_counts").show()

Step-by-Step Debugging
----------------------

One of SparkForge's powerful features is the ability to execute individual steps for debugging:

.. code-block:: python

   # Execute just the Bronze step
   bronze_result = pipeline.execute_bronze_step("events", input_data=df)
   print(f"🔍 Bronze step result:")
   print(f"   Status: {bronze_result.status.value}")
   print(f"   Validation passed: {bronze_result.validation_result.validation_passed}")
   print(f"   Output rows: {bronze_result.output_count}")

   # Execute just the Silver step
   silver_result = pipeline.execute_silver_step("purchases")
   print(f"🔍 Silver step result:")
   print(f"   Status: {silver_result.status.value}")
   print(f"   Output rows: {silver_result.output_count}")
   print(f"   Duration: {silver_result.duration_seconds:.2f}s")

   # Execute just the Gold step
   gold_result = pipeline.execute_gold_step("user_counts")
   print(f"🔍 Gold step result:")
   print(f"   Status: {gold_result.status.value}")
   print(f"   Output rows: {gold_result.output_count}")
   print(f"   Duration: {gold_result.duration_seconds:.2f}s")

Try It Yourself!
----------------

Now it's your turn! Try modifying the pipeline:

Exercise 1: Add More Data
~~~~~~~~~~~~~~~~~~~~~~~~~

Add more sample data and see how the pipeline handles it:

.. code-block:: python

   # Your turn! Add more data here
   new_data = [
       ("David", "purchase"),
       ("Eve", "click"),
       # ... add more records
   ]

   new_df = spark.createDataFrame(new_data, ["user", "action"])

   # Run the pipeline with new data
   # result = pipeline.run_incremental(bronze_sources={"events": new_df})

   print("📝 Add your code here!")

Exercise 2: Modify the Silver Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Try filtering for different actions or adding new transformations:

.. code-block:: python

   # Your turn! Modify the Silver transformation
   # Try filtering for "click" instead of "purchase"
   # Or add a new column to the data

   print("📝 Modify the Silver layer transformation here!")

Exercise 3: Add a New Gold Layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new Gold transformation that counts actions by type:

.. code-block:: python

   # Your turn! Add a new Gold transformation
   # Try counting actions by type instead of users

   print("📝 Add your Gold transformation here!")

What You've Learned
-------------------

.. admonition:: 🎉 Congratulations!

   You've successfully built your first SparkForge pipeline!

Key Concepts:

1. **Bronze Layer**: Raw data ingestion and basic validation
2. **Silver Layer**: Data cleaning and transformation
3. **Gold Layer**: Business analytics and insights
4. **Step-by-Step Debugging**: Execute individual steps for troubleshooting
5. **Pipeline Execution**: Run complete pipelines with different modes

Next Steps:

- :doc:`progressive_examples` - Learn more advanced concepts
- :doc:`usecase_ecommerce` - Build a real business pipeline
- :doc:`usecase_iot` - Process IoT sensor data
- :doc:`user_guide` - Learn advanced features and patterns

Cleanup
-------

Don't forget to stop the Spark session when you're done!

.. code-block:: python

   # Stop the Spark session
   spark.stop()
   print("🛑 Spark session stopped. Goodbye! 👋")
