5-Minute Quick Start
====================

Get up and running with SparkForge in under 5 minutes! This guide will have you processing data through Bronze → Silver → Gold layers quickly.

Prerequisites
-------------

Before you start, make sure you have:

- Python 3.8+ installed
- Java 8+ installed (for PySpark)
- Basic understanding of Python

Installation
------------

.. code-block:: bash

   pip install sparkforge pyspark

Your First Pipeline (2 minutes)
--------------------------------

Let's create the simplest possible pipeline:

.. note::

   **Validation Requirements**: All pipeline steps must have validation rules. SparkForge will reject invalid configurations with clear error messages to help you fix issues quickly.

.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import SparkSession, functions as F

   # 1. Start Spark
   spark = SparkSession.builder.appName("My First Pipeline").getOrCreate()

   # 2. Create some sample data
   data = [("user1", "click", 100), ("user2", "purchase", 200)]
   df = spark.createDataFrame(data, ["user_id", "action", "value"])

   # 3. Build your pipeline
   builder = PipelineBuilder(spark=spark, schema="my_first_schema")

   # Bronze: Raw data (just validate it exists)
   builder.with_bronze_rules(name="events", rules={"user_id": [F.col("user_id").isNotNull()]})

   # Silver: Clean the data
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=lambda spark, df, silvers: df.filter(F.col("value") > 50),
       rules={"value": [F.col("value") > 50]},
       table_name="clean_events"
   )

   # Gold: Aggregate the data
   builder.add_gold_transform(
       name="daily_metrics",
       transform=lambda spark, silvers: silvers["clean_events"].groupBy("action").count(),
       rules={"action": [F.col("action").isNotNull()]},
       table_name="daily_metrics"
   )

   # 4. Create and run the pipeline
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": df})

   print(f"Pipeline completed! Status: {result.status}")

Understanding the Pipeline
--------------------------

Let's break down what just happened:

**Bronze Layer (Raw Data)**
- Validates that `user_id` is not null
- Stores raw data as-is for audit purposes

**Silver Layer (Clean Data)**
- Filters out low-value events (value <= 50)
- Applies data quality rules
- Creates a clean dataset for analysis

**Gold Layer (Analytics)**
- Aggregates data by action type
- Creates business-ready metrics
- Optimized for reporting and dashboards

Key Concepts
------------

**PipelineBuilder**: The main class for building pipelines
- ``with_bronze_rules()``: Define validation rules for raw data
- ``add_silver_transform()``: Add data cleaning and transformation steps
- ``add_gold_transform()``: Add business analytics and aggregation steps

**Execution Modes**:
- ``run_initial_load()``: Process all data from scratch
- ``run_incremental()``: Process only new/changed data (coming soon)

**Validation Rules**: PySpark Column expressions that define data quality
- ``F.col("column").isNotNull()``: Check for null values
- ``F.col("value") > 50``: Numeric comparisons
- ``F.col("status").isin(["active", "pending"])``: Value lists

Next Steps (3 minutes)
-----------------------

Now that you have a working pipeline, let's explore more features:

**1. Add More Validation Rules**

.. code-block:: python

   builder.with_bronze_rules(
       name="events",
       rules={
           "user_id": [F.col("user_id").isNotNull()],
           "action": [F.col("action").isin(["click", "purchase", "view"])],
           "value": [F.col("value") > 0]
       }
   )

**2. Use Incremental Processing**

.. code-block:: python

   # Add timestamp column for incremental processing
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"  # Enable incremental processing
   )

**3. Add Error Handling**

.. code-block:: python

   try:
       result = pipeline.run_initial_load(bronze_sources={"events": df})
       print(f"Success! Processed {result.total_steps} steps")
   except Exception as e:
       print(f"Pipeline failed: {e}")

**4. View Pipeline Results**

.. code-block:: python

   # Check pipeline status
   print(f"Status: {result.status}")
   print(f"Total steps: {result.total_steps}")
   print(f"Successful steps: {result.successful_steps}")
   print(f"Failed steps: {result.failed_steps}")

   # View the final data
   spark.table("my_first_schema.daily_metrics").show()

Common Patterns
---------------

**E-commerce Analytics**
.. code-block:: python

   # Track user behavior
   builder.with_bronze_rules(name="user_events", rules={"user_id": [F.col("user_id").isNotNull()]})
   builder.add_silver_transform(
       name="user_sessions",
       source_bronze="user_events",
       transform=lambda spark, df, silvers: df.groupBy("user_id").agg(F.count("*").alias("event_count")),
       rules={"event_count": [F.col("event_count") > 0]},
       table_name="user_sessions"
   )

**IoT Sensor Data**
.. code-block:: python

   # Process sensor readings
   builder.with_bronze_rules(
       name="sensor_data",
       rules={
           "sensor_id": [F.col("sensor_id").isNotNull()],
           "temperature": [F.col("temperature").between(-50, 150)],
           "timestamp": [F.col("timestamp").isNotNull()]
       },
       incremental_col="timestamp"
   )

**Business Intelligence**
.. code-block:: python

   # Create KPI dashboards
   builder.add_gold_transform(
       name="kpi_dashboard",
       transform=lambda spark, silvers: silvers["clean_data"].groupBy("date").agg(
           F.sum("revenue").alias("daily_revenue"),
           F.count("*").alias("transaction_count")
       ),
       rules={"daily_revenue": [F.col("daily_revenue") >= 0]},
       table_name="kpi_dashboard"
   )

Troubleshooting
---------------

**Common Issues:**

1. **"No module named 'sparkforge'"**
   - Run: ``pip install sparkforge``

2. **"Java gateway process exited"**
   - Install Java 8+: ``brew install openjdk@8`` (macOS) or ``sudo apt-get install openjdk-8-jdk`` (Ubuntu)

3. **"Table not found"**
   - Make sure to run ``pipeline.run_initial_load()`` before accessing tables

4. **"Validation failed"**
   - Check your data against the validation rules
   - Use ``df.show()`` to inspect your data

**Getting Help:**

- Check the `Troubleshooting Guide <troubleshooting.html>`_
- Browse `Examples <examples/index.html>`_
- Read the `User Guide <user_guide.html>`_

What's Next?
------------

You're now ready to build production data pipelines! Here's what to explore next:

1. **`User Guide <user_guide.html>`_**: Complete feature documentation
2. **`Examples <examples/index.html>`_**: Real-world pipeline examples
3. **`API Reference <api_reference.html>`_**: Detailed API documentation
4. **`Troubleshooting <troubleshooting.html>`_**: Common issues and solutions

Happy data processing! 🚀
