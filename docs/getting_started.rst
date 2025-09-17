Getting Started with SparkForge
================================

A quick start guide to get you up and running with SparkForge in minutes.

Installation
------------

.. code-block:: bash

   pip install sparkforge

Your First Pipeline
-------------------

Let's build a simple e-commerce analytics pipeline:

.. important::

   **Validation System**: SparkForge includes a robust validation system that ensures data quality from the start. All pipeline steps must have validation rules, and invalid configurations are rejected with clear error messages.

.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import functions as F
   from pyspark.sql import SparkSession

   # 1. Initialize Spark
   spark = SparkSession.builder \
       .appName("My First Pipeline") \
       .master("local[*]") \
       .getOrCreate()

   # 2. Create sample data
   events_data = [
       ("user1", "click", "product1", "2024-01-01 10:00:00"),
       ("user2", "purchase", "product2", "2024-01-01 11:00:00"),
       ("user3", "view", "product1", "2024-01-01 12:00:00"),
   ]
   events_df = spark.createDataFrame(events_data, ["user_id", "action", "product_id", "timestamp"])

   # 3. Build pipeline
   builder = PipelineBuilder(spark=spark, schema="analytics")

   # Bronze: Raw events
   builder.with_bronze_rules(
       name="events",
       rules={
           "user_id": [F.col("user_id").isNotNull()],
           "action": [F.col("action").isNotNull()],
           "timestamp": [F.col("timestamp").isNotNull()]
       }
   )

   # Silver: Clean events
   def clean_events(spark, bronze_df, prior_silvers):
       return bronze_df.filter(F.col("action").isin(["click", "view", "purchase"]))

   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=clean_events,
       rules={"action": [F.col("action").isNotNull()]},
       table_name="clean_events"
   )

   # Gold: Daily summary
   def daily_summary(spark, silvers):
       events_df = silvers["clean_events"]
       return events_df.groupBy("action").count()

   builder.add_gold_transform(
       name="daily_summary",
       transform=daily_summary,
       rules={"action": [F.col("action").isNotNull()]},
       table_name="daily_summary",
       source_silvers=["clean_events"]
   )

   # 4. Execute pipeline
   pipeline = builder.to_pipeline()
   result = pipeline.initial_load(bronze_sources={"events": events_df})

   print(f"Pipeline completed: {result.success}")
   print(f"Rows written: {result.totals['total_rows_written']}")

What Just Happened?
-------------------

1. **Bronze Layer**: We defined validation rules for raw event data
2. **Silver Layer**: We cleaned the data by filtering valid actions
3. **Gold Layer**: We created a daily summary by action type
4. **Execution**: We ran the pipeline and got results

Next Steps
----------

Debug Individual Steps
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Test Bronze step
   bronze_result = pipeline.execute_bronze_step("events", input_data=events_df)
   print(f"Bronze validation: {bronze_result.validation_result.validation_passed}")

   # Test Silver step
   silver_result = pipeline.execute_silver_step("clean_events")
   print(f"Silver output rows: {silver_result.output_count}")

   # Test Gold step
   gold_result = pipeline.execute_gold_step("daily_summary")
   print(f"Gold duration: {gold_result.duration_seconds:.2f}s")

Add Incremental Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Enable incremental processing
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"  # Process only new data
   )

   # Run incrementally
   new_events = spark.createDataFrame([...], schema)
   result = pipeline.run_incremental(bronze_sources={"events": new_events})

Enable Parallel Execution
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Silver steps run in parallel
   builder = PipelineBuilder(
       spark=spark,
       schema="analytics",
       enable_parallel_silver=True,
       max_parallel_workers=4
   )

Add Data Validation
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Set quality thresholds
   builder = PipelineBuilder(
       spark=spark,
       schema="analytics",
       min_bronze_rate=95.0,  # 95% data quality required
       min_silver_rate=98.0,  # 98% data quality required
       min_gold_rate=99.0     # 99% data quality required
   )

Common Patterns
---------------

E-commerce Pipeline
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Bronze: Raw orders
   builder.with_bronze_rules(
       name="orders",
       rules={
           "order_id": [F.col("order_id").isNotNull()],
           "customer_id": [F.col("customer_id").isNotNull()],
           "amount": [F.col("amount") > 0],
           "timestamp": [F.col("timestamp").isNotNull()]
       },
       incremental_col="timestamp"
   )

   # Silver: Enriched orders
   def enrich_orders(spark, bronze_df, prior_silvers):
       return (bronze_df
           .withColumn("order_date", F.date_trunc("day", "timestamp"))
           .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
           .withColumn("order_category", F.when(F.col("amount") > 100, "high_value").otherwise("standard"))
       )

   builder.add_silver_transform(
       name="enriched_orders",
       source_bronze="orders",
       transform=enrich_orders,
       rules={
           "order_date": [F.col("order_date").isNotNull()],
           "order_category": [F.col("order_category").isNotNull()]
       },
       table_name="enriched_orders",
       watermark_col="timestamp"
   )

   # Gold: Daily revenue
   def daily_revenue(spark, silvers):
       orders_df = silvers["enriched_orders"]
       return (orders_df
           .groupBy("order_date")
           .agg(
               F.sum("amount").alias("total_revenue"),
               F.count("*").alias("order_count"),
               F.countDistinct("customer_id").alias("unique_customers")
           )
       )

   builder.add_gold_transform(
       name="daily_revenue",
       transform=daily_revenue,
       rules={
           "order_date": [F.col("order_date").isNotNull()],
           "total_revenue": [F.col("total_revenue") > 0]
       },
       table_name="daily_revenue",
       source_silvers=["enriched_orders"]
   )

IoT Sensor Data Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Bronze: Raw sensor data
   builder.with_bronze_rules(
       name="sensor_data",
       rules={
           "sensor_id": [F.col("sensor_id").isNotNull()],
           "temperature": [F.col("temperature").between(-50, 150)],
           "humidity": [F.col("humidity").between(0, 100)],
           "timestamp": [F.col("timestamp").isNotNull()]
       },
       incremental_col="timestamp"
   )

   # Silver: Processed sensor data
   def process_sensor_data(spark, bronze_df, prior_silvers):
       return (bronze_df
           .withColumn("is_anomaly", F.col("temperature") > 100)
           .withColumn("sensor_zone", F.substring("sensor_id", 1, 2))
           .filter(F.col("temperature").isNotNull())
       )

   builder.add_silver_transform(
       name="processed_sensors",
       source_bronze="sensor_data",
       transform=process_sensor_data,
       rules={
           "sensor_zone": [F.col("sensor_zone").isNotNull()],
           "is_anomaly": [F.col("is_anomaly").isNotNull()]
       },
       table_name="processed_sensors",
       watermark_col="timestamp"
   )

   # Gold: Zone analytics
   def zone_analytics(spark, silvers):
       sensors_df = silvers["processed_sensors"]
       return (sensors_df
           .groupBy("sensor_zone", F.date_trunc("hour", "timestamp").alias("hour"))
           .agg(
               F.avg("temperature").alias("avg_temperature"),
               F.max("temperature").alias("max_temperature"),
               F.sum("is_anomaly").alias("anomaly_count")
           )
       )

   builder.add_gold_transform(
       name="zone_analytics",
       transform=zone_analytics,
       rules={
           "sensor_zone": [F.col("sensor_zone").isNotNull()],
           "avg_temperature": [F.col("avg_temperature").isNotNull()]
       },
       table_name="zone_analytics",
       source_silvers=["processed_sensors"]
   )

Troubleshooting
---------------

Check Pipeline Status
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   result = pipeline.run_incremental(bronze_sources={"events": events_df})

   if not result.success:
       print(f"Pipeline failed: {result.error_message}")
       print(f"Failed steps: {result.failed_steps}")

Debug Specific Steps
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Check Bronze validation
   bronze_result = pipeline.execute_bronze_step("events", input_data=events_df)
   if not bronze_result.validation_result.validation_passed:
       print(f"Bronze validation failed: {bronze_result.validation_result.validation_rate:.2f}%")

   # Check Silver output
   silver_result = pipeline.execute_silver_step("clean_events")
   print(f"Silver output: {silver_result.output_count} rows")

Monitor Performance
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sparkforge.performance import performance_monitor

   with performance_monitor("pipeline_execution"):
       result = pipeline.run_incremental(bronze_sources={"events": events_df})

   print(f"Execution time: {result.totals['total_duration_secs']:.2f}s")

What's Next?
------------

- :doc:`user_guide` - Learn advanced features and patterns
- :doc:`quick_reference` - Quick reference for common tasks
- :doc:`api_reference` - Complete API documentation
- :doc:`examples/index` - More working examples

Need Help?
----------

- Check the :doc:`troubleshooting` section in the User Guide
- Look at the :doc:`examples/index` directory for working code
- Review the :doc:`api_reference` for detailed documentation

.. admonition:: Happy Pipeline Building! 🚀

   You're now ready to build production-ready data pipelines with SparkForge!
