Quick Reference
===============

This quick reference provides essential SparkForge syntax and patterns for rapid development.

Basic Pipeline Setup
--------------------

.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import functions as F
   
   # Initialize
   builder = PipelineBuilder(spark=spark, schema="analytics")
   
   # Build pipeline
   pipeline = (builder
       .with_bronze_rules(name="events", rules={"id": [F.col("id").isNotNull()]})
       .add_silver_transform(name="clean", source_bronze="events", 
                           transform=clean_func, rules={}, table_name="clean_events")
       .add_gold_transform(name="analytics", transform=analytics_func, 
                         rules={}, table_name="analytics")
       .to_pipeline()
   )
   
   # Execute
   result = pipeline.initial_load(bronze_sources={"events": events_df})

Validation Rules
----------------

.. code-block:: python

   # Basic validation
   rules = {
       "user_id": [F.col("user_id").isNotNull()],
       "email": [F.col("email").contains("@")],
       "age": [F.col("age") > 0, F.col("age") < 120]
   }
   
   # String shortcuts
   rules = {
       "id": ["not_null", "positive"],
       "status": ["not_null"],
       "amount": ["non_negative"]
   }

Execution Modes
---------------

.. code-block:: python

   # Initial load - process all data
   result = pipeline.initial_load(bronze_sources={"events": events_df})
   
   # Incremental - process new data only
   result = pipeline.run_incremental(bronze_sources={"events": new_events_df})
   
   # Full refresh - force reprocessing
   result = pipeline.run_full_refresh(bronze_sources={"events": events_df})

Configuration Options
---------------------

.. code-block:: python

   builder = PipelineBuilder(
       spark=spark,
       schema="analytics",
       min_bronze_rate=95.0,      # Quality thresholds
       min_silver_rate=98.0,
       min_gold_rate=99.0,
       enable_parallel_silver=True, # Parallel execution
       max_parallel_workers=4,
       verbose=True                # Logging
   )

Common Patterns
---------------

**Bronze with Incremental Processing**:
.. code-block:: python

   builder.with_bronze_rules(
       name="events",
       rules={"timestamp": [F.col("timestamp").isNotNull()]},
       incremental_col="timestamp"  # Enables watermarking
   )

**Silver with Dependencies**:
.. code-block:: python

   builder.add_silver_transform(
       name="enriched_events",
       source_bronze="events",
       transform=enrich_func,
       rules={},
       table_name="enriched_events",
       depends_on=["user_profiles"]  # Wait for other Silver steps
   )

**Gold Aggregation**:
.. code-block:: python

   def daily_metrics(spark, silvers):
       events = silvers["clean_events"]
       return events.groupBy("date").agg(F.count("*").alias("events"))
   
   builder.add_gold_transform(
       name="daily_metrics",
       transform=daily_metrics,
       rules={},
       table_name="daily_metrics"
   )

For the complete quick reference with more examples, see: `QUICK_REFERENCE.md <markdown/QUICK_REFERENCE.md>`_
