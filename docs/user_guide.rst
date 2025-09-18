User Guide
==========

This comprehensive guide covers all aspects of using SparkForge for building data pipelines with the Medallion Architecture.

Getting Started
---------------

If you're new to SparkForge, start with the `Quick Start Guide <quick_start_5_min.html>`_ to get up and running in minutes.

Core Concepts
-------------

Medallion Architecture
~~~~~~~~~~~~~~~~~~~~~~

SparkForge implements the Medallion Architecture with three distinct layers:

- **Bronze Layer**: Raw data ingestion and initial validation
- **Silver Layer**: Cleaned, enriched, and transformed data
- **Gold Layer**: Business-ready analytics and reporting datasets

Validation System
~~~~~~~~~~~~~~~~~

SparkForge includes a robust validation system that ensures data quality from the start:

- **Early Validation**: Invalid configurations are rejected during construction
- **Required Rules**: All step types must have non-empty validation rules
- **Clear Error Messages**: Detailed error messages help you fix issues quickly
- **Type Safety**: Transform functions and dependencies are validated

.. code-block:: python

   # ✅ Valid - has required rules
   BronzeStep(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]}
   )

   # ❌ Invalid - empty rules rejected
   BronzeStep(
       name="events",
       rules={}  # ValidationError: Rules must be a non-empty dictionary
   )

Pipeline Building
~~~~~~~~~~~~~~~~~

Use the PipelineBuilder to construct your data pipeline:

.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import functions as F

   # Initialize builder
   builder = PipelineBuilder(spark=spark, schema="analytics")

   # Add Bronze validation
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"
   )

   # Add Silver transformation
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=clean_transform,
       rules={"status": [F.col("status").isNotNull()]},
       table_name="clean_events"
   )

   # Add Gold aggregation
   builder.add_gold_transform(
       name="daily_metrics",
       transform=aggregate_transform,
       rules={"date": [F.col("date").isNotNull()]},
       table_name="daily_metrics"
   )

Execution Modes
~~~~~~~~~~~~~~~

SparkForge supports different execution modes:

- **Initial Load**: Process all data from scratch
- **Incremental**: Process only new/changed data (coming soon)
- **Validation Only**: Run validation without writing data

.. code-block:: python

   # Initial load
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})

   # Validation only
   result = pipeline.run_validation(bronze_sources={"events": source_df})

Data Validation
---------------

Validation Rules
~~~~~~~~~~~~~~~~

Define data quality rules using PySpark Column expressions:

.. code-block:: python

   rules = {
       "user_id": [F.col("user_id").isNotNull()],
       "email": [F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$")],
       "age": [F.col("age").between(0, 120)],
       "status": [F.col("status").isin(["active", "inactive", "pending"])]
   }

Common Validation Patterns
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Null Checks**
.. code-block:: python

   "column_name": [F.col("column_name").isNotNull()]

**Range Validation**
.. code-block:: python

   "value": [F.col("value").between(0, 1000)]

**Pattern Matching**
.. code-block:: python

   "email": [F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$")]

**Value Lists**
.. code-block:: python

   "status": [F.col("status").isin(["active", "inactive", "pending"])]

**Complex Conditions**
.. code-block:: python

   "valid_data": [F.col("value") > 0, F.col("status") == "active"]

Validation Thresholds
~~~~~~~~~~~~~~~~~~~~~

Configure validation thresholds for each layer:

.. code-block:: python

   from sparkforge.models import ValidationThresholds

   thresholds = ValidationThresholds(
       bronze=95.0,  # 95% of bronze data must pass validation
       silver=98.0,  # 98% of silver data must pass validation
       gold=99.0     # 99% of gold data must pass validation
   )

Error Handling
--------------

SparkForge provides comprehensive error handling:

**Pipeline Errors**
.. code-block:: python

   try:
       result = pipeline.run_initial_load(bronze_sources={"events": df})
   except PipelineError as e:
       print(f"Pipeline failed: {e}")
       print(f"Error details: {e.context}")

**Validation Errors**
.. code-block:: python

   try:
       result = pipeline.run_initial_load(bronze_sources={"events": df})
   except ValidationError as e:
       print(f"Validation failed: {e}")
       print(f"Failed rules: {e.failed_rules}")

**Step Errors**
.. code-block:: python

   try:
       result = pipeline.run_initial_load(bronze_sources={"events": df})
   except StepError as e:
       print(f"Step failed: {e}")
       print(f"Step name: {e.context.get('step_name')}")

Logging and Monitoring
----------------------

SparkForge includes built-in logging and monitoring:

**Pipeline Logging**
.. code-block:: python

   from sparkforge.logging import PipelineLogger

   logger = PipelineLogger(level="INFO")
   builder = PipelineBuilder(spark=spark, schema="analytics", logger=logger)

**Execution Monitoring**
.. code-block:: python

   result = pipeline.run_initial_load(bronze_sources={"events": df})

   print(f"Status: {result.status}")
   print(f"Total steps: {result.total_steps}")
   print(f"Successful steps: {result.successful_steps}")
   print(f"Failed steps: {result.failed_steps}")
   print(f"Duration: {result.duration_seconds} seconds")

**Step-by-Step Debugging**
.. code-block:: python

   # Execute individual steps for debugging
   bronze_result = pipeline.execute_bronze_step("events", {"events": df})
   silver_result = pipeline.execute_silver_step("clean_events", {"events": df})

Advanced Features
-----------------

Multi-Schema Support
~~~~~~~~~~~~~~~~~~~~

Work with multiple schemas for different environments:

.. code-block:: python

   # Development schema
   dev_builder = PipelineBuilder(spark=spark, schema="dev_analytics")

   # Production schema
   prod_builder = PipelineBuilder(spark=spark, schema="prod_analytics")

Auto-Inference
~~~~~~~~~~~~~~

SparkForge can automatically infer dependencies:

.. code-block:: python

   # Auto-infer silver step dependencies
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",  # Automatically inferred
       transform=clean_transform,
       rules={"status": [F.col("status").isNotNull()]},
       table_name="clean_events"
   )

Column Filtering
~~~~~~~~~~~~~~~~

Control which columns are preserved after validation:

.. code-block:: python

   # Only keep columns with validation rules
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       filter_columns_by_rules=True
   )

Incremental Processing
~~~~~~~~~~~~~~~~~~~~~~

Enable incremental processing with timestamp columns:

.. code-block:: python

   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"  # Enable watermarking
   )

Performance Optimization
------------------------

Best Practices
~~~~~~~~~~~~~~

**1. Use Appropriate Data Types**
.. code-block:: python

   # Use appropriate data types for better performance
   df = df.withColumn("timestamp", F.col("timestamp").cast("timestamp"))

**2. Optimize Validation Rules**
.. code-block:: python

   # Combine multiple conditions into single rule when possible
   "valid_user": [F.col("user_id").isNotNull() & F.col("email").isNotNull()]

**3. Use Incremental Processing**
.. code-block:: python

   # Enable incremental processing for large datasets
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"
   )

**4. Monitor Performance**
.. code-block:: python

   # Check execution metrics
   result = pipeline.run_initial_load(bronze_sources={"events": df})
   print(f"Execution time: {result.duration_seconds} seconds")

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**1. "No module named 'sparkforge'"**
- Solution: Run ``pip install sparkforge``

**2. "Java gateway process exited"**
- Solution: Install Java 8+ and set JAVA_HOME

**3. "Table not found"**
- Solution: Run ``pipeline.run_initial_load()`` before accessing tables

**4. "Validation failed"**
- Solution: Check your data against validation rules

**5. "Step execution failed"**
- Solution: Check step dependencies and transform functions

Debugging Tips
~~~~~~~~~~~~~~

**1. Use Step-by-Step Execution**
.. code-block:: python

   # Execute individual steps
   bronze_result = pipeline.execute_bronze_step("events", {"events": df})
   print(f"Bronze step result: {bronze_result.status}")

**2. Check Data Quality**
.. code-block:: python

   # Inspect your data
   df.show()
   df.printSchema()
   df.describe().show()

**3. Validate Rules**
.. code-block:: python

   # Test validation rules
   valid_df = df.filter(F.col("user_id").isNotNull())
   print(f"Valid rows: {valid_df.count()}/{df.count()}")

**4. Check Dependencies**
.. code-block:: python

   # Validate pipeline dependencies
   errors = builder.validate_pipeline()
   if errors:
       print(f"Pipeline validation errors: {errors}")

Best Practices
--------------

**1. Start Simple**
- Begin with basic validation rules
- Add complexity gradually
- Test each step independently

**2. Use Meaningful Names**
- Choose descriptive step names
- Use consistent naming conventions
- Document your pipeline logic

**3. Handle Errors Gracefully**
- Implement proper error handling
- Log errors for debugging
- Provide meaningful error messages

**4. Monitor Performance**
- Track execution times
- Monitor data quality metrics
- Optimize based on performance data

**5. Test Thoroughly**
- Test with sample data
- Validate edge cases
- Test error conditions

Next Steps
----------

Now that you understand the core concepts, explore:

1. **`Examples <examples/index.html>`_**: Real-world pipeline examples
2. **`API Reference <api_reference.html>`_**: Detailed API documentation
3. **`Troubleshooting <troubleshooting.html>`_**: Common issues and solutions
4. **`Migration Guides <migration_guides.html>`_**: Upgrading from older versions

Happy data processing! 🚀
