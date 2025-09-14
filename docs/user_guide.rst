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

Execution Modes
~~~~~~~~~~~~~~~

SparkForge supports multiple execution modes:

- **Initial Load**: Process all data from scratch
- **Incremental**: Process only new/changed data
- **Full Refresh**: Force complete reprocessing
- **Validation Only**: Check data quality without writing

Advanced Features
-----------------

Parallel Execution
~~~~~~~~~~~~~~~~~~

Configure parallel execution for better performance:

.. code-block:: python

   builder = PipelineBuilder(
       spark=spark,
       schema="analytics",
       enable_parallel_silver=True,
       max_parallel_workers=4,
       execution_mode=ExecutionMode.ADAPTIVE
   )

Quality Thresholds
~~~~~~~~~~~~~~~~~~

Set data quality requirements for each layer:

.. code-block:: python

   builder = PipelineBuilder(
       spark=spark,
       schema="analytics",
       min_bronze_rate=95.0,  # 95% minimum quality for Bronze
       min_silver_rate=98.0,  # 98% minimum quality for Silver
       min_gold_rate=99.0     # 99% minimum quality for Gold
   )

Best Practices
--------------

1. **Start Simple**: Begin with basic validation rules and gradually add complexity
2. **Monitor Quality**: Set appropriate quality thresholds for your use case
3. **Use Incremental Processing**: Enable watermarking for efficient updates
4. **Test Thoroughly**: Use validation-only mode to test without side effects
5. **Monitor Performance**: Use parallel execution for independent steps

Additional Resources
--------------------

- `Quick Start Guide <quick_start_5_min.html>`_ - Get started in 5 minutes
- `API Reference <api_reference.html>`_ - Complete API documentation
- `Examples <examples/index.html>`_ - Working code samples
- `Troubleshooting <troubleshooting.html>`_ - Common issues and solutions

For the complete user guide with detailed examples, see: `USER_GUIDE.md <../USER_GUIDE.md>`_
