SparkForge Documentation
========================

A simplified, production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). Build scalable data pipelines with clean, maintainable code and comprehensive validation.

.. note::

   SparkForge provides a complete Medallion Architecture implementation with Bronze → Silver → Gold data layering.

Quick Start
-----------

Get up and running with SparkForge in under 5 minutes:

.. code-block:: bash

   pip install sparkforge
   python examples/hello_world.py

.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import SparkSession, functions as F

   # Start Spark
   spark = SparkSession.builder.appName("My Pipeline").getOrCreate()

   # Build pipeline
   builder = PipelineBuilder(spark=spark, schema="my_schema")
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]}
   )
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
       rules={"status": [F.col("status").isNotNull()]},
       table_name="clean_events"
   )
   builder.add_gold_transform(
       name="analytics",
       transform=lambda spark, silvers: silvers["clean_events"].groupBy("category").count(),
       rules={"category": [F.col("category").isNotNull()]},
       table_name="analytics",
       source_silvers=["clean_events"]
   )

   # Execute
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})

Features
--------

🏗️ **Medallion Architecture**
   Bronze → Silver → Gold data layering with automatic dependency management

⚡ **Simplified Execution**
   Clean, maintainable execution engine with step-by-step processing

🎯 **Auto-Inference**
   Automatically infers source dependencies, reducing boilerplate by 70%

🛠️ **Preset Configurations**
   One-line setup for development, production, and testing environments

🔧 **Validation Helpers**
   Built-in methods for common validation patterns (not_null, positive_numbers, etc.)

📊 **Smart Detection**
   Automatic timestamp column detection for watermarking

🏢 **Multi-Schema Support**
   Cross-schema data flows for multi-tenant, environment separation, and compliance

🔍 **Step-by-Step Debugging**
   Execute individual pipeline steps independently for troubleshooting

✅ **Enhanced Data Validation**
   Configurable validation thresholds with automatic security validation

🎛️ **Column Filtering Control**
   Explicit control over which columns are preserved after validation

🔄 **Incremental Processing**
   Watermarking and incremental updates with Delta Lake

💧 **Delta Lake Integration**
   Full support for ACID transactions, time travel, and schema evolution

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   quick_start_5_min
   getting_started
   hello_world

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user_guide
   progressive_examples
   quick_reference

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api_reference

.. toctree::
   :maxdepth: 2
   :caption: Examples

   examples/index

.. toctree::
   :maxdepth: 2
   :caption: Use Cases

   usecase_ecommerce
   usecase_iot
   usecase_bi

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   decision_trees
   migration_guides
   troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Development

   notebooks/index

Installation
------------

.. code-block:: bash

   pip install sparkforge

Prerequisites:
- Python 3.8+
- Java 8+ (for PySpark)
- PySpark 3.2.4+
- Delta Lake 1.2.0+

Examples
--------

**Hello World** - The simplest possible pipeline
.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import functions as F

   builder = PipelineBuilder(spark=spark, schema="hello")
   builder.with_bronze_rules(name="events", rules={"user": [F.col("user").isNotNull()]})
   builder.add_silver_transform(
       name="purchases",
       source_bronze="events",
       transform=lambda spark, df, silvers: df.filter(F.col("action") == "purchase"),
       rules={"action": [F.col("action") == "purchase"]},
       table_name="purchases"
   )
   builder.add_gold_transform(
       name="user_counts",
       transform=lambda spark, silvers: silvers["purchases"].groupBy("user").count(),
       rules={"user": [F.col("user").isNotNull()]},
       table_name="user_counts",
       source_silvers=["purchases"]
   )

   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})

**E-commerce Analytics** - Real-world business intelligence
.. code-block:: python

   # Track user behavior and purchases
   builder.with_bronze_rules(
       name="user_events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"
   )
   builder.add_silver_transform(
       name="user_sessions",
       source_bronze="user_events",
       transform=lambda spark, df, silvers: df.groupBy("user_id").agg(
           F.count("*").alias("event_count"),
           F.max("timestamp").alias("last_activity")
       ),
       rules={"event_count": [F.col("event_count") > 0]},
       table_name="user_sessions"
   )

**IoT Sensor Data** - Real-time sensor processing
.. code-block:: python

   # Process sensor readings with validation
   builder.with_bronze_rules(
       name="sensor_data",
       rules={
           "sensor_id": [F.col("sensor_id").isNotNull()],
           "temperature": [F.col("temperature").between(-50, 150)],
           "timestamp": [F.col("timestamp").isNotNull()]
       },
       incremental_col="timestamp"
   )

Key Benefits
------------

**Simplified Development**
   Clean, maintainable code with minimal boilerplate

**Production Ready**
   Built-in error handling, logging, and monitoring

**Scalable Architecture**
   Designed for enterprise-scale data processing

**Delta Lake Integration**
   ACID transactions, time travel, and schema evolution

**Comprehensive Testing**
   469+ tests with 100% success rate

**Active Community**
   Regular updates and community support

Support
-------

- **Documentation**: Complete guides and API reference
- **Examples**: Real-world pipeline examples
- **Community**: GitHub discussions and issues
- **Professional**: Enterprise support available

License
-------

This project is licensed under the MIT License - see the `LICENSE <https://github.com/eddiethedean/sparkforge/blob/main/LICENSE>`_ file for details.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
