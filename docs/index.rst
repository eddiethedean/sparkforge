SparkForge Documentation
========================

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). Build scalable data pipelines with clean, maintainable code and comprehensive validation.

.. note::

   **Engine Configuration Required**: Before using SparkForge, you must configure the engine:

   .. code-block:: python

      from pipeline_builder.engine_config import configure_engine
      from pyspark.sql import SparkSession

      spark = SparkSession.builder.getOrCreate()
      configure_engine(spark=spark)

Quick Links
-----------

**For Beginners:**
- :doc:`getting_started` - Start here!
- :doc:`quick_start_5_min` - 5-minute quick start
- :doc:`hello_world` - Simplest example

**For Users:**
- :doc:`user_guide` - Comprehensive user guide
- :doc:`progressive_examples` - Step-by-step examples
- :doc:`api_reference` - Complete API reference

**For Developers:**
- :doc:`Architecture` - Architecture documentation
- :doc:`api_reference` - Detailed API documentation
- :doc:`DEPLOYMENT_GUIDE` - Deployment guide

Quick Start
-----------

Get up and running with SparkForge in under 5 minutes:

.. code-block:: bash

   pip install pyspark==3.5.0 delta-spark==3.0.0
   python examples/core/hello_world.py

.. code-block:: python

   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions
   from pyspark.sql import SparkSession

   # Configure engine (required!)
   spark = SparkSession.builder.appName("My Pipeline").getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   # Build pipeline
   builder = PipelineBuilder(spark=spark, schema="my_schema")
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]}
   )
   
   def clean_events(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       return bronze_df.filter(F.col("status") == "active")
   
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=clean_events,
       rules={"status": [F.col("status").isNotNull()]},
       table_name="clean_events"
   )
   
   def analytics(spark, silvers):
       F = get_default_functions()
       return silvers["clean_events"].groupBy("category").count()
   
   builder.add_gold_transform(
       name="analytics",
       transform=analytics,
       rules={"category": [F.col("category").isNotNull()]},
       table_name="analytics",
       source_silvers=["clean_events"]
   )

   # Execute
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})
   print(f"Status: {result.status.value}")

Features
--------

🏗️ **Medallion Architecture**
   Bronze → Silver → Gold data layering with automatic dependency management

⚡ **Service-Oriented Design**
   Modular architecture with dedicated services for validation, storage, transformation, and reporting

🎯 **Automatic Dependency Management**
   Automatically analyzes step dependencies and executes in correct order

🛠️ **Engine Configuration**
   Works with both real PySpark and mock Spark for testing

🔧 **Validation System**
   Built-in validation with configurable thresholds and string rule support

📊 **Incremental Processing**
   Efficient incremental updates with Delta Lake watermarking

💧 **Delta Lake Integration**
   Full support for ACID transactions, time travel, and schema evolution

🔍 **Step Executors**
   Dedicated executors for Bronze, Silver, and Gold steps

✅ **Comprehensive Error Handling**
   Centralized error handling with detailed context and suggestions

📈 **Execution Reporting**
   Detailed execution reports with metrics and timing

Documentation by User Type
---------------------------

For Beginners
~~~~~~~~~~~~~

New to SparkForge? Start here:

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   getting_started
   quick_start_5_min
   hello_world
   QUICKSTART

For Users
~~~~~~~~~

Building pipelines? These guides are for you:

.. toctree::
   :maxdepth: 2
   :caption: User Guides

   USER_GUIDE
   progressive_examples
   Architecture

.. toctree::
   :maxdepth: 2
   :caption: Use Cases

   usecase_ecommerce
   usecase_iot
   usecase_bi

For Developers
~~~~~~~~~~~~~

Advanced topics and API documentation:

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api_reference
   ENHANCED_API_REFERENCE
   writer_api_reference

.. toctree::
   :maxdepth: 2
   :caption: Technical Guides

   DEPLOYMENT_GUIDE
   PERFORMANCE_TUNING_GUIDE
   COMPREHENSIVE_TROUBLESHOOTING_GUIDE

Examples
--------

**Hello World** - The simplest possible pipeline

.. code-block:: python

   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions

   configure_engine(spark=spark)
   F = get_default_functions()

   builder = PipelineBuilder(spark=spark, schema="hello")
   builder.with_bronze_rules(
       name="events",
       rules={"user": [F.col("user").isNotNull()]}
   )
   
   def purchases_transform(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       return bronze_df.filter(F.col("action") == "purchase")
   
   builder.add_silver_transform(
       name="purchases",
       source_bronze="events",
       transform=purchases_transform,
       rules={"action": [F.col("action") == "purchase"]},
       table_name="purchases"
   )
   
   def user_counts_transform(spark, silvers):
       F = get_default_functions()
       return silvers["purchases"].groupBy("user").count()
   
   builder.add_gold_transform(
       name="user_counts",
       transform=user_counts_transform,
       rules={"user": [F.col("user").isNotNull()]},
       table_name="user_counts",
       source_silvers=["purchases"]
   )

   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})

Installation
------------

.. code-block:: bash

   pip install pyspark==3.5.0 delta-spark==3.0.0

Prerequisites:
- Python 3.8+
- Java 17 (for Spark 3.5)
- PySpark 3.5+
- Delta Lake 3.0.0+

Key Benefits
------------

**Simplified Development**
   Clean, maintainable code with minimal boilerplate

**Production Ready**
   Built-in error handling, logging, and monitoring

**Service-Oriented Architecture**
   Modular design with dedicated services for each concern

**Scalable Architecture**
   Designed for enterprise-scale data processing

**Delta Lake Integration**
   ACID transactions, time travel, and schema evolution

**Comprehensive Testing**
   Extensive test suite with high coverage

Support
-------

- **Documentation**: Complete guides and API reference
- **Examples**: Real-world pipeline examples in `examples/` directory
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
