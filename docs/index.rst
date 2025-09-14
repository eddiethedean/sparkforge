SparkForge Documentation
========================

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). Build scalable data pipelines with built-in parallel execution, comprehensive validation, and enterprise-grade monitoring.

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
   result = pipeline.initial_load(bronze_sources={"events": source_df})
   print(f"Pipeline completed: {result.success}")

Key Features
------------

🏗️ **Medallion Architecture**
   Bronze → Silver → Gold data layering with automatic dependency management and built-in validation.

⚡ **Parallel Execution**
   Independent Silver steps run concurrently for maximum performance with unified execution support.

🔍 **Step-by-Step Debugging**
   Execute individual pipeline steps independently for troubleshooting and development.

✅ **Data Validation**
   Configurable validation thresholds and comprehensive quality checks at every layer.

🔄 **Incremental Processing**
   Watermarking and incremental updates with Delta Lake for efficient data processing.

💧 **Delta Lake Integration**
   Full support for ACID transactions, time travel, and schema evolution.

Getting Started
---------------

Choose your learning path:

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   quick_start_5_min
   hello_world
   getting_started
   progressive_examples

Use Cases
---------

Build real-world pipelines:

.. toctree::
   :maxdepth: 2
   :caption: Use Cases

   usecase_ecommerce
   usecase_iot
   usecase_bi

Documentation
-------------

Complete documentation and references:

.. toctree::
   :maxdepth: 2
   :caption: Documentation

   user_guide
   quick_reference
   api_reference
   examples/index

Advanced Topics
---------------

Learn advanced features and best practices:

.. toctree::
   :maxdepth: 2
   :caption: Advanced Topics

   decision_trees
   visual_guide
   migration_guides
   troubleshooting

Interactive Learning
--------------------

Hands-on tutorials and examples:

.. toctree::
   :maxdepth: 2
   :caption: Interactive Learning

   notebooks/index
   examples/index

Installation
------------

Install SparkForge with pip:

.. code-block:: bash

   pip install sparkforge

Requirements:

- Python 3.8+
- Java 8+ (for PySpark)
- PySpark 3.2.4+
- Delta Lake 1.2.0+

For detailed installation instructions, see :doc:`getting_started`.

Examples
--------

Try these examples to get started:

**Hello World Example:**

.. code-block:: python

   from sparkforge import PipelineBuilder
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.appName("Hello World").getOrCreate()
   builder = PipelineBuilder(spark=spark, schema="hello_world")

   # Bronze: Validate data
   builder.with_bronze_rules(
       name="events",
       rules={"user": [F.col("user").isNotNull()]}
   )

   # Silver: Filter purchases
   builder.add_silver_transform(
       name="purchases",
       source_bronze="events",
       transform=lambda spark, df, silvers: df.filter(F.col("action") == "purchase"),
       rules={"action": [F.col("action") == "purchase"]},
       table_name="purchases"
   )

   # Gold: Count users
   builder.add_gold_transform(
       name="user_counts",
       transform=lambda spark, silvers: silvers["purchases"].groupBy("user").count(),
       rules={"user": [F.col("user").isNotNull()]},
       table_name="user_counts",
       source_silvers=["purchases"]
   )

   # Execute
   pipeline = builder.to_pipeline()
   result = pipeline.initial_load(bronze_sources={"events": source_df})

For more examples, see :doc:`usecase_ecommerce`, :doc:`usecase_iot`, and :doc:`examples/index`.

Community
---------

- **GitHub**: `https://github.com/eddiethedean/sparkforge <https://github.com/eddiethedean/sparkforge>`_
- **Issues**: Report bugs and request features
- **Discussions**: Ask questions and share solutions
- **Contributing**: Help improve SparkForge

License
-------

This project is licensed under the MIT License - see the `LICENSE <https://github.com/eddiethedean/sparkforge/blob/main/LICENSE>`_ file for details.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
