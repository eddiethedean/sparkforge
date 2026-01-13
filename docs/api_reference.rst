API Reference
=============

This section provides comprehensive API documentation for all PipelineBuilder classes, methods, and functions.

.. note::

   **Note for Read the Docs**: The interactive API documentation below requires PySpark to be installed.
   If you're viewing this on Read the Docs, the classes may not be fully documented due to missing dependencies.
   For complete API documentation with examples, see the full reference below.

.. important::

   **Engine Configuration Required**: Before using PipelineBuilder, you must configure the engine:

   .. code-block:: python

      from pipeline_builder.engine_config import configure_engine
      from pyspark.sql import SparkSession

      spark = SparkSession.builder.getOrCreate()
      configure_engine(spark=spark)

   **Validation System**: PipelineBuilder includes a robust validation system that enforces data quality requirements:

   - **BronzeStep**: Must have non-empty validation rules
   - **SilverStep**: Must have non-empty validation rules, valid transform function, and valid source_bronze
   - **GoldStep**: Must have non-empty validation rules and valid transform function

   Invalid configurations are rejected during construction with clear error messages, ensuring data quality from the start.

Core Classes
------------

PipelineBuilder
~~~~~~~~~~~~~~~

The main class for building data pipelines with the Medallion Architecture.

.. autoclass:: pipeline_builder.pipeline.builder.PipelineBuilder
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionEngine
~~~~~~~~~~~~~~~

The execution engine for processing pipeline steps with service-oriented architecture.

.. autoclass:: pipeline_builder.execution.ExecutionEngine
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Step Executors
--------------

BronzeStepExecutor
~~~~~~~~~~~~~~~~~~

Executor for Bronze layer steps.

.. autoclass:: pipeline_builder.step_executors.bronze.BronzeStepExecutor
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SilverStepExecutor
~~~~~~~~~~~~~~~~~~

Executor for Silver layer steps.

.. autoclass:: pipeline_builder.step_executors.silver.SilverStepExecutor
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

GoldStepExecutor
~~~~~~~~~~~~~~~~

Executor for Gold layer steps.

.. autoclass:: pipeline_builder.step_executors.gold.GoldStepExecutor
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Services
--------

ExecutionValidator
~~~~~~~~~~~~~~~~~~

Service for validating data during pipeline execution.

.. autoclass:: pipeline_builder.validation.execution_validator.ExecutionValidator
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

TableService
~~~~~~~~~~~~

Service for table operations and schema management.

.. autoclass:: pipeline_builder.storage.table_service.TableService
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

WriteService
~~~~~~~~~~~~

Service for writing DataFrames to tables.

.. autoclass:: pipeline_builder.storage.write_service.WriteService
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SchemaManager
~~~~~~~~~~~~~

Service for schema validation and management.

.. autoclass:: pipeline_builder.storage.schema_manager.SchemaManager
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

TransformService
~~~~~~~~~~~~~~~~

Service for applying transformations to DataFrames.

.. autoclass:: pipeline_builder.transformation.transform_service.TransformService
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionReporter
~~~~~~~~~~~~~~~~~

Service for creating execution reports.

.. autoclass:: pipeline_builder.reporting.execution_reporter.ExecutionReporter
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ErrorHandler
~~~~~~~~~~~~

Centralized error handler for pipeline operations.

.. autoclass:: pipeline_builder.errors.error_handler.ErrorHandler
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Data Models
-----------

BronzeStep
~~~~~~~~~~

Configuration for Bronze layer steps (raw data validation and ingestion).

.. autoclass:: pipeline_builder.models.steps.BronzeStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SilverStep
~~~~~~~~~~

Configuration for Silver layer steps (data cleaning and enrichment).

.. autoclass:: pipeline_builder.models.steps.SilverStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

GoldStep
~~~~~~~~

Configuration for Gold layer steps (business analytics and reporting).

.. autoclass:: pipeline_builder.models.steps.GoldStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineConfig
~~~~~~~~~~~~~~

Main pipeline configuration.

.. autoclass:: pipeline_builder.models.pipeline.PipelineConfig
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ValidationThresholds
~~~~~~~~~~~~~~~~~~~~~

Validation thresholds for each pipeline layer.

.. autoclass:: pipeline_builder.models.base.ValidationThresholds
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Execution Models
----------------

StepExecutionResult
~~~~~~~~~~~~~~~~~~~

Result of executing a single pipeline step.

.. autoclass:: pipeline_builder.execution.StepExecutionResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionResult
~~~~~~~~~~~~~~~

Result of executing a complete pipeline.

.. autoclass:: pipeline_builder.execution.ExecutionResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionContext
~~~~~~~~~~~~~~~

Context for pipeline execution.

.. autoclass:: pipeline_builder.models.execution.ExecutionContext
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepResult
~~~~~~~~~~

Result of executing a single step.

.. autoclass:: pipeline_builder.models.execution.StepResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Enums
-----

ExecutionMode
~~~~~~~~~~~~~

Pipeline execution modes.

.. autoclass:: pipeline_builder.execution.ExecutionMode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepStatus
~~~~~~~~~~

Step execution status.

.. autoclass:: pipeline_builder.execution.StepStatus
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepType
~~~~~~~~

Types of pipeline steps.

.. autoclass:: pipeline_builder.execution.StepType
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelinePhase
~~~~~~~~~~~~~

Pipeline phases (Bronze, Silver, Gold).

.. autoclass:: pipeline_builder.models.enums.PipelinePhase
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

WriteMode
~~~~~~~~~

Write modes for table operations.

.. autoclass:: pipeline_builder.models.enums.WriteMode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ValidationResult
~~~~~~~~~~~~~~~

Validation result status.

.. autoclass:: pipeline_builder.models.enums.ValidationResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Error Classes
-------------

PipelineConfigurationError
~~~~~~~~~~~~~~~~~~~~~~~~~~

Error raised when pipeline configuration is invalid.

.. autoclass:: pipeline_builder.models.exceptions.PipelineConfigurationError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineExecutionError
~~~~~~~~~~~~~~~~~~~~~~

Error raised when pipeline execution fails.

.. autoclass:: pipeline_builder.models.exceptions.PipelineExecutionError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Compatibility Layer
-------------------

Functions
~~~~~~~~~

Get default functions from the configured engine.

.. autofunction:: pipeline_builder.functions.get_default_functions

Compatibility Module
~~~~~~~~~~~~~~~~~~~~~

Protocol-based compatibility layer for SparkForge.

.. automodule:: pipeline_builder.compat
   :members:
   :undoc-members:
   :noindex:

Table Operations
----------------

Table operation utilities.

.. automodule:: pipeline_builder.table_operations
   :members:
   :undoc-members:
   :noindex:

Dependencies
------------

DependencyGraph
~~~~~~~~~~~~~~~

Graph representation of pipeline dependencies.

.. autoclass:: pipeline_builder.dependencies.graph.DependencyGraph
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepNode
~~~~~~~~

Node in the dependency graph.

.. autoclass:: pipeline_builder.dependencies.graph.StepNode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

LogWriter
--------

LogWriter
~~~~~~~~~

Writer for logging pipeline execution results.

.. autoclass:: pipeline_builder.writer.core.LogWriter
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Examples
--------

Basic Pipeline
~~~~~~~~~~~~~~

.. code-block:: python

   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.functions import get_default_functions
   from pyspark.sql import SparkSession

   # Configure engine (required!)
   spark = SparkSession.builder.getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   # Create pipeline
   builder = PipelineBuilder(spark=spark, schema="analytics")

   # Add Bronze step
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"
   )

   # Add Silver step
   def clean_events(spark, bronze_df, prior_silvers):
       return bronze_df.filter(F.col("status") == "active")

   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=clean_events,
       rules={"status": [F.col("status").isNotNull()]},
       table_name="clean_events"
   )

   # Add Gold step
   def daily_metrics(spark, silvers):
       return silvers["clean_events"].groupBy("date").count()

   builder.add_gold_transform(
       name="daily_metrics",
       transform=daily_metrics,
       rules={"date": [F.col("date").isNotNull()]},
       table_name="daily_metrics",
       source_silvers=["clean_events"]
   )

   # Execute pipeline
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})

   print(f"Pipeline completed: {result.status.value}")
   print(f"Rows written: {result.metrics.total_rows_written}")

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

   from pipeline_builder.models.exceptions import (
       PipelineConfigurationError,
       PipelineExecutionError
   )

   try:
       result = pipeline.run_initial_load(bronze_sources={"events": df})
   except PipelineConfigurationError as e:
       print(f"Configuration error: {e}")
   except PipelineExecutionError as e:
       print(f"Execution error: {e}")

Service Usage
~~~~~~~~~~~~~

.. code-block:: python

   from pipeline_builder.execution import ExecutionEngine
   from pipeline_builder.models import PipelineConfig

   # Create execution engine (services are initialized internally)
   config = PipelineConfig.create_default(schema="analytics")
   engine = ExecutionEngine(spark=spark, config=config)

   # Services are available as attributes:
   # - engine.validator (ExecutionValidator)
   # - engine.table_service (TableService)
   # - engine.write_service (WriteService)
   # - engine.transform_service (TransformService)
   # - engine.reporter (ExecutionReporter)
   # - engine.error_handler (ErrorHandler)

Logging
~~~~~~~

.. code-block:: python

   from pipeline_builder.writer import LogWriter

   # Create LogWriter (new simplified API)
   writer = LogWriter(
       spark=spark,
       schema="monitoring",
       table_name="pipeline_logs"
   )

   # Log execution result
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})
   writer.append(result)

   # Query logs
   logs = spark.table("monitoring.pipeline_logs")
   logs.show()

For more examples, see the `Examples <examples/index.html>`_ section.

Migration Guide
----------------

From Old API to New API
~~~~~~~~~~~~~~~~~~~~~~~

**Old API (Deprecated):**

.. code-block:: python

   builder.add_bronze_step("events", transform_func, rules)
   builder.add_silver_step("clean", transform_func, rules, source_bronze="events")
   builder.add_gold_step("metrics", "table", transform_func, rules, source_silvers=["clean"])

**New API:**

.. code-block:: python

   builder.with_bronze_rules(name="events", rules=rules, incremental_col="timestamp")
   builder.add_silver_transform(
       name="clean",
       source_bronze="events",
       transform=transform_func,
       rules=rules,
       table_name="clean_events"
   )
   builder.add_gold_transform(
       name="metrics",
       transform=transform_func,
       rules=rules,
       table_name="metrics",
       source_silvers=["clean"]
   )

**Key Changes:**

- ``add_bronze_step`` → ``with_bronze_rules`` (Bronze steps don't have transforms)
- ``add_silver_step`` → ``add_silver_transform``
- ``add_gold_step`` → ``add_gold_transform``
- Transform function signature changed: ``(spark, bronze_df, prior_silvers)`` for Silver, ``(spark, silvers)`` for Gold
- Execution methods: ``initial_load()`` → ``run_initial_load()``, ``incremental()`` → ``run_incremental()``
- Parallel execution removed: Sequential execution with dependency-aware ordering
- Engine configuration required: Must call ``configure_engine(spark=spark)`` before use
