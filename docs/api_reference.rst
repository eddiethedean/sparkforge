API Reference
=============

This section provides comprehensive API documentation for all PipelineBuilder classes, methods, and functions.

.. note::

   **Note for Read the Docs**: The interactive API documentation below requires PySpark to be installed.
   If you're viewing this on Read the Docs, the classes may not be fully documented due to missing dependencies.
   For complete API documentation with examples, see the full reference below.

.. important::

   **Validation System**: PipelineBuilder now includes a robust validation system that enforces data quality requirements:

   - **BronzeStep**: Must have non-empty validation rules
   - **SilverStep**: Must have non-empty validation rules, valid transform function, and valid source_bronze (except for existing tables)
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

PipelineRunner
~~~~~~~~~~~~~~

The simplified pipeline runner for executing data pipelines.

.. autoclass:: pipeline_builder.pipeline.runner.SimplePipelineRunner
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionEngine
~~~~~~~~~~~~~~~

The simplified execution engine for processing pipeline steps.

.. autoclass:: pipeline_builder.execution.ExecutionEngine
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Data Models
-----------

BronzeStep
~~~~~~~~~~

Configuration for Bronze layer steps (raw data validation and ingestion).

.. autoclass:: pipeline_builder.models.BronzeStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SilverStep
~~~~~~~~~~

Configuration for Silver layer steps (data cleaning and enrichment).

.. autoclass:: pipeline_builder.models.SilverStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

GoldStep
~~~~~~~~

Configuration for Gold layer steps (business analytics and reporting).

.. autoclass:: pipeline_builder.models.GoldStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineConfig
~~~~~~~~~~~~~~

Main pipeline configuration.

.. autoclass:: pipeline_builder.models.PipelineConfig
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ValidationThresholds
~~~~~~~~~~~~~~~~~~~~

Validation thresholds for each pipeline layer.

.. autoclass:: pipeline_builder.models.ValidationThresholds
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ParallelConfig
~~~~~~~~~~~~~~

Configuration for parallel execution.

.. autoclass:: pipeline_builder.models.ParallelConfig
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

PipelineReport
~~~~~~~~~~~~~~

Report of pipeline execution results.

.. autoclass:: pipeline_builder.models.PipelineReport
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

PipelineStatus
~~~~~~~~~~~~~~

Pipeline execution status.

.. autoclass:: pipeline_builder.models.PipelineStatus
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineMode
~~~~~~~~~~~~

Pipeline execution modes.

.. autoclass:: pipeline_builder.models.PipelineMode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Error Classes
-------------

SparkForgeError
~~~~~~~~~~~~~~~

Base exception for all SparkForge errors.

.. autoclass:: pipeline_builder.errors.SparkForgeError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ValidationError
~~~~~~~~~~~~~~~

Error raised when data validation fails.

.. autoclass:: pipeline_builder.errors.ValidationError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionError
~~~~~~~~~~~~~~

Error raised when step execution fails.

.. autoclass:: pipeline_builder.errors.ExecutionError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ConfigurationError
~~~~~~~~~~~~~~~~~~

Error raised when pipeline configuration is invalid.

.. autoclass:: pipeline_builder.errors.ConfigurationError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineError
~~~~~~~~~~~~~

Error raised when pipeline execution fails.

.. autoclass:: pipeline_builder.errors.PipelineError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

DataError
~~~~~~~~~

Error raised when data processing fails.

.. autoclass:: pipeline_builder.errors.DataError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SystemError
~~~~~~~~~~~

Error raised when system-level operations fail.

.. autoclass:: pipeline_builder.errors.SystemError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PerformanceError
~~~~~~~~~~~~~~~~

Error raised when performance issues are detected.

.. autoclass:: pipeline_builder.errors.PerformanceError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Logging
-------

PipelineLogger
~~~~~~~~~~~~~~

Simplified logger for pipeline execution.

.. autoclass:: pipeline_builder.logging.PipelineLogger
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Utility Functions
-----------------

get_logger
~~~~~~~~~~

Get the global logger instance.

.. autofunction:: pipeline_builder.logging.get_logger

set_logger
~~~~~~~~~~

Set the global logger instance.

.. autofunction:: pipeline_builder.logging.set_logger

create_logger
~~~~~~~~~~~~~

Create a new logger instance.

.. autofunction:: pipeline_builder.logging.create_logger

Validation
----------

UnifiedValidator
~~~~~~~~~~~~~~~~

Unified validation system for data and pipeline validation.

.. autoclass:: pipeline_builder.validation.UnifiedValidator
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

apply_column_rules
~~~~~~~~~~~~~~~~~~

Apply validation rules to a DataFrame.

.. autofunction:: pipeline_builder.validation.apply_column_rules

assess_data_quality
~~~~~~~~~~~~~~~~~~~

Assess data quality metrics.

.. autofunction:: pipeline_builder.validation.assess_data_quality

get_dataframe_info
~~~~~~~~~~~~~~~~~~

Get DataFrame information and statistics.

.. autofunction:: pipeline_builder.validation.get_dataframe_info

Dependencies
------------

DependencyAnalyzer
~~~~~~~~~~~~~~~~~~

Analyzer for pipeline dependencies.

.. autoclass:: pipeline_builder.dependencies.DependencyAnalyzer
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

DependencyGraph
~~~~~~~~~~~~~~~

Graph representation of pipeline dependencies.

.. autoclass:: pipeline_builder.dependencies.DependencyGraph
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

DependencyAnalysisResult
~~~~~~~~~~~~~~~~~~~~~~~~

Result of dependency analysis.

.. autoclass:: pipeline_builder.dependencies.DependencyAnalysisResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepNode
~~~~~~~~

Node in the dependency graph.

.. autoclass:: pipeline_builder.dependencies.StepNode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Table Operations
----------------

fqn
~~~

Generate fully qualified table name.

.. autofunction:: pipeline_builder.table_operations.fqn

Type Definitions
----------------

ColumnRules
~~~~~~~~~~~

Type alias for column validation rules.

.. autodata:: pipeline_builder.types.ColumnRules

TransformFunction
~~~~~~~~~~~~~~~~~

Type alias for transform functions.

.. autodata:: pipeline_builder.types.TransformFunction

SilverTransformFunction
~~~~~~~~~~~~~~~~~~~~~~~

Type alias for silver transform functions.

.. autodata:: pipeline_builder.types.SilverTransformFunction

GoldTransformFunction
~~~~~~~~~~~~~~~~~~~~~

Type alias for gold transform functions.

.. autodata:: pipeline_builder.types.GoldTransformFunction

ExecutionConfig
~~~~~~~~~~~~~~~

Type alias for execution configuration.

.. autodata:: pipeline_builder.types.ExecutionConfig

PipelineConfig
~~~~~~~~~~~~~~

Type alias for pipeline configuration.

.. autodata:: pipeline_builder.types.PipelineConfig

ValidationConfig
~~~~~~~~~~~~~~~~

Type alias for validation configuration.

.. autodata:: pipeline_builder.types.ValidationConfig

MonitoringConfig
~~~~~~~~~~~~~~~~

Type alias for monitoring configuration.

.. autodata:: pipeline_builder.types.MonitoringConfig

Examples
--------

Basic Pipeline
~~~~~~~~~~~~~~

.. code-block:: python

   from pipeline_builder import PipelineBuilder
   from pyspark.sql import functions as F

   # Create pipeline
   builder = PipelineBuilder(spark=spark, schema="analytics")

   # Add Bronze step
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"
   )

   # Add Silver step
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
       rules={"status": [F.col("status").isNotNull()]},
       table_name="clean_events"
   )

   # Add Gold step
   builder.add_gold_transform(
       name="daily_metrics",
       transform=lambda spark, silvers: silvers["clean_events"].groupBy("date").count(),
       rules={"date": [F.col("date").isNotNull()]},
       table_name="daily_metrics"
   )

   # Execute pipeline
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

   from pipeline_builder.errors import ValidationError, ExecutionError, PipelineError

   try:
       result = pipeline.run_initial_load(bronze_sources={"events": df})
   except ValidationError as e:
       print(f"Validation failed: {e}")
       print(f"Context: {e.context}")
   except ExecutionError as e:
       print(f"Execution failed: {e}")
       print(f"Step: {e.context.get('step_name')}")
   except PipelineError as e:
       print(f"Pipeline failed: {e}")
       print(f"Errors: {e.context.get('errors')}")

Logging
~~~~~~~

.. code-block:: python

   from pipeline_builder.logging import PipelineLogger

   # Create logger
   logger = PipelineLogger(level="INFO")

   # Use with pipeline
   builder = PipelineBuilder(spark=spark, schema="analytics", logger=logger)

   # Log messages
   logger.info("Starting pipeline execution")
   logger.error("Pipeline failed", extra={"step": "bronze"})

Validation
~~~~~~~~~~

.. code-block:: python

   from pipeline_builder.validation import apply_column_rules, assess_data_quality

   # Apply validation rules
   valid_df, invalid_df, stats = apply_column_rules(
       df, rules, stage="bronze", step="events"
   )

   # Assess data quality
   quality = assess_data_quality(df)
   print(f"Quality rate: {quality['quality_rate']}%")

Dependencies
~~~~~~~~~~~~

.. code-block:: python

   from pipeline_builder.dependencies import DependencyAnalyzer

   # Analyze dependencies
   analyzer = DependencyAnalyzer()
   result = analyzer.analyze_pipeline(bronze_steps, silver_steps, gold_steps)

   # Get execution order
   execution_order = result.execution_order
   print(f"Execution order: {execution_order}")

For more examples, see the `Examples <examples/index.html>`_ section.
