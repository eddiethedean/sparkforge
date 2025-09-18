API Reference
=============

This section provides comprehensive API documentation for all SparkForge classes, methods, and functions.

.. note::

   **Note for Read the Docs**: The interactive API documentation below requires PySpark to be installed.
   If you're viewing this on Read the Docs, the classes may not be fully documented due to missing dependencies.
   For complete API documentation with examples, see the full reference below.

.. important::

   **Validation System**: SparkForge now includes a robust validation system that enforces data quality requirements:

   - **BronzeStep**: Must have non-empty validation rules
   - **SilverStep**: Must have non-empty validation rules, valid transform function, and valid source_bronze (except for existing tables)
   - **GoldStep**: Must have non-empty validation rules and valid transform function

   Invalid configurations are rejected during construction with clear error messages, ensuring data quality from the start.

Core Classes
------------

PipelineBuilder
~~~~~~~~~~~~~~~

The main class for building data pipelines with the Medallion Architecture.

.. autoclass:: sparkforge.pipeline.builder.PipelineBuilder
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineRunner
~~~~~~~~~~~~~~

The simplified pipeline runner for executing data pipelines.

.. autoclass:: sparkforge.pipeline.runner.SimplePipelineRunner
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionEngine
~~~~~~~~~~~~~~~

The simplified execution engine for processing pipeline steps.

.. autoclass:: sparkforge.execution.ExecutionEngine
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Data Models
-----------

BronzeStep
~~~~~~~~~~

Configuration for Bronze layer steps (raw data validation and ingestion).

.. autoclass:: sparkforge.models.BronzeStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SilverStep
~~~~~~~~~~

Configuration for Silver layer steps (data cleaning and enrichment).

.. autoclass:: sparkforge.models.SilverStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

GoldStep
~~~~~~~~

Configuration for Gold layer steps (business analytics and reporting).

.. autoclass:: sparkforge.models.GoldStep
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineConfig
~~~~~~~~~~~~~~

Main pipeline configuration.

.. autoclass:: sparkforge.models.PipelineConfig
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ValidationThresholds
~~~~~~~~~~~~~~~~~~~~

Validation thresholds for each pipeline layer.

.. autoclass:: sparkforge.models.ValidationThresholds
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ParallelConfig
~~~~~~~~~~~~~~

Configuration for parallel execution.

.. autoclass:: sparkforge.models.ParallelConfig
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Execution Models
----------------

StepExecutionResult
~~~~~~~~~~~~~~~~~~~

Result of executing a single pipeline step.

.. autoclass:: sparkforge.execution.StepExecutionResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionResult
~~~~~~~~~~~~~~~

Result of executing a complete pipeline.

.. autoclass:: sparkforge.execution.ExecutionResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineReport
~~~~~~~~~~~~~~

Report of pipeline execution results.

.. autoclass:: sparkforge.models.PipelineReport
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Enums
-----

ExecutionMode
~~~~~~~~~~~~~

Pipeline execution modes.

.. autoclass:: sparkforge.execution.ExecutionMode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepStatus
~~~~~~~~~~

Step execution status.

.. autoclass:: sparkforge.execution.StepStatus
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepType
~~~~~~~~

Types of pipeline steps.

.. autoclass:: sparkforge.execution.StepType
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineStatus
~~~~~~~~~~~~~~

Pipeline execution status.

.. autoclass:: sparkforge.models.PipelineStatus
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineMode
~~~~~~~~~~~~

Pipeline execution modes.

.. autoclass:: sparkforge.models.PipelineMode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Error Classes
-------------

SparkForgeError
~~~~~~~~~~~~~~~

Base exception for all SparkForge errors.

.. autoclass:: sparkforge.errors.SparkForgeError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ValidationError
~~~~~~~~~~~~~~~

Error raised when data validation fails.

.. autoclass:: sparkforge.errors.ValidationError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ExecutionError
~~~~~~~~~~~~~~

Error raised when step execution fails.

.. autoclass:: sparkforge.errors.ExecutionError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

ConfigurationError
~~~~~~~~~~~~~~~~~~

Error raised when pipeline configuration is invalid.

.. autoclass:: sparkforge.errors.ConfigurationError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PipelineError
~~~~~~~~~~~~~

Error raised when pipeline execution fails.

.. autoclass:: sparkforge.errors.PipelineError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

DataError
~~~~~~~~~

Error raised when data processing fails.

.. autoclass:: sparkforge.errors.DataError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

SystemError
~~~~~~~~~~~

Error raised when system-level operations fail.

.. autoclass:: sparkforge.errors.SystemError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

PerformanceError
~~~~~~~~~~~~~~~~

Error raised when performance issues are detected.

.. autoclass:: sparkforge.errors.PerformanceError
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Logging
-------

PipelineLogger
~~~~~~~~~~~~~~

Simplified logger for pipeline execution.

.. autoclass:: sparkforge.logging.PipelineLogger
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Utility Functions
-----------------

get_logger
~~~~~~~~~~

Get the global logger instance.

.. autofunction:: sparkforge.logging.get_logger

set_logger
~~~~~~~~~~

Set the global logger instance.

.. autofunction:: sparkforge.logging.set_logger

create_logger
~~~~~~~~~~~~~

Create a new logger instance.

.. autofunction:: sparkforge.logging.create_logger

Validation
----------

UnifiedValidator
~~~~~~~~~~~~~~~~

Unified validation system for data and pipeline validation.

.. autoclass:: sparkforge.validation.UnifiedValidator
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

apply_column_rules
~~~~~~~~~~~~~~~~~~

Apply validation rules to a DataFrame.

.. autofunction:: sparkforge.validation.apply_column_rules

assess_data_quality
~~~~~~~~~~~~~~~~~~~

Assess data quality metrics.

.. autofunction:: sparkforge.validation.assess_data_quality

get_dataframe_info
~~~~~~~~~~~~~~~~~~

Get DataFrame information and statistics.

.. autofunction:: sparkforge.validation.get_dataframe_info

Dependencies
------------

DependencyAnalyzer
~~~~~~~~~~~~~~~~~~

Analyzer for pipeline dependencies.

.. autoclass:: sparkforge.dependencies.DependencyAnalyzer
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

DependencyGraph
~~~~~~~~~~~~~~~

Graph representation of pipeline dependencies.

.. autoclass:: sparkforge.dependencies.DependencyGraph
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

DependencyAnalysisResult
~~~~~~~~~~~~~~~~~~~~~~~~

Result of dependency analysis.

.. autoclass:: sparkforge.dependencies.DependencyAnalysisResult
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

StepNode
~~~~~~~~

Node in the dependency graph.

.. autoclass:: sparkforge.dependencies.StepNode
   :members:
   :undoc-members:
   :show-inheritance:
   :noindex:

Table Operations
----------------

fqn
~~~

Generate fully qualified table name.

.. autofunction:: sparkforge.table_operations.fqn

Type Definitions
----------------

ColumnRules
~~~~~~~~~~~

Type alias for column validation rules.

.. autodata:: sparkforge.types.ColumnRules

TransformFunction
~~~~~~~~~~~~~~~~~

Type alias for transform functions.

.. autodata:: sparkforge.types.TransformFunction

SilverTransformFunction
~~~~~~~~~~~~~~~~~~~~~~~

Type alias for silver transform functions.

.. autodata:: sparkforge.types.SilverTransformFunction

GoldTransformFunction
~~~~~~~~~~~~~~~~~~~~~

Type alias for gold transform functions.

.. autodata:: sparkforge.types.GoldTransformFunction

ExecutionConfig
~~~~~~~~~~~~~~~

Type alias for execution configuration.

.. autodata:: sparkforge.types.ExecutionConfig

PipelineConfig
~~~~~~~~~~~~~~

Type alias for pipeline configuration.

.. autodata:: sparkforge.types.PipelineConfig

ValidationConfig
~~~~~~~~~~~~~~~~

Type alias for validation configuration.

.. autodata:: sparkforge.types.ValidationConfig

MonitoringConfig
~~~~~~~~~~~~~~~~

Type alias for monitoring configuration.

.. autodata:: sparkforge.types.MonitoringConfig

Examples
--------

Basic Pipeline
~~~~~~~~~~~~~~

.. code-block:: python

   from sparkforge import PipelineBuilder
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

   from sparkforge.errors import ValidationError, ExecutionError, PipelineError

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

   from sparkforge.logging import PipelineLogger

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

   from sparkforge.validation import apply_column_rules, assess_data_quality

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

   from sparkforge.dependencies import DependencyAnalyzer

   # Analyze dependencies
   analyzer = DependencyAnalyzer()
   result = analyzer.analyze_pipeline(bronze_steps, silver_steps, gold_steps)

   # Get execution order
   execution_order = result.execution_order
   print(f"Execution order: {execution_order}")

For more examples, see the `Examples <examples/index.html>`_ section.
