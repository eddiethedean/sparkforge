# Troubleshooting Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Installation Issues](#installation-issues)
3. [Engine Configuration Issues](#engine-configuration-issues)
4. [Pipeline Configuration and Validation](#pipeline-configuration-and-validation)
5. [Pipeline Execution Errors](#pipeline-execution-errors)
6. [Data Validation Issues](#data-validation-issues)
7. [Delta Lake and Table Issues](#delta-lake-and-table-issues)
8. [Related Guides](#related-guides)

---

## Introduction

This guide lists common issues and fixes when installing, configuring, and running SparkForge pipelines. All content is self-contained within the guides directory. For error types and handling patterns, see [Error Handling](./ERROR_HANDLING_GUIDE.md). For debugging a single step, see [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md).

## Installation Issues

**Python version**

- SparkForge requires **Python 3.8+**.
- **Symptom:** `ImportError` or `SyntaxError` when importing or running.
- **Fix:** Check with `python --version`. Use pyenv, conda, or system Python to install 3.8 or higher.

**Java**

- PySpark needs Java (e.g. **Java 17** for Spark 3.5).
- **Symptoms:** `Java gateway process exited before sending its port number`, or `JAVA_HOME is not set`.
- **Fix:** Install Java 17 (or the version that matches your Spark). Set `JAVA_HOME` and ensure `java -version` works. Example (Linux): `export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64` and add `$JAVA_HOME/bin` to `PATH`.

**Dependencies**

- **Symptom:** `ImportError` for pyspark, delta, or pipeline_builder.
- **Fix:** `pip install pyspark delta-spark pipeline-builder[pyspark]`. Resolve version conflicts in a virtual environment.

## Engine Configuration Issues

**Engine not configured**

- **Symptoms:** `RuntimeError: Engine not configured` or `AttributeError: 'NoneType' object has no attribute 'functions'`.
- **Cause:** Pipeline components were used before calling `configure_engine(spark=spark)`.
- **Fix:** Create the Spark session, then immediately call `configure_engine(spark=spark)`, and only then import and use PipelineBuilder or other pipeline components. See [Getting Started](./GETTING_STARTED_GUIDE.md).

**Functions not available**

- **Symptom:** `F.col` or similar fails with NoneType.
- **Fix:** Call `get_default_functions()` only after the engine is configured: `from pipeline_builder.functions import get_default_functions` and `F = get_default_functions()`.

## Pipeline Configuration and Validation

**Invalid schema or thresholds**

- **Symptoms:** `PipelineConfigurationError` or `ValidationError` about schema or thresholds.
- **Fix:** Use a non-empty schema name (e.g. `schema="analytics"`). Set `min_bronze_rate`, `min_silver_rate`, `min_gold_rate` between 0 and 100 if you use them.

**Empty or invalid rules**

- **Symptoms:** `Rules must be a non-empty dictionary` or similar.
- **Fix:** Pass a non-empty dict for `rules` on every step (bronze, silver, gold). Each key is a column name; each value is a list of expressions or string rules. See [Validation Rules](./VALIDATION_RULES_GUIDE.md).

**Transform not callable**

- **Symptom:** Error that transform must be callable.
- **Fix:** Pass a function (e.g. `def my_transform(spark, df, prior_silvers): ...`) for `transform`, not a string or other non-callable.

**Missing or wrong source step**

- **Symptoms:** `Bronze step 'X' not found`, `Silver steps not found`, or circular dependency.
- **Fix:** Ensure every `source_bronze` and `source_silvers` name matches a step you defined. Avoid circular dependencies (e.g. silver A depending on silver B and B depending on A). Run `builder.validate_pipeline()` and fix any reported errors.

## Pipeline Execution Errors

**Step execution failed**

- **Symptom:** Pipeline run fails; result status is not "completed"; step result has an error.
- **Fix:** Check `result.errors` and the per-step info in `result.bronze_results`, `result.silver_results`, `result.gold_results` for the failing step. Use [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) to run that step alone and inspect the DataFrame or exception. Ensure the transform does not assume columns or schemas that are not present.

**Empty DataFrame or no rows written**

- **Symptom:** Run completes but 0 rows processed or written.
- **Fix:** Check that input DataFrames are non-empty (or that incremental filter is not excluding all rows). Ensure validation rules are not filtering out every row. Inspect the step output with stepwise execution.

## Data Validation Issues

**Validation rate below threshold**

- **Symptom:** Run fails because validation rate is below `min_bronze_rate`, `min_silver_rate`, or `min_gold_rate`.
- **Fix:** Improve data quality or relax thresholds temporarily. Inspect which rows fail validation (e.g. run stepwise and count before/after validation). Adjust rules if they are too strict for the actual data. See [Validation Rules](./VALIDATION_RULES_GUIDE.md).

**Columns missing after transform**

- **Symptom:** Columns disappear or rules complain about missing columns.
- **Fix:** Only columns that have at least one validation rule are kept. Add rules for every column you want to preserve in that step’s output. See [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md).

## Delta Lake and Table Issues

**Delta extensions not configured**

- **Symptom:** Errors when writing or reading Delta tables.
- **Fix:** Configure Spark with Delta Lake extensions before creating the session:
  - `spark.sql.extensions`: `io.delta.sql.DeltaSparkSessionExtension`
  - `spark.sql.catalog.spark_catalog`: `org.apache.spark.sql.delta.catalog.DeltaCatalog`
  See [Getting Started](./GETTING_STARTED_GUIDE.md) and [Deployment](./DEPLOYMENT_GUIDE.md).

**Schema or table does not exist / permission denied**

- **Symptom:** Table or schema creation fails, or write permission denied.
- **Fix:** Ensure the Spark session has permission to create schemas and write to the warehouse path. Create the schema explicitly if required (e.g. `spark.sql("CREATE SCHEMA IF NOT EXISTS my_schema")`). Check storage credentials and paths (e.g. S3, HDFS).

**Table already exists / overwrite behavior**

- **Symptom:** Unexpected append or overwrite.
- **Fix:** `run_initial_load` and `run_full_refresh` overwrite target tables; `run_incremental` appends. See [Execution Modes](./EXECUTION_MODES_GUIDE.md).

## Related Guides

- [Error Handling](./ERROR_HANDLING_GUIDE.md) — Exception types and what to do.
- [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) — Debug one step at a time.
- [Getting Started](./GETTING_STARTED_GUIDE.md) — Engine and first pipeline.
- [Deployment](./DEPLOYMENT_GUIDE.md) — Production and Delta setup.
