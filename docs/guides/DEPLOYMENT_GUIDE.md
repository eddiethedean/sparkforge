# Deployment Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Local Development Setup](#local-development-setup)
4. [Production Checklist](#production-checklist)
5. [Spark and Delta Lake Configuration](#spark-and-delta-lake-configuration)
6. [Monitoring and Logging](#monitoring-and-logging)
7. [Cloud and CI/CD](#cloud-and-cicd)
8. [Related Guides](#related-guides)

---

## Introduction

This guide covers deploying SparkForge pipelines from local development to production: prerequisites, Spark and Delta Lake setup, engine configuration, monitoring, and high-level cloud/CI/CD options. All content is self-contained within the guides directory.

## Prerequisites

### System Requirements

- **Python**: 3.8 or higher
- **Java**: 11 or higher (17 recommended for Spark 3.5)
- **Memory**: 8GB RAM minimum; 16GB+ recommended for production
- **Storage**: Sufficient space for Delta tables and logs
- **Network**: Access to data sources (files, object storage, databases) as needed

### Software

- **Apache Spark**: 3.5.x (or compatible)
- **Delta Lake**: 3.0.x (or compatible with your Spark version)
- **PySpark**: Match Spark version

Install:

```bash
pip install pyspark>=3.5.0,<3.6.0
pip install delta-spark
pip install pipeline-builder[pyspark]
```

On macOS/Linux, install Java if needed:

```bash
# macOS
brew install openjdk@17

# Ubuntu/Debian
sudo apt-get install openjdk-17-jdk

java -version  # verify
```

## Local Development Setup

1. **Create Spark session with Delta Lake**

```python
from pyspark.sql import SparkSession
from pipeline_builder.engine_config import configure_engine

spark = SparkSession.builder \
    .appName("SparkForgePipeline") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

configure_engine(spark=spark)
```

2. **Use a dedicated schema** for dev (e.g. `schema="dev_analytics"`) so you do not overwrite production tables.

3. **Build and run** as in [Getting Started](./GETTING_STARTED_GUIDE.md) and [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md).

## Production Checklist

- **Engine:** Always call `configure_engine(spark=spark)` before using any pipeline components.
- **Schema:** Use a dedicated schema for production tables (e.g. `schema="production"` or per-environment).
- **Validation thresholds:** Set `min_bronze_rate`, `min_silver_rate`, `min_gold_rate` on `PipelineBuilder` if you want runs to fail when validation rate is below a threshold.
- **Logging:** Use [LogWriter](./LOGGING_AND_MONITORING_GUIDE.md) to log every run (`append(result, run_id=...)`) for auditing and trends.
- **Error handling:** Check `result.status.value` and `result.errors` after each run; integrate with your alerting.
- **Resources:** Tune Spark driver/executor memory and cores for your cluster size and data volume (see [Performance](./PERFORMANCE_GUIDE.md)).

## Spark and Delta Lake Configuration

**Minimal (local/prototyping):**

```python
spark = SparkSession.builder \
    .appName("MyPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

**Production-oriented (tuning):**

```python
spark = SparkSession.builder \
    .appName("ProductionPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "200") \
    .getOrCreate()
```

**Databricks:** Delta is built in; create the session as usual and call `configure_engine(spark=spark)`.

**Object storage (e.g. S3):** Set warehouse dir and credentials as needed:

```python
spark.conf.set("spark.sql.warehouse.dir", "s3://your-bucket/warehouse/")
# Configure S3/credentials per your environment (e.g. IAM, access keys)
```

## Monitoring and Logging

- **Log every run:** Use `LogWriter(spark, schema="...", table_name="pipeline_logs")` and `log_writer.append(result, run_id=...)`. See [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md).
- **Alert on failure:** If `result.status.value != "completed"`, use `result.errors` and your alerting channel.
- **Alert on duration:** If `result.duration_seconds` exceeds a threshold, trigger alerts.
- **Validation rates:** Check `result.bronze_results`, `result.silver_results`, `result.gold_results` for per-step `validation_rate` and row counts.

## Cloud and CI/CD

**General:** Run your pipeline entrypoint (script or function) in the environment your cloud or scheduler provides (e.g. AWS EMR step, Azure Synapse job, Databricks job, Kubernetes CronJob). Ensure:

- Spark (and Delta) are configured for that environment.
- `configure_engine(spark=spark)` is called at startup.
- Data paths and credentials are available (env vars, IAM, or secrets).
- LogWriter table is in a persistent store (e.g. Delta on S3/ADLS).

**Scheduling:** Use your platform’s scheduler (Airflow, cron, Databricks scheduler, Step Functions, etc.) to run initial load once and incremental runs on an interval. Pass the same `bronze_sources` contract (e.g. read from a table or path that your job populates).

**CI/CD:** In CI, run tests (e.g. unit tests with mock Spark); optionally run a small integration test that builds a pipeline and runs `run_validation_only` or a minimal `run_initial_load` with a tiny DataFrame. Do not depend on production credentials in CI unless in a secure, isolated way.

## Related Guides

- [Getting Started](./GETTING_STARTED_GUIDE.md) — Install and first pipeline.
- [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md) — LogWriter setup and usage.
- [Performance](./PERFORMANCE_GUIDE.md) — Tuning and resource configuration.
- [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) — Common deployment and runtime issues.
