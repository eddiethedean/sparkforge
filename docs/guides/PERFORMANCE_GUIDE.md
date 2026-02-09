# Performance Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [When to Care About Performance](#when-to-care-about-performance)
3. [Built-in Metrics](#built-in-metrics)
4. [Pipeline and Transform Optimization](#pipeline-and-transform-optimization)
5. [Data Processing and Partitioning](#data-processing-and-partitioning)
6. [Memory and Caching](#memory-and-caching)
7. [Resource Configuration](#resource-configuration)
8. [Best Practices](#best-practices)
9. [Related Guides](#related-guides)

---

## Introduction

This guide describes how to observe and improve SparkForge pipeline performance: built-in metrics, step ordering, validation and transform tuning, partitioning, memory and caching, and Spark resource settings. All content is self-contained within the guides directory.

## When to Care About Performance

- **Large data:** Pipelines processing many rows or wide tables benefit from partitioning, resource tuning, and early filtering.
- **Slow runs:** If runs exceed your SLA or tie up resources, use metrics to find slow steps and apply the optimizations below.
- **Production:** Set validation thresholds and log every run (see [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md)); use resource and Spark configs suited to your cluster.

## Built-in Metrics

The framework records execution metrics on the **result** (PipelineReport) and, when enabled, in verbose logging.

**Access from result:**

```python
result = pipeline.run_initial_load(bronze_sources={"raw_events": source_df})

# Overall
print(result.status.value)
print(result.duration_seconds)
print(result.metrics.total_steps)
print(result.metrics.successful_steps)
print(result.metrics.total_rows_processed)
print(result.metrics.total_rows_written)

# Per layer
print(result.metrics.bronze_duration)
print(result.metrics.silver_duration)
print(result.metrics.gold_duration)

# Per step (example)
print(result.bronze_results["raw_events"]["duration"])
print(result.bronze_results["raw_events"]["rows_processed"])
print(result.silver_results["processed_events"]["validation_rate"])
```

**Verbose logging:** Pass `verbose=True` when creating the PipelineBuilder to get more detailed step-by-step logging during execution.

## Pipeline and Transform Optimization

**Step ordering:** Steps run in dependency order. Keep the dependency graph clear: bronze first, then silver that depend on bronze, then gold that depend on silver. Avoid unnecessary cross-dependencies so the runner can execute efficiently.

**Validation rules:** Prefer a single expression per column when possible (e.g. combine null check and range in one expression) to reduce passes. Use [Validation Rules](./VALIDATION_RULES_GUIDE.md) helpers where they fit.

**Transforms:** Prefer native Spark SQL/column operations over Python UDFs. Filter early (e.g. drop invalid or unneeded rows at the start of the transform). Select only columns you need for downstream steps to reduce memory and shuffle.

Example of an efficient transform:

```python
def efficient_transform(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df \
        .filter(F.col("user_id").isNotNull()) \
        .filter(F.col("timestamp") > F.lit("2020-01-01")) \
        .select("user_id", "timestamp", "amount", "category") \
        .withColumn("segment", F.when(F.col("amount") > 1000, "premium").otherwise("standard"))
```

## Data Processing and Partitioning

**Partitioning:** For large DataFrames, repartition or coalesce as needed inside your transform (e.g. by date or key column) so downstream shuffles are balanced. Avoid over-partitioning (many tiny tasks) and under-partitioning (few large tasks).

**Data types:** Cast columns to the smallest appropriate type (e.g. integer instead of string for IDs) to reduce memory and improve shuffle.

**Early filtering:** Filter out invalid or unneeded rows as early as possible in each transform so less data flows through the pipeline.

## Memory and Caching

**Spark memory:** Configure driver and executor memory for your cluster (see [Resource Configuration](#resource-configuration)). Enable adaptive execution and, if useful, Kryo serialization:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

**Caching:** Cache a DataFrame only if it is reused multiple times (e.g. same DataFrame read or computed once and then used in several branches). Call `.cache()` and then trigger an action (e.g. `.count()`) to materialize. When the data is no longer needed, call `.unpersist()` to free memory. Do not cache every intermediate; it can hurt by filling memory.

**Column pruning:** In transforms, use `.select(...)` to keep only required columns so less data is held and shuffled.

## Resource Configuration

**Cluster sizing (guidance):**

- Small datasets (e.g. &lt; 1GB): 2–4 cores, 8–16 GB RAM.
- Medium (e.g. 1–10 GB): 4–8 cores, 16–32 GB RAM.
- Large (&gt; 10 GB): 8+ cores, 32+ GB RAM.

**Spark config (examples):**

```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.driver.memory", "4g")
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")
```

Tune `shuffle.partitions` and `default.parallelism` to match your data size and cluster; larger data usually benefits from more partitions.

## Best Practices

1. **Measure first:** Use the built-in metrics (duration, rows per step) to find the slowest steps before optimizing.
2. **Validate and build once:** Call `validate_pipeline()` and `to_pipeline()` once; reuse the pipeline instance for multiple runs.
3. **Log runs:** Use LogWriter to log every run so you can analyze trends (see [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md)).
4. **Incremental when possible:** Use `run_incremental` for regular updates so only new data is processed (see [Execution Modes](./EXECUTION_MODES_GUIDE.md)).
5. **Right-size resources:** Match Spark memory and parallelism to your cluster and data volume.

## Related Guides

- [Execution Modes](./EXECUTION_MODES_GUIDE.md) — Result object and run modes.
- [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md) — Log runs and analyze trends.
- [Deployment](./DEPLOYMENT_GUIDE.md) — Production Spark and resource setup.
