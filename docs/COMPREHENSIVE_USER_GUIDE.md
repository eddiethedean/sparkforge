# Comprehensive User Guide: PipelineBuilder Framework

**Version**: 1.3.3  
**Last Updated**: January 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [PipelineBuilder and PipelineRunner Architecture](#pipelinebuilder-and-pipelinerunner-architecture)
4. [Data Flow Between Steps](#data-flow-between-steps)
5. [Simple Medallion Pipeline Example](#simple-medallion-pipeline-example)
6. [Complex Multi-Source Pipeline](#complex-multi-source-pipeline)
7. [LogWriter Integration](#logwriter-integration)
8. [Databricks Deployment](#databricks-deployment)
9. [Best Practices](#best-practices)

---

## Introduction

**PipelineBuilder** is a production-ready data pipeline framework built on Apache Spark and Delta Lake. It implements the **Medallion Architecture** (Bronze → Silver → Gold) to transform raw data into business-ready analytics through a clean, maintainable API.

### Key Concepts

- **Bronze Layer**: Raw data ingestion with validation (in-memory)
  - Currently sourced from existing Spark DataFrames or Delta tables
  - Future feature: Direct integration with external sources (S3, Azure Blob, Kafka, etc.)
- **Silver Layer**: Data cleaning and enrichment (persisted to Delta Lake)
- **Gold Layer**: Business analytics and aggregations (persisted to Delta Lake)

### Why PipelineBuilder?

- **70% Less Boilerplate**: Clean API vs raw Spark code
- **Built-in Validation**: Data quality checks at every layer
- **Automatic Parallelization**: Dependency-aware parallel execution
- **Production Ready**: Error handling, monitoring, alerting
- **Delta Lake Integration**: ACID transactions, time travel, schema evolution

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone [gitlab repo link]

# Install
pip install .
```

### 5-Minute Hello World

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pipeline_builder import PipelineBuilder

# Initialize Spark
spark = SparkSession.builder.appName("My Pipeline").getOrCreate()

# Load Bronze data (existing Spark DataFrame or Delta table)
data = [("Alice", "click"), ("Bob", "view"), ("Alice", "purchase")]
df = spark.createDataFrame(data, ["user", "action"])
# OR: df = spark.table("existing_bronze_data")

# Build pipeline (3 steps)
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze: Validate
builder.with_bronze_rules(
    name="events",
    rules={
        "user": [F.col("user").isNotNull()],
        "action": [F.col("action").isNotNull()]
    }
)

# Silver: Filter
builder.add_silver_transform(
    name="purchases",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("action") == "purchase"),
    rules={
        "user": [F.col("user").isNotNull()],
        "action": [F.col("action") == "purchase"]
    },
    table_name="purchases"
)

# Gold: Aggregate
builder.add_gold_transform(
    name="user_counts",
    transform=lambda spark, silvers: silvers["purchases"].groupBy("user").count(),
    rules={"user": [F.col("user").isNotNull()], "count": [F.col("count") > 0]},
    table_name="user_counts",
    source_silvers=["purchases"]
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": df})

print(f"✅ Pipeline completed: {result.status}")
print(f"Rows written: {result.metrics.total_rows_written}")
```

---

## PipelineBuilder and PipelineRunner Architecture

### Component Overview

**Diagram 1A: Component Architecture - High-Level Overview**

```
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR APPLICATION                            │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ↓
         ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
         ┃           PIPELINEBUILDER FRAMEWORK                      ┃
         ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    STEP 1: BUILD                STEP 2: EXECUTE
    ═════════════                ════════════════
    
┌──────────────────────┐       ┌──────────────────────┐
│  PipelineBuilder     │       │  PipelineRunner      │
│  ──────────────      │       │  ──────────────      │
│                      │       │                      │
│  Configure:          │       │  Run Modes:          │
│  • Bronze rules      │       │  • Initial load      │
│  • Silver transforms │─────→ │  • Incremental       │
│  • Gold transforms   │       │  • Full refresh      │
│  • Validation rules  │       │  • Validation only   │
│                      │       │                      │
│  to_pipeline()       │       │  Executes:           │
│         ↓            │       │  • Dependency        │
│  Returns Runner      │─────→ │    analysis          │
│                      │       │  • Parallel steps    │
│                      │       │  • Data validation   │
│                      │       │  • Delta writes      │
└──────────────────────┘       │                      │
                               │                      │
                               └──────────────────────┘
                                       │
                                       ↓
                              ┌─────────────────┐
                              │ PipelineReport  │
                              │ ───────────────  │
                              │ • Status        │
                              │ • Metrics       │
                              │ • Step results  │
                              │ • Errors        │
                              └─────────────────┘

         ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
         ┃                  DATA FLOW                              ┃
         ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
    │   BRONZE    │         │   SILVER    │         │    GOLD     │
    │ ─────────── │         │ ─────────── │         │ ─────────── │
    │ Raw data    │         │ Clean data  │         │ Analytics   │
    │ validation  │────────→│ transforms  │────────→│ aggregates  │
    │             │         │             │         │             │
    │ In-memory   │         │ Delta table │         │ Delta table │
    └─────────────┘         └─────────────┘         └─────────────┘

         ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
         ┃              MONITORING (OPTIONAL)                      ┃
         ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

                        ┌──────────────────────┐
                        │     LogWriter        │
                        │  ──────────────────  │
                        │  Log execution       │
                        │  Track trends        │
                        │  Detect anomalies    │
                        │  Performance reports │
                        └──────────────────────┘
                                   │
                                   ↓
                        ┌──────────────────────┐
                        │   Delta Log Table    │
                        │ (Historical metrics) │
                        └──────────────────────┘
```

### Detailed Breakdown

**Diagram 1B: Component Architecture - Detailed Breakdown**

```
USER CODE
═════════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│                            PipelineBuilder                                  │
│                      (Fluent API - Build Phase)                            │
│                                                                             │
│  Initialize:                                                                │
│  builder = PipelineBuilder(spark, schema="analytics",                      │
│                            min_bronze_rate=95.0,                            │
│                            min_silver_rate=98.0,                            │
│                            min_gold_rate=99.0)                              │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  1. BRONZE LAYER - with_bronze_rules()                                │ │
│  │  ────────────────────────────────────────                             │ │
│  │  Purpose: Define raw data validation rules                            │ │
│  │                                                                        │ │
│  │  Parameters:                              Creates:                    │ │
│  │  • name: "events"                         ┌────────────────┐          │ │
│  │  • rules: validation expressions       ──→│  BronzeStep    │          │ │
│  │  • incremental_col: "timestamp"            │  ────────────  │          │ │
│  │  • schema (optional): "raw_data"         │  • name        │          │ │
│  │                                           │  • rules       │          │ │
│  │  Stored in: bronze_steps = {}            │  • incremental │          │ │
│  │              {"events": BronzeStep}       └────────────────┘          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                      ↓                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  2. SILVER LAYER - add_silver_transform()                             │ │
│  │  ──────────────────────────────────────────                           │ │
│  │  Purpose: Define data transformation & enrichment                     │ │
│  │                                                                        │ │
│  │  Parameters:                              Creates:                    │ │
│  │  • name: "clean_events"                   ┌────────────────┐          │ │
│  │  • source_bronze: "events"             ──→│  SilverStep    │          │ │
│  │  • transform: clean_func()                │  ────────────  │          │ │
│  │  • rules: validation expressions         │  • name        │          │ │
│  │  • table_name: "clean_events"            │  • transform   │          │ │
│  │  • schema (optional): "processing"       │  • rules       │          │ │
│  │                                           │  • table_name  │          │ │
│  │  Stored in: silver_steps = {}            │  • dependencies│          │ │
│  │              {"clean_events": SilverStep} └────────────────┘          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                      ↓                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  3. GOLD LAYER - add_gold_transform()                                 │ │
│  │  ────────────────────────────────────────                             │ │
│  │  Purpose: Define business analytics & aggregations                    │ │
│  │                                                                        │ │
│  │  Parameters:                              Creates:                    │ │
│  │  • name: "daily_metrics"                  ┌────────────────┐          │ │
│  │  • transform: metrics_func()           ──→│   GoldStep     │          │ │
│  │  • rules: validation expressions          │  ────────────  │          │ │
│  │  • table_name: "daily_metrics"           │  • name        │          │ │
│  │  • source_silvers: ["clean_events"]      │  • transform   │          │ │
│  │  • schema (optional): "analytics"        │  • rules       │          │ │
│  │                                           │  • table_name  │          │ │
│  │  Stored in: gold_steps = {}              │  • dependencies│          │ │
│  │              {"daily_metrics": GoldStep}  └────────────────┘          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  Internal Components:                                                       │
│  ├─ ValidationThresholds (min quality rates: bronze 95%, silver 98%, ...)  │
│  ├─ PipelineConfig (schema, parallel settings, thresholds)                 │
│  ├─ PipelineValidator (validates step configurations & dependencies)       │
│  └─ Step Collections: bronze_steps{}, silver_steps{}, gold_steps{}         │
│                                                                             │
│                    ↓ pipeline = builder.to_pipeline()                      │
│                      (validates all steps & dependencies)                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                            PipelineRunner                                   │
│                     (Orchestration - Execution Phase)                      │
│                                                                             │
│  Receives: bronze_steps, silver_steps, gold_steps, config, logger          │
│                                                                             │
│  Execution Methods:                                                         │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ run_initial_load(bronze_sources: Dict[str, DataFrame])            │    │
│  │   → Full data load with overwrite mode                            │    │
│  │   → Use case: First-time pipeline execution                       │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │ run_incremental(bronze_sources: Dict[str, DataFrame])             │    │
│  │   → Process new data only with append mode                        │    │
│  │   → Use case: Daily/hourly incremental updates                    │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │ run_full_refresh(bronze_sources: Dict[str, DataFrame])            │    │
│  │   → Reprocess all data with overwrite mode                        │    │
│  │   → Use case: Data corrections, schema changes                    │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │ run_validation_only(bronze_sources: Dict[str, DataFrame])         │    │
│  │   → Validate data quality without writing                         │    │
│  │   → Use case: Testing, quality checks                             │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                    ↓                                        │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                      ExecutionEngine                               │    │
│  │                   (Core Execution Logic)                           │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ PHASE 1: DEPENDENCY ANALYSIS                                 │ │    │
│  │  │ ──────────────────────────────                               │ │    │
│  │  │ DependencyAnalyzer.analyze_dependencies(all_steps)           │ │    │
│  │  │                                                               │ │    │
│  │  │ Builds dependency graph:                                     │ │    │
│  │  │ • Bronze steps: No dependencies (execute first)              │ │    │
│  │  │ • Silver steps: Depend on Bronze + optional other Silvers    │ │    │
│  │  │ • Gold steps: Depend on Silver steps                         │ │    │
│  │  │                                                               │ │    │
│  │  │ Creates execution groups for parallel processing:            │ │    │
│  │  │   Group 0: [bronze_events, bronze_users, bronze_products]    │ │    │
│  │  │   Group 1: [silver_clean_events, silver_clean_users]         │ │    │
│  │  │   Group 2: [silver_enriched_events] (depends on group 1)     │ │    │
│  │  │   Group 3: [gold_daily_metrics, gold_user_summary]           │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ PHASE 2: PARALLEL EXECUTION                                  │ │    │
│  │  │ ─────────────────────────                                    │ │    │
│  │  │ ThreadPoolExecutor (max_workers=4 by default)                │ │    │
│  │  │                                                               │ │    │
│  │  │ For each execution group (in order):                         │ │    │
│  │  │   Submit all steps in group to thread pool                   │ │    │
│  │  │   Wait for all steps in group to complete                    │ │    │
│  │  │   Move to next group                                         │ │    │
│  │  │                                                               │ │    │
│  │  │ Example timeline:                                            │ │    │
│  │  │   t0: Group 0 starts → [Bronze1, Bronze2, Bronze3] parallel │ │    │
│  │  │   t1: Group 0 done                                           │ │    │
│  │  │   t1: Group 1 starts → [Silver1, Silver2] parallel          │ │    │
│  │  │   t2: Group 1 done                                           │ │    │
│  │  │   t2: Group 2 starts → [Gold1, Gold2] parallel              │ │    │
│  │  │   t3: All done                                               │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ PHASE 3: STEP EXECUTION (per step)                          │ │    │
│  │  │ ────────────────────────────────                             │ │    │
│  │  │ For each step:                                               │ │    │
│  │  │                                                               │ │    │
│  │  │ Bronze Step:                                                 │ │    │
│  │  │   1. Get source DataFrame from bronze_sources                │ │    │
│  │  │   2. Apply validation rules → apply_column_rules()           │ │    │
│  │  │   3. Calculate validation_rate                               │ │    │
│  │  │   4. Check against min_bronze_rate threshold                 │ │    │
│  │  │   5. Store validated DataFrame in context (in-memory)        │ │    │
│  │  │                                                               │ │    │
│  │  │ Silver Step:                                                 │ │    │
│  │  │   1. Get Bronze DataFrame from context                       │ │    │
│  │  │   2. Get prior Silver DataFrames from context (if needed)    │ │    │
│  │  │   3. Execute transform(spark, bronze_df, prior_silvers)      │ │    │
│  │  │   4. Apply validation rules → apply_column_rules()           │ │    │
│  │  │   5. Check against min_silver_rate threshold                 │ │    │
│  │  │   6. Write to Delta Lake: schema.table_name                  │ │    │
│  │  │   7. Track metrics (rows_written, duration)                  │ │    │
│  │  │                                                               │ │    │
│  │  │ Gold Step:                                                   │ │    │
│  │  │   1. Get all required Silver DataFrames from context         │ │    │
│  │  │   2. Execute transform(spark, silvers_dict)                  │ │    │
│  │  │   3. Apply validation rules → apply_column_rules()           │ │    │
│  │  │   4. Check against min_gold_rate threshold                   │ │    │
│  │  │   5. Write to Delta Lake: schema.table_name                  │ │    │
│  │  │   6. Track metrics (rows_written, duration)                  │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ PHASE 4: VALIDATION ENGINE                                   │ │    │
│  │  │ ────────────────────────                                     │ │    │
│  │  │ apply_column_rules(df, rules, stage, step_name)             │ │    │
│  │  │                                                               │ │    │
│  │  │ For each column in rules:                                    │ │    │
│  │  │   • Apply PySpark Column expressions                         │ │    │
│  │  │   • Mark rows as valid/invalid                               │ │    │
│  │  │   • Separate valid_df and invalid_df                         │ │    │
│  │  │   • Calculate validation_rate = valid / total * 100          │ │    │
│  │  │                                                               │ │    │
│  │  │ If validation_rate < threshold:                              │ │    │
│  │  │   • Raise ValidationError                                    │ │    │
│  │  │   • Include detailed error context                           │ │    │
│  │  │   • Provide suggestions for fixing                           │ │    │
│  │  │                                                               │ │    │
│  │  │ Return: (valid_df, invalid_df, validation_stats)            │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐ │    │
│  │  │ PHASE 5: ERROR HANDLING                                      │ │    │
│  │  │ ─────────────────────                                        │ │    │
│  │  │ • Capture exceptions per step                                │ │    │
│  │  │ • Create StepExecutionResult with error details              │ │    │
│  │  │ • Include error context & suggestions                        │ │    │
│  │  │ • Continue processing other steps (fail-fast disabled)       │ │    │
│  │  │ • Aggregate all errors in final report                       │ │    │
│  │  └──────────────────────────────────────────────────────────────┘ │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                    ↓                                        │
│  Returns: PipelineReport                                                   │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  • pipeline_id: Unique identifier for this pipeline               │    │
│  │  • execution_id: Unique identifier for this run                   │    │
│  │  • status: completed / failed                                     │    │
│  │  • mode: initial / incremental / full_refresh / validation_only   │    │
│  │  • start_time / end_time / duration_seconds                       │    │
│  │  • metrics:                                                        │    │
│  │      - total_steps / successful_steps / failed_steps              │    │
│  │      - total_rows_processed / total_rows_written                  │    │
│  │      - bronze_duration / silver_duration / gold_duration          │    │
│  │      - parallel_efficiency (0-100%)                               │    │
│  │  • bronze_results: {step_name: {status, duration, rows, ...}}     │    │
│  │  • silver_results: {step_name: {status, duration, rows, ...}}     │    │
│  │  • gold_results: {step_name: {status, duration, rows, ...}}       │    │
│  │  • errors: [error messages if any]                                │    │
│  │  • warnings: [warning messages if any]                            │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘

FRAMEWORK OUTPUTS
═════════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│                         Delta Lake Tables                                   │
│                    (Persisted in Spark Metastore)                          │
│                                                                             │
│  analytics.clean_events      ← Silver table written by ExecutionEngine     │
│  analytics.enriched_events   ← Silver table                                │
│  analytics.daily_metrics     ← Gold table written by ExecutionEngine       │
│  analytics.user_summary      ← Gold table                                  │
│                                                                             │
│  Features:                                                                  │
│  • ACID Transactions: Reliable concurrent writes                           │
│  • Time Travel: Query historical versions                                  │
│  • Schema Evolution: Add/modify columns safely                             │
│  • Data Optimization: OPTIMIZE & Z-ORDER for performance                   │
│  • Data Cleanup: VACUUM to remove old files                                │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                      PipelineReport Object                                  │
│                   (Available to User Code)                                 │
│                                                                             │
│  Use cases:                                                                 │
│  • Check Status: if result.status == "completed"                           │
│  • Monitor Metrics: result.metrics.total_rows_written                      │
│  • Log Execution: writer.append(result)  # LogWriter integration           │
│  • Alert on Failure: send_alert() if result.metrics.failed_steps > 0       │
│  • Debug Issues: print(result.errors) for error messages                   │
│  • Access Step Data: result.silver_results["clean_events"]["dataframe"]    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Between Steps

Understanding how data moves through the pipeline is crucial for building complex workflows. The ExecutionEngine maintains an **execution context** that stores DataFrames as they flow through each step.

**Note on Bronze Data Sources**: Currently, Bronze data is sourced from existing Spark DataFrames or Delta tables. You load your raw data into Spark first (via `spark.read.parquet()`, `spark.table()`, or `spark.createDataFrame()`), then pass it to the pipeline. Future versions will support direct integration with external sources like S3, Azure Blob Storage, Kafka, and other data sources.

**Diagram 2A: Execution Context & Data Flow**

```
EXECUTION CONTEXT (In-Memory Dictionary)
════════════════════════════════════════════════════════════════════════

The ExecutionEngine maintains a context dictionary that stores DataFrames
as they flow through the pipeline:

context = {
    "bronze_step_name": DataFrame (validated bronze data),
    "silver_step_name": DataFrame (from Delta table),
    "gold_step_name": DataFrame (from Delta table)
}

DATA FLOW THROUGH PIPELINE
════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER: Data Validation (In-Memory Only)                    │
└─────────────────────────────────────────────────────────────────────┘

Input: bronze_sources = {"events": source_df, "users": users_df}

Step 1: Bronze "events"                   Step 2: Bronze "users"
┌──────────────────────┐                  ┌──────────────────────┐
│ source_df            │                  │ users_df             │
│   ↓                  │                  │   ↓                  │
│ Apply validation     │                  │ Apply validation     │
│ rules                │                  │ rules                │
│   ↓                  │                  │   ↓                  │
│ validated_df         │                  │ validated_users_df   │
└──────────────────────┘                  └──────────────────────┘
         │                                          │
         ↓                                          ↓
    context["events"] = validated_df          context["users"] = validated_users_df


┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER: Data Transformation (Writes to Delta Tables)        │
└─────────────────────────────────────────────────────────────────────┘

Step 3: Silver "clean_events"               Step 4: Silver "clean_users"
(depends on Bronze "events")                (depends on Bronze "users")

┌──────────────────────────────┐           ┌──────────────────────────────┐
│ Get bronze_df from context:  │           │ Get bronze_df from context:  │
│   bronze_df = context["events"]│         │   bronze_df = context["users"]│
│                              │           │                              │
│ Get prior silvers (if any):  │           │ Get prior silvers (if any):  │
│   prior_silvers = {}         │           │   prior_silvers = {}         │
│                              │           │                              │
│ Execute transform:           │           │ Execute transform:           │
│   output_df = transform(     │           │   output_df = transform(     │
│     spark,                   │           │     spark,                   │
│     bronze_df,               │           │     bronze_df,               │
│     prior_silvers            │           │     prior_silvers            │
│   )                          │           │   )                          │
│   ↓                          │           │   ↓                          │
│ Apply validation rules       │           │ Apply validation rules       │
│   ↓                          │           │   ↓                          │
│ Write to Delta:              │           │ Write to Delta:              │
│   analytics.clean_events     │           │   analytics.clean_users      │
│   ↓                          │           │   ↓                          │
│ Read back from Delta         │           │ Read back from Delta         │
└──────────────────────────────┘           └──────────────────────────────┘
         │                                          │
         ↓                                          ↓
context["clean_events"] = spark.table(...)   context["clean_users"] = spark.table(...)


Step 5: Silver "enriched_events"
(depends on Bronze "events" AND Silver "clean_users")

┌────────────────────────────────────────────┐
│ Get bronze_df from context:                │
│   bronze_df = context["events"]            │
│                                            │
│ Get prior silvers:                         │
│   prior_silvers = {                        │
│     "clean_users": context["clean_users"]  │
│   }                                        │
│                                            │
│ Execute transform:                         │
│   output_df = transform(                   │
│     spark,                                 │
│     bronze_df,                             │
│     prior_silvers  # Can join with users!  │
│   )                                        │
│   ↓                                        │
│ Example transform:                         │
│   bronze_df.join(                          │
│     prior_silvers["clean_users"],          │
│     "user_id"                              │
│   )                                        │
│   ↓                                        │
│ Apply validation rules                     │
│   ↓                                        │
│ Write to Delta:                            │
│   analytics.enriched_events                │
│   ↓                                        │
│ Read back from Delta                       │
└────────────────────────────────────────────┘
         │
         ↓
context["enriched_events"] = spark.table(...)


┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER: Business Analytics (Writes to Delta Tables)           │
└─────────────────────────────────────────────────────────────────────┘

Step 6: Gold "user_analytics"
(depends on multiple Silver tables)

┌────────────────────────────────────────────┐
│ Get all required silvers from context:     │
│   silvers = {                              │
│     "clean_events": context["clean_events"],│
│     "clean_users": context["clean_users"], │
│     "enriched_events": context["enriched_events"]│
│   }                                        │
│                                            │
│ Execute transform:                         │
│   output_df = transform(                   │
│     spark,                                 │
│     silvers  # Dict of all silver DFs     │
│   )                                        │
│   ↓                                        │
│ Example transform:                         │
│   silvers["enriched_events"]               │
│     .groupBy("user_id", "date")            │
│     .agg(                                  │
│       count("*").alias("event_count"),     │
│       sum("value").alias("total_value")    │
│     )                                      │
│   ↓                                        │
│ Apply validation rules                     │
│   ↓                                        │
│ Write to Delta:                            │
│   analytics.user_analytics                 │
└────────────────────────────────────────────┘

FINAL OUTPUT: Delta Lake Tables
════════════════════════════════════════════════════════════════════════
analytics.clean_events       ← Silver table (persisted)
analytics.clean_users        ← Silver table (persisted)
analytics.enriched_events    ← Silver table (persisted)
analytics.user_analytics     ← Gold table (persisted)
```

**Diagram 2B: Multi-Source Data Flow Example**

```
SCENARIO: E-commerce pipeline with 3 bronze sources
════════════════════════════════════════════════════════════════════════

┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  BRONZE 1   │  │  BRONZE 2   │  │  BRONZE 3   │
│  ─────────  │  │  ─────────  │  │  ─────────  │
│             │  │             │  │             │
│   users     │  │   orders    │  │  products   │
│    100K     │  │    500K     │  │    10K      │
│   rows      │  │    rows     │  │   rows      │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       │ Validate       │ Validate       │ Validate
       │ 95% pass       │ 95% pass       │ 95% pass
       ↓                ↓                ↓
  context["users"]  context["orders"] context["products"]
       │                │                │
       └────────┬───────┴────────┬───────┘
                │                │
                ↓                ↓
       ┌─────────────┐  ┌─────────────┐
       │  SILVER 1   │  │  SILVER 2   │
       │  ─────────  │  │  ─────────  │
       │             │  │             │
       │ clean_users │  │clean_orders │
       │   (users)   │  │  (orders +  │
       │             │  │   users +   │
       │   98K rows  │  │   products) │
       │             │  │   475K rows │
       └──────┬──────┘  └──────┬──────┘
              │                │
              │ Write Delta    │ Write Delta
              ↓                ↓
       Delta.clean_users  Delta.clean_orders
              │                │
              │ Read back      │ Read back
              ↓                ↓
       context["clean_users"] context["clean_orders"]
              │                │
              └────────┬───────┘
                       │
                       ↓
              ┌─────────────────┐
              │     SILVER 3    │
              │  ─────────────  │
              │                 │
              │ order_summary   │
              │ (clean_orders + │
              │  clean_users)   │
              │                 │
              │   450K rows     │
              └────────┬────────┘
                       │
                       │ Write Delta
                       ↓
               Delta.order_summary
                       │
                       │ Read back
                       ↓
              context["order_summary"]
                       │
                       ↓
       ┌───────────────┴───────────────┐
       │               │               │
       ↓               ↓               ↓
┌────────────┐  ┌────────────┐  ┌────────────┐
│  GOLD 1    │  │  GOLD 2    │  │  GOLD 3    │
│ ──────────  │  │ ──────────  │  │ ──────────  │
│            │  │            │  │            │
│  daily_    │  │  user_     │  │  product_  │
│  revenue   │  │  lifetime_ │  │  performance│
│            │  │  value     │  │            │
│  365 rows  │  │  100K rows │  │  10K rows  │
└────────────┘  └────────────┘  └────────────┘
       │               │               │
       │ Write Delta   │ Write Delta   │ Write Delta
       ↓               ↓               ↓
Delta.daily_    Delta.user_     Delta.product_
   revenue         lifetime        performance
                    value

KEY INSIGHTS:
─────────────
1. Bronze validates but doesn't write (stays in-memory)
2. Silver reads from context, transforms, writes to Delta
3. Silver can depend on other Silver tables (via prior_silvers)
4. Gold reads multiple Silver tables from context
5. Context grows as pipeline executes:
   context = {
     "users": DF,
     "orders": DF,
     "products": DF,
     "clean_users": DF,
     "clean_orders": DF,
     "order_summary": DF
   }
```

**Multi-Source Code Example**

```python
from pipeline_builder import PipelineBuilder
from pyspark.sql import functions as F

# Initialize
builder = PipelineBuilder(spark=spark, schema="analytics")

# BRONZE: Define 3 sources
builder.with_bronze_rules(
    name="users",
    rules={"user_id": [F.col("user_id").isNotNull()], "status": [F.col("status").isNotNull()]}
)
builder.with_bronze_rules(
    name="orders",
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "user_id": [F.col("user_id").isNotNull()],
        "product_id": [F.col("product_id").isNotNull()],
        "quantity": [F.col("quantity") > 0],
        "price": [F.col("price") > 0],
        "order_date": [F.col("order_date").isNotNull()]
    }
)
builder.with_bronze_rules(
    name="products",
    rules={"product_id": [F.col("product_id").isNotNull()]}
)

# SILVER: Clean and transform
def clean_users(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("status") == "active")

def clean_orders(spark, bronze_df, prior_silvers):
    # Join with users and products
    return (bronze_df
        .join(prior_silvers["clean_users"], "user_id")
        .join(prior_silvers["clean_products"], "product_id")
        .withColumn("total_price", F.col("quantity") * F.col("price"))
    )

def order_summary(spark, bronze_df, prior_silvers):
    # Aggregate clean_orders
    return (prior_silvers["clean_orders"]
        .groupBy("user_id", "order_date")
        .agg(
            F.sum("total_price").alias("daily_spend"),
            F.count("order_id").alias("order_count")
        )
    )

# Add Silver transforms with dependencies
builder.add_silver_transform(
    name="clean_users",
    source_bronze="users",
    transform=clean_users,
    rules={"user_id": [F.col("user_id").isNotNull()], "status": [F.col("status") == "active"]},
    table_name="clean_users"
)

builder.add_silver_transform(
    name="clean_products",
    source_bronze="products",
    transform=lambda spark, df, s: df,
    rules={"product_id": [F.col("product_id").isNotNull()]},
    table_name="clean_products"
)

builder.add_silver_transform(
    name="clean_orders",
    source_bronze="orders",
    transform=clean_orders,  # Depends on clean_users and clean_products!
    rules={
        "order_id": [F.col("order_id").isNotNull()],
        "user_id": [F.col("user_id").isNotNull()],
        "product_id": [F.col("product_id").isNotNull()],
        "total_price": [F.col("total_price") > 0]
    },
    table_name="clean_orders",
    depends_on=["clean_users", "clean_products"]  # Explicit dependencies
)

builder.add_silver_transform(
    name="order_summary",
    source_bronze="orders",
    transform=order_summary,  # Depends on clean_orders!
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "order_date": [F.col("order_date").isNotNull()],
        "daily_spend": [F.col("daily_spend") > 0],
        "order_count": [F.col("order_count") > 0]
    },
    table_name="order_summary",
    depends_on=["clean_orders"]
)

# GOLD: Business analytics
def user_lifetime_value(spark, silvers):
    return (silvers["order_summary"]
        .groupBy("user_id")
        .agg(F.sum("daily_spend").alias("lifetime_value"))
    )

builder.add_gold_transform(
    name="user_lifetime_value",
    transform=user_lifetime_value,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "lifetime_value": [F.col("lifetime_value") > 0]
    },
    table_name="user_lifetime_value",
    source_silvers=["order_summary"]
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(
    bronze_sources={
        "users": users_df,
        "orders": orders_df,
        "products": products_df
    }
)
```

---

## Simple Medallion Pipeline Example

Here's a complete, production-ready example showing the Bronze → Silver → Gold pattern:

**Note**: The Bronze layer receives data from existing Spark DataFrames. Load your raw data into Spark first using `spark.read.parquet()`, `spark.table()`, or `spark.createDataFrame()`, then pass it to the pipeline via `bronze_sources`. Future versions will support direct external source integration.

**Diagram 3: Simple Pipeline Data Flow**

```
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER                             │
│  (Raw Data Ingestion + Validation)                         │
│                                                             │
│  Input: source_df                                          │
│  Rules: {                                                   │
│    "user_id": [F.col("user_id").isNotNull()],            │
│    "timestamp": [F.col("timestamp").isNotNull()]          │
│  }                                                          │
│  ↓ Validation (95% pass rate required)                    │
│  Output: Validated DataFrame (in-memory)                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    SILVER LAYER                             │
│  (Data Cleaning + Enrichment)                              │
│                                                             │
│  Input: Bronze DataFrame                                   │
│  Transform Function:                                        │
│    def clean_events(spark, bronze_df, prior_silvers):     │
│      return bronze_df \                                    │
│        .filter(F.col("status") == "active") \             │
│        .withColumn("processed_at", F.current_timestamp())  │
│                                                             │
│  Rules: {                                                   │
│    "user_id": [F.col("user_id").isNotNull()],            │
│    "processed_at": [F.col("processed_at").isNotNull()]    │
│  }                                                          │
│  ↓ Validation (98% pass rate required)                    │
│  Output: schema.clean_events Delta Table                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     GOLD LAYER                              │
│  (Business Analytics)                                       │
│                                                             │
│  Input: Dict of Silver DataFrames                          │
│  Transform Function:                                        │
│    def daily_metrics(spark, silvers):                     │
│      return silvers["clean_events"] \                     │
│        .groupBy("date") \                                  │
│        .agg(F.count("*").alias("count"))                  │
│                                                             │
│  Rules: {                                                   │
│    "date": [F.col("date").isNotNull()],                  │
│    "count": [F.col("count") > 0]                         │
│  }                                                          │
│  ↓ Validation (99% pass rate required)                    │
│  Output: schema.daily_metrics Delta Table                  │
└─────────────────────────────────────────────────────────────┘
```

**Complete Code Example**

```python
from pipeline_builder import PipelineBuilder
from pyspark.sql import functions as F

# Initialize builder
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0
)

# Bronze: Raw data validation
# Note: Load your raw data into Spark first, then pass to pipeline
# source_df = spark.read.parquet("/path/to/raw/data")
# OR: source_df = spark.table("existing_raw_table")

builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "status": [F.col("status").isNotNull()]
    },
    incremental_col="timestamp"
)

# Silver: Data cleaning
def clean_events(spark, bronze_df, prior_silvers):
    return bronze_df \
        .filter(F.col("status") == "active") \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("date", F.to_date(F.col("timestamp")))

builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=clean_events,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "status": [F.col("status") == "active"],
        "processed_at": [F.col("processed_at").isNotNull()],
        "date": [F.col("date").isNotNull()]
    },
    table_name="clean_events"
)

# Gold: Business analytics
def daily_metrics(spark, silvers):
    return silvers["clean_events"] \
        .groupBy("date") \
        .agg(F.count("*").alias("count"))

builder.add_gold_transform(
    name="daily_metrics",
    transform=daily_metrics,
    rules={
        "date": [F.col("date").isNotNull()],
        "count": [F.col("count") > 0]
    },
    table_name="daily_metrics",
    source_silvers=["clean_events"]
)

# Build and execute
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})
```

---

## Complex Multi-Source Pipeline

**Diagram 4: Multi-Source Pipeline**

```
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   BRONZE 1    │  │   BRONZE 2    │  │   BRONZE 3    │
│    "users"    │  │   "events"    │  │   "products"  │
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        ↓                  ↓                  ↓
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   SILVER 1    │  │   SILVER 2    │  │   SILVER 3    │
│ "clean_users" │  │"clean_events" │  │"clean_products│
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ↓
              ┌─────────────────────────────────────┐
              │  All Gold steps use Silver sources  │
              │  (can run in parallel)              │
              └─────────────────────────────────────┘
                           ↓
              ┌────────────┼────────────┐
              ↓            ↓            ↓
     ┌────────────┐ ┌────────────┐ ┌────────────┐
     │  GOLD 1    │ │  GOLD 2    │ │  GOLD 3    │
     │"user_analyt│ │"daily_kpis"│ │"product_sum│
     │    ics"    │ │            │ │   mary"    │
     └────────────┘ └────────────┘ └────────────┘
```

**Execution Flow**:
1. All Bronze steps execute first (can run in parallel)
2. All Silver steps execute after their Bronze dependencies (can run in parallel)
3. All Gold steps execute based on their Silver dependencies (can run in parallel)
4. Multiple Gold tables can be created from the same Silver sources

---

## LogWriter Integration

**Diagram 5: LogWriter Integration**

```
┌─────────────────────────────────────────────────────────────┐
│                    Pipeline Execution                       │
│                                                             │
│  PipelineRunner.run_initial_load()                         │
│  ↓                                                          │
│  ExecutionEngine                                            │
│  ↓                                                          │
│  Returns: PipelineReport                                    │
│    ├─ status                                               │
│    ├─ metrics (rows, duration, validation rates)           │
│    ├─ bronze_results                                       │
│    ├─ silver_results                                       │
│    └─ gold_results                                         │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                      LogWriter                              │
│  (Structured Logging & Analytics)                          │
│                                                             │
│  writer = LogWriter(                                       │
│      spark=spark,                                          │
│      schema="monitoring",                                   │
│      table_name="pipeline_logs"                            │
│  )                                                          │
│                                                             │
│  # Log the execution                                       │
│  writer.append(report)                                     │
│  ↓                                                          │
│  Creates log entries:                                      │
│    - One row per pipeline step                            │
│    - Captures: duration, validation_rate, rows_written    │
│    - Status: success/failure                              │
│    - Error messages if any                                │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│            monitoring.pipeline_logs (Delta Table)           │
│                                                             │
│  Columns:                                                  │
│  ├─ run_id                                                 │
│  ├─ execution_id                                           │
│  ├─ phase (bronze/silver/gold)                            │
│  ├─ step_name                                              │
│  ├─ start_time / end_time / duration_secs                 │
│  ├─ rows_processed / rows_written                         │
│  ├─ validation_rate                                        │
│  ├─ success (boolean)                                      │
│  └─ error_message                                          │
│                                                             │
│  Analytics Methods:                                        │
│  • analyze_quality_trends()                               │
│  • analyze_execution_trends()                             │
│  • detect_quality_anomalies()                             │
│  • generate_performance_report()                          │
└─────────────────────────────────────────────────────────────┘
```

**Code Example**

```python
from pipeline_builder import PipelineBuilder, LogWriter

# Build and run pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")
# ... configure pipeline ...
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": df})

# Initialize LogWriter
writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Log the execution
writer.append(result)

# Query logs
writer.show_logs(limit=20)

# Analyze trends (last 30 days)
quality_trends = writer.analyze_quality_trends(days=30)
execution_trends = writer.analyze_execution_trends(days=30)
anomalies = writer.detect_quality_anomalies()

# Generate performance report
report = writer.generate_performance_report()
print(f"Average execution time: {report['avg_execution_time']}")
print(f"Success rate: {report['success_rate']}")
```

---

## Databricks Deployment

**Diagram 6: Databricks Architecture**

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks Workspace                     │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Databricks Job                         │   │
│  │                                                      │   │
│  │  Task 1: Bronze Ingestion                          │   │
│  │  ├─ Notebook: notebooks/bronze_ingestion.py        │   │
│  │  ├─ Cluster: Job Cluster (autoscale 2-8)          │   │
│  │  └─ Schedule: Daily at 2 AM                        │   │
│  │                ↓                                    │   │
│  │  Task 2: Silver Processing                         │   │
│  │  ├─ Notebook: notebooks/silver_processing.py       │   │
│  │  ├─ Depends on: Task 1                            │   │
│  │  └─ Cluster: Same as Task 1                       │   │
│  │                ↓                                    │   │
│  │  Task 3: Gold Analytics                           │   │
│  │  ├─ Notebook: notebooks/gold_analytics.py         │   │
│  │  ├─ Depends on: Task 2                            │   │
│  │  └─ Cluster: Same as Task 1                       │   │
│  │                ↓                                    │   │
│  │  Task 4: Log Results                              │   │
│  │  ├─ Notebook: notebooks/log_execution.py          │   │
│  │  ├─ Depends on: Task 3                            │   │
│  │  └─ Cluster: Small cluster (2 workers)            │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           Delta Lake Storage                        │   │
│  │                                                      │   │
│  │  /mnt/delta/analytics/                             │   │
│  │  ├─ clean_events/                                  │   │
│  │  ├─ daily_metrics/                                 │   │
│  │  └─ ...                                            │   │
│  │                                                      │   │
│  │  /mnt/delta/monitoring/                            │   │
│  │  └─ pipeline_logs/                                 │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**Deployment Option 1: Single Notebook**

```python
# Databricks Notebook: etl_pipeline.py

from pipeline_builder import PipelineBuilder, LogWriter
from pyspark.sql import functions as F

# Get Databricks context
spark = spark  # Available in Databricks notebooks

# Widget for source path (parameterized jobs)
dbutils.widgets.text("source_path", "/mnt/raw/events/")
source_path = dbutils.widgets.get("source_path")

# Load source data
source_df = spark.read.parquet(source_path)

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")
# ... configure pipeline ...
pipeline = builder.to_pipeline()

# Execute
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

# Log results
writer = LogWriter(spark=spark, schema="monitoring", table_name="pipeline_logs")
writer.append(result)

# Check for failures
if result.status != "completed":
    raise Exception(f"Pipeline failed: {result.errors}")
```

---

## Best Practices

### 1. Data Quality

- Set appropriate validation thresholds for each layer
- Use comprehensive validation rules
- Monitor data quality metrics over time
- Implement alerts for quality degradation

### 2. Performance

- Enable parallel execution for independent steps
- Use appropriate partitioning strategies
- Monitor execution times and optimize slow steps
- Use Delta Lake OPTIMIZE for better query performance

### 3. Error Handling

- Implement comprehensive error handling
- Use retry mechanisms for transient failures
- Log detailed error information
- Implement circuit breakers for external dependencies

### 4. Monitoring

- Use structured logging with LogWriter
- Monitor key performance metrics
- Set up alerts for failures
- Track data quality trends

### 5. Testing

- Test individual steps in isolation
- Use realistic test data
- Test error scenarios
- Validate data quality rules

### 6. Schema Evolution

- Plan for schema changes in Delta Lake
- Use schema evolution features
- Test migrations thoroughly
- Document schema changes

---

## Additional Resources

- **API Reference**: See [API Reference](ENHANCED_API_REFERENCE.md)
- **Troubleshooting**: See [Troubleshooting Guide](COMPREHENSIVE_TROUBLESHOOTING_GUIDE.md)
- **Writer Guide**: See [LogWriter User Guide](writer_user_guide.md)
- **Deployment Guide**: See [Production Deployment Guide](PRODUCTION_DEPLOYMENT_GUIDE.md)
- **Performance Tuning**: See [Performance Tuning Guide](PERFORMANCE_TUNING_GUIDE.md)

---

## Conclusion

PipelineBuilder provides a powerful, flexible framework for building robust data pipelines with the Bronze → Silver → Gold architecture. This guide covered essential concepts and features needed to build production-ready data pipelines.

For more information, see the [Examples](../examples/) directory and [API Reference](ENHANCED_API_REFERENCE.md).

**Happy Pipeline Building! 🚀**

