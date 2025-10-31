#!/usr/bin/env python3
"""
Diagram generation script for comprehensive user guide documentation.

This script generates ASCII diagrams using the asciimatics library for the
COMPREHENSIVE_USER_GUIDE.md documentation file.

Usage:
    python scripts/generate_diagrams.py
"""

from asciimatics.screen import Screen
from asciimatics.widgets import Widget
from typing import List, Tuple


def create_box_diagram_1a() -> str:
    """Create Diagram 1A: Component Architecture - High-Level Overview."""
    return """
┌─────────────────────────────────────────────────────────────────────┐
│                         YOUR APPLICATION                            │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ↓
         ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
         ┃           PIPELINEBUILDER FRAMEWORK                    ┃
         ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

    STEP 1: BUILD                STEP 2: EXECUTE
    ═════════════                ════════════════
    
┌──────────────────────┐       ┌──────────────────────┐
│  PipelineBuilder     │       │  PipelineRunner      │
│  ──────────────      │       │  ──────────────      │
│                      │       │                      │
│  Configure:          │       │  Run Modes:          │
│  • Bronze rules      │       │  • Initial load      │
│  • Silver transforms │──────→│  • Incremental       │
│  • Gold transforms   │       │  • Full refresh      │
│  • Validation rules  │       │  • Validation only   │
│                      │       │                      │
│  to_pipeline()       │       │  Executes:           │
│         ↓            │       │  • Dependency        │
│  Returns Runner      │──────→│    analysis          │
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
                              │ ─────────────── │
                              │ • Status        │
                              │ • Metrics       │
                              │ • Step results  │
                              │ • Errors        │
                              └─────────────────┘

         ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
         ┃                  DATA FLOW                             ┃
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
         ┃              MONITORING (OPTIONAL)                     ┃
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
"""


def create_box_diagram_1b() -> str:
    """Create Diagram 1B: Component Architecture - Detailed Breakdown."""
    return """
USER CODE
═════════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│                            PipelineBuilder                                  │
│                      (Fluent API - Build Phase)                             │
│                                                                             │
│  Initialize:                                                                │
│  builder = PipelineBuilder(spark, schema="analytics",                       │
│                            min_bronze_rate=95.0,                            │
│                            min_silver_rate=98.0,                            │
│                            min_gold_rate=99.0)                              │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  1. BRONZE LAYER - with_bronze_rules()                                │ │
│  │  ────────────────────────────────────────                             │ │
│  │  Purpose: Define raw data validation rules                            │ │
│  │                                                                       │ │
│  │  Parameters:                              Creates:                    │ │
│  │  • name: "events"                         ┌────────────────┐          │ │
│  │  • rules: validation expressions       ──→│  BronzeStep    │          │ │
│  │  • incremental_col: "timestamp"            │  ────────────  │         │ │
│  │  • schema (optional): "raw_data"         │  • name        │           │ │
│  │                                           │  • rules       │          │ │
│  │  Stored in: bronze_steps = {}            │  • incremental │           │ │
│  │              {"events": BronzeStep}       └────────────────┘          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                      ↓                                     │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  2. SILVER LAYER - add_silver_transform()                             │ │
│  │  ──────────────────────────────────────────                           │ │
│  │  Purpose: Define data transformation & enrichment                     │ │
│  │                                                                       │ │
│  │  Parameters:                              Creates:                    │ │
│  │  • name: "clean_events"                   ┌────────────────┐          │ │
│  │  • source_bronze: "events"             ──→│  SilverStep    │          │ │
│  │  • transform: clean_func()                │  ────────────  │          │ │
│  │  • rules: validation expressions          │  • name        │          │ │
│  │  • table_name: "clean_events"             │  • transform   │          │ │
│  │  • schema (optional): "processing"        │  • rules       │          │ │
│  │                                           │  • table_name  │          │ │
│  │  Stored in: silver_steps = {}             │  • dependencies│          │ │
│  │              {"clean_events": SilverStep} └────────────────┘          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                      ↓                                     │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  3. GOLD LAYER - add_gold_transform()                                 │ │
│  │  ────────────────────────────────────────                             │ │
│  │  Purpose: Define business analytics & aggregations                    │ │
│  │                                                                       │ │
│  │  Parameters:                              Creates:                    │ │
│  │  • name: "daily_metrics"                  ┌────────────────┐          │ │
│  │  • transform: metrics_func()           ──→│   GoldStep     │          │ │
│  │  • rules: validation expressions          │  ────────────  │          │ │
│  │  • table_name: "daily_metrics"            │  • name        │          │ │
│  │  • source_silvers: ["clean_events"]       │  • transform   │          │ │
│  │  • schema (optional): "analytics"         │  • rules       │          │ │
│  │                                           │  • table_name  │          │ │
│  │  Stored in: gold_steps = {}               │  • dependencies│          │ │
│  │              {"daily_metrics": GoldStep}  └────────────────┘          │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│  Internal Components:                                                      │
│  ├─ ValidationThresholds (min quality rates: bronze 95%, silver 98%, ...)  │
│  ├─ PipelineConfig (schema, parallel settings, thresholds)                 │
│  ├─ PipelineValidator (validates step configurations & dependencies)       │
│  └─ Step Collections: bronze_steps{}, silver_steps{}, gold_steps{}         │
│                                                                            │
│                    ↓ pipeline = builder.to_pipeline()                      │
│                      (validates all steps & dependencies)                  │
└────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ↓
┌────────────────────────────────────────────────────────────────────────────┐
│                            PipelineRunner                                  │
│                     (Orchestration - Execution Phase)                      │
│                                                                            │
│  Receives: bronze_steps, silver_steps, gold_steps, config, logger          │
│                                                                            │
│  Execution Methods:                                                        │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │ run_initial_load(bronze_sources: Dict[str, DataFrame])             │    │
│  │   → Full data load with overwrite mode                             │    │
│  │   → Use case: First-time pipeline execution                        │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │ run_incremental(bronze_sources: Dict[str, DataFrame])              │    │
│  │   → Process new data only with append mode                         │    │
│  │   → Use case: Daily/hourly incremental updates                     │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │ run_full_refresh(bronze_sources: Dict[str, DataFrame])             │    │
│  │   → Reprocess all data with overwrite mode                         │    │
│  │   → Use case: Data corrections, schema changes                     │    │
│  ├────────────────────────────────────────────────────────────────────┤    │
│  │ run_validation_only(bronze_sources: Dict[str, DataFrame])          │    │
│  │   → Validate data quality without writing                          │    │
│  │   → Use case: Testing, quality checks                              │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                    ↓                                       │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │                      ExecutionEngine                               │    │
│  │                   (Core Execution Logic)                           │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐  │    │
│  │  │ PHASE 1: DEPENDENCY ANALYSIS                                 │  │    │
│  │  │ ──────────────────────────────                               │  │    │
│  │  │ DependencyAnalyzer.analyze_dependencies(all_steps)           │  │    │
│  │  │                                                              │  │    │
│  │  │ Builds dependency graph:                                     │  │    │
│  │  │ • Bronze steps: No dependencies (execute first)              │  │    │
│  │  │ • Silver steps: Depend on Bronze + optional other Silvers    │  │    │
│  │  │ • Gold steps: Depend on Silver steps                         │  │    │
│  │  │                                                              │  │    │
│  │  │ Creates execution groups for parallel processing:            │  │    │
│  │  │   Group 0: [bronze_events, bronze_users, bronze_products]    │  │    │
│  │  │   Group 1: [silver_clean_events, silver_clean_users]         │  │    │
│  │  │   Group 2: [silver_enriched_events] (depends on group 1)     │  │    │
│  │  │   Group 3: [gold_daily_metrics, gold_user_summary]           │  │    │
│  │  └──────────────────────────────────────────────────────────────┘  │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐  │    │
│  │  │ PHASE 2: PARALLEL EXECUTION                                  │  │    │
│  │  │ ─────────────────────────                                    │  │    │
│  │  │ ThreadPoolExecutor (max_workers=4 by default)                │  │    │
│  │  │                                                              │  │    │
│  │  │ For each execution group (in order):                         │  │    │
│  │  │   Submit all steps in group to thread pool                   │  │    │
│  │  │   Wait for all steps in group to complete                    │  │    │
│  │  │   Move to next group                                         │  │    │
│  │  │                                                              │  │    │
│  │  │ Example timeline:                                            │  │    │
│  │  │   t0: Group 0 starts → [Bronze1, Bronze2, Bronze3] parallel  │  │    │
│  │  │   t1: Group 0 done                                           │  │    │
│  │  │   t1: Group 1 starts → [Silver1, Silver2] parallel           │  │    │
│  │  │   t2: Group 1 done                                           │  │    │
│  │  │   t2: Group 2 starts → [Gold1, Gold2] parallel               │  │    │
│  │  │   t3: All done                                               │  │    │
│  │  └──────────────────────────────────────────────────────────────┘  │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐  │    │
│  │  │ PHASE 3: STEP EXECUTION (per step)                           │  │    │
│  │  │ ────────────────────────────────                             │  │    │
│  │  │ For each step:                                               │  │    │
│  │  │                                                              │  │    │
│  │  │ Bronze Step:                                                 │  │    │
│  │  │   1. Get source DataFrame from bronze_sources                │  │    │
│  │  │   2. Apply validation rules → apply_column_rules()           │  │    │
│  │  │   3. Calculate validation_rate                               │  │    │
│  │  │   4. Check against min_bronze_rate threshold                 │  │    │
│  │  │   5. Store validated DataFrame in context (in-memory)        │  │    │
│  │  │                                                              │  │    │
│  │  │ Silver Step:                                                 │  │    │
│  │  │   1. Get Bronze DataFrame from context                       │  │    │
│  │  │   2. Get prior Silver DataFrames from context (if needed)    │  │    │
│  │  │   3. Execute transform(spark, bronze_df, prior_silvers)      │  │    │
│  │  │   4. Apply validation rules → apply_column_rules()           │  │    │
│  │  │   5. Check against min_silver_rate threshold                 │  │    │
│  │  │   6. Write to Delta Lake: schema.table_name                  │  │    │
│  │  │   7. Track metrics (rows_written, duration)                  │  │    │
│  │  │                                                              │  │    │
│  │  │ Gold Step:                                                   │  │    │
│  │  │   1. Get all required Silver DataFrames from context         │  │    │
│  │  │   2. Execute transform(spark, silvers_dict)                  │  │    │
│  │  │   3. Apply validation rules → apply_column_rules()           │  │    │
│  │  │   4. Check against min_gold_rate threshold                   │  │    │
│  │  │   5. Write to Delta Lake: schema.table_name                  │  │    │
│  │  │   6. Track metrics (rows_written, duration)                  │  │    │
│  │  └──────────────────────────────────────────────────────────────┘  │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐  │    │
│  │  │ PHASE 4: VALIDATION ENGINE                                   │  │    │
│  │  │ ────────────────────────                                     │  │    │
│  │  │ apply_column_rules(df, rules, stage, step_name)              │  │    │
│  │  │                                                              │  │    │
│  │  │ For each column in rules:                                    │  │    │
│  │  │   • Apply PySpark Column expressions                         │  │    │
│  │  │   • Mark rows as valid/invalid                               │  │    │
│  │  │   • Separate valid_df and invalid_df                         │  │    │
│  │  │   • Calculate validation_rate = valid / total * 100          │  │    │
│  │  │                                                              │  │    │
│  │  │ If validation_rate < threshold:                              │  │    │
│  │  │   • Raise ValidationError                                    │  │    │
│  │  │   • Include detailed error context                           │  │    │ 
│  │  │   • Provide suggestions for fixing                           │  │    │
│  │  │                                                              │  │    │
│  │  │ Return: (valid_df, invalid_df, validation_stats)             │  │    │
│  │  └──────────────────────────────────────────────────────────────┘  │    │
│  │                                                                    │    │
│  │  ┌──────────────────────────────────────────────────────────────┐  │    │
│  │  │ PHASE 5: ERROR HANDLING                                      │  │    │
│  │  │ ─────────────────────                                        │  │    │
│  │  │ • Capture exceptions per step                                │  │    │
│  │  │ • Create StepExecutionResult with error details              │  │    │
│  │  │ • Include error context & suggestions                        │  │    │
│  │  │ • Continue processing other steps (fail-fast disabled)       │  │    │
│  │  │ • Aggregate all errors in final report                       │  │    │
│  │  └──────────────────────────────────────────────────────────────┘  │    │
│  └────────────────────────────────────────────────────────────────────┘    │
│                                    ↓                                       │
│  Returns: PipelineReport                                                   │
│  ┌────────────────────────────────────────────────────────────────────┐    │
│  │  • pipeline_id: Unique identifier for this pipeline                │    │
│  │  • execution_id: Unique identifier for this run                    │    │
│  │  • status: completed / failed                                      │    │
│  │  • mode: initial / incremental / full_refresh / validation_only    │    │
│  │  • start_time / end_time / duration_seconds                        │    │
│  │  • metrics:                                                        │    │
│  │      - total_steps / successful_steps / failed_steps               │    │
│  │      - total_rows_processed / total_rows_written                   │    │
│  │      - bronze_duration / silver_duration / gold_duration           │    │
│  │      - parallel_efficiency (0-100%)                                │    │
│  │  • bronze_results: {step_name: {status, duration, rows, ...}}      │    │
│  │  • silver_results: {step_name: {status, duration, rows, ...}}      │    │
│  │  • gold_results: {step_name: {status, duration, rows, ...}}        │    │
│  │  • errors: [error messages if any]                                 │    │
│  │  • warnings: [warning messages if any]                             │    │
│  └────────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘

FRAMEWORK OUTPUTS
══════════════════════════════════════════════════════════════════════════════

┌────────────────────────────────────────────────────────────────────────────┐
│                         Delta Lake Tables                                  │
│                    (Persisted in Spark Metastore)                          │
│                                                                            │
│  analytics.clean_events      ← Silver table written by ExecutionEngine     │
│  analytics.enriched_events   ← Silver table                                │
│  analytics.daily_metrics     ← Gold table written by ExecutionEngine       │
│  analytics.user_summary      ← Gold table                                  │
│                                                                            │
│  Features:                                                                 │
│  • ACID Transactions: Reliable concurrent writes                           │
│  • Time Travel: Query historical versions                                  │
│  • Schema Evolution: Add/modify columns safely                             │
│  • Data Optimization: OPTIMIZE & Z-ORDER for performance                   │
│  • Data Cleanup: VACUUM to remove old files                                │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│                      PipelineReport Object                                 │
│                   (Available to User Code)                                 │
│                                                                            │
│  Use cases:                                                                │
│  • Check Status: if result.status.value == "completed"                     │
│  • Monitor Metrics: result.metrics.total_rows_written                      │
│  • Log Execution: writer.append(result)  # LogWriter integration           │
│  • Alert on Failure: send_alert() if result.metrics.failed_steps > 0       │
│  • Debug Issues: print(result.errors) for error messages                   │
│  • Access Step Data: result.silver_results["clean_events"]["dataframe"]    │
└────────────────────────────────────────────────────────────────────────────┘
"""
def create_diagram_2a() -> str:
    """Create Diagram 2A: Execution Context & Data Flow."""
    return """
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Data Flow Between Steps                                  │
│           Execution Context & State Management                              │
└─────────────────────────────────────────────────────────────────────────────┘

Execution Flow:

Bronze Sources (Input) → Context Storage → Silver Processing → Context Update
                                  ↓                                ↓
                           Delta Lake Write                    Delta Lake Write
                                  ↓                                ↓
                            Gold Processing → Context Update → Delta Lake Write

Execution Context (In-Memory State):
───────────────────────────────────────────────────────────────────────────────
│                                                                             │
│  context = {                                                                │
│    "bronze_events": DataFrame      # From bronze_sources input            │
│    "bronze_users": DataFrame       # From bronze_sources input            │
│    "silver_clean_events": DataFrame # From Silver step transform          │
│    "silver_enriched_events": DataFrame # From Silver step transform       │
│    "gold_daily_metrics": DataFrame  # From Gold step transform            │
│  }                                                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

How Data Flows:
───────────────────────────────────────────────────────────────────────────────
1. Bronze Steps:
   - Receive DataFrames from bronze_sources parameter
   - Validate data using rules
   - Store result in context[step_name]

2. Silver Steps:
   - Read from context["bronze_<name>"]
   - Can read from context["silver_<other_name>"] if source_silvers specified
   - Execute transform function
   - Validate result using rules
   - Write to Delta Lake at schema.table_name
   - Store result in context[step_name] for downstream steps

3. Gold Steps:
   - Read multiple DataFrames from context
   - Execute transform function
   - Validate result using rules
   - Write to Delta Lake at schema.table_name
   - Store result in context for downstream Gold steps (if any)
"""


def create_diagram_2b() -> str:
    """Create Diagram 2B: Multi-Source Data Flow Example."""
    return """
Multi-Source Pipeline Data Flow Example:
═══════════════════════════════════════════════════════════════════════════════

Bronze Sources (3 inputs):
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   events    │  │   users     │  │  products   │
│  DataFrame  │  │  DataFrame  │  │  DataFrame  │
└─────────────┘  └─────────────┘  └─────────────┘
       │                │                │
       ▼                ▼                ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                      Execution Context (Growing)                           │
│                                                                             │
│  {                                                                          │
│    "bronze_events": validated events DataFrame    ← Added by bronze_events │
│    "bronze_users": validated users DataFrame      ← Added by bronze_users  │
│    "bronze_products": validated products DF       ← Added by bronze_products│
│  }                                                                          │
└────────────────────────────────────────────────────────────────────────────┘
       │                │                │
       ▼                ▼                ▼
┌──────────────┐  ┌──────────────┐                ┌──────────────┐
│ silver_clean │  │ silver_clean │                │ silver_product│
│    events    │  │    users     │                │   catalog    │
└──────────────┘  └──────────────┘                └──────────────┘
       │                │                                │
       ▼                ▼                                ▼
    Write to         Write to                         Write to
  Delta Lake        Delta Lake                       Delta Lake
  (analytics.       (analytics.                      (analytics.
   clean_events)     clean_users)                     product_catalog)
       │                │                                │
       └────────────────┼────────────────────────────────┘
                        ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                 Execution Context (Updated)                                │
│                                                                             │
│  {                                                                          │
│    "bronze_events": ...,                                                    │
│    "bronze_users": ...,                                                     │
│    "bronze_products": ...,                                                  │
│    "silver_clean_events": cleaned events DF     ← Added by Silver step     │
│    "silver_clean_users": cleaned users DF       ← Added by Silver step     │
│    "silver_product_catalog": products DF        ← Added by Silver step     │
│  }                                                                          │
└────────────────────────────────────────────────────────────────────────────┘
                        │
                        ▼
              ┌──────────────────┐
              │  gold_enriched   │  (Reads all 3 silver DataFrames)
              │     events       │
              └──────────────────┘
                        │
                        ▼
                    Write to
                  Delta Lake
                (analytics.
                 enriched_events)
                        │
                        ▼
┌────────────────────────────────────────────────────────────────────────────┐
│              Execution Context (Final)                                     │
│                                                                             │
│  {                                                                          │
│    "bronze_events": ...,                                                    │
│    "bronze_users": ...,                                                     │
│    "bronze_products": ...,                                                  │
│    "silver_clean_events": ...,                                              │
│    "silver_clean_users": ...,                                               │
│    "silver_product_catalog": ...,                                           │
│    "gold_enriched_events": enriched events DF  ← Added by Gold step        │
│  }                                                                          │
└────────────────────────────────────────────────────────────────────────────┘
"""


def create_diagram_3() -> str:
    """Create Diagram 3: Simple Pipeline Data Flow."""
    return """
Simple Medallion Pipeline Flow:
═══════════════════════════════════════════════════════════════════════════════

                    User Code (Build Phase)
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│  builder = PipelineBuilder(spark, schema="analytics")                    │
│                                                                           │
│  # Bronze: Validate raw data                                             │
│  builder.with_bronze_rules(                                              │
│      name="events",                                                       │
│      rules={"user": [F.col("user").isNotNull()]}                        │
│  )                                                                        │
│                                                                           │
│  # Silver: Transform & enrich                                            │
│  builder.add_silver_transform(                                           │
│      name="purchases",                                                    │
│      source_bronze="events",                                             │
│      transform=lambda spark, df, silvers: df.filter(...),              │
│      rules={"user": [F.col("user").isNotNull()]}                        │
│  )                                                                        │
│                                                                           │
│  # Gold: Business analytics                                               │
│  builder.add_gold_transform(                                             │
│      name="user_counts",                                                 │
│      transform=lambda spark, silvers: silvers["purchases"].groupBy(...),│
│      rules={"count": [F.col("count") > 0]}                               │
│  )                                                                        │
│                                                                           │
│  pipeline = builder.to_pipeline()                                        │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                      User Code (Execution Phase)
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│  runner = PipelineRunner(pipeline)                                       │
│  report = runner.run_incremental(bronze_sources={                        │
│      "events": events_df  # Input raw data                                │
│  })                                                                       │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                        Execution Engine
┌────────────────────────────────────────────────────────────────────────────┐
│  STEP 1: Bronze - "events"                                               │
│  ─────────────────────────────────────────────────────                    │
│  Input:    events_df (from bronze_sources)                               │
│  Process:  Apply validation rules                                        │
│  Store:    context["events"] = validated_df                              │
│  Duration: 2.3s                                                           │
│                                                                            │
│  STEP 2: Silver - "purchases"                                            │
│  ─────────────────────────────────────────────────────                    │
│  Input:    context["events"]                                             │
│  Process:  Transform (filter purchases)                                  │
│  Validate: Apply rules                                                   │
│  Write:    analytics.purchases (Delta Lake)                             │
│  Store:    context["purchases"] = transformed_df                         │
│  Duration: 5.1s                                                           │
│                                                                            │
│  STEP 3: Gold - "user_counts"                                            │
│  ─────────────────────────────────────────────────────                    │
│  Input:    context["purchases"]                                          │
│  Process:  Aggregate (groupBy user, count)                              │
│  Validate: Apply rules                                                   │
│  Write:    analytics.user_counts (Delta Lake)                           │
│  Duration: 3.4s                                                           │
│                                                                            │
│  Total Duration: 10.8s                                                    │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                          Pipeline Report
┌────────────────────────────────────────────────────────────────────────────┐
│  Status: SUCCESS                                                          │
│  Steps: 3 succeeded, 0 failed                                            │
│  Duration: 10.8s                                                          │
│  Rows: Bronze=10000, Silver=5000, Gold=100                               │
└────────────────────────────────────────────────────────────────────────────┘
"""


def create_diagram_4() -> str:
    """Create Diagram 4: Multi-Source Pipeline."""
    return """
Multi-Source Pipeline Example:
═══════════════════════════════════════════════════════════════════════════════

Three Bronze Sources → Three Silver Steps → One Gold Step

                    BUILD PHASE
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  builder.with_bronze_rules(name="events", rules={...})                 │
│  builder.with_bronze_rules(name="users", rules={...})                  │
│  builder.with_bronze_rules(name="products", rules={...})               │
│                                                                          │
│  builder.add_silver_transform(name="clean_events", source_bronze="events",...)│
│  builder.add_silver_transform(name="clean_users", source_bronze="users",...)│
│  builder.add_silver_transform(name="product_catalog", source_bronze="products",...)│
│                                                                          │
│  builder.add_gold_transform(                                            │
│      name="enriched_analytics",                                         │
│      source_silvers=["clean_events", "clean_users", "product_catalog"], │
│      transform=lambda spark, silvers: ...                              │
│  )                                                                       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
                    EXECUTION PHASE
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  runner = PipelineRunner(pipeline)                                     │
│  report = runner.run_incremental(bronze_sources={                      │
│      "events": events_df,                                               │
│      "users": users_df,                                                 │
│      "products": products_df                                            │
│  })                                                                     │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│  GROUP 0: Bronze Steps (Parallel Execution)                            │
│  ═════════════════════════════════════════════════════════════════════│
│                                                                          │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐       │
│  │  bronze_events │    │  bronze_users  │    │ bronze_products│       │
│  │                │    │                │    │                │       │
│  │  Duration: 2s  │    │  Duration: 1s  │    │  Duration: 3s  │       │
│  │  Validate      │    │  Validate      │    │  Validate      │       │
│  │  Store context │    │  Store context │    │  Store context │       │
│  └────────────────┘    └────────────────┘    └────────────────┘       │
│         │                     │                     │                  │
│         └─────────────────────┼─────────────────────┘                  │
│                               ▼                                         │
│                         Group 0 Complete                                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│  GROUP 1: Silver Steps (Parallel Execution)                            │
│  ═════════════════════════════════════════════════════════════════════│
│                                                                          │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐ │
│  │ silver_clean     │    │ silver_clean     │    │ silver_product   │ │
│  │     events       │    │     users        │    │    catalog       │ │
│  │                  │    │                  │    │                  │ │
│  │  Duration: 5s    │    │  Duration: 4s    │    │  Duration: 6s    │ │
│  │  Transform       │    │  Transform       │    │  Transform       │ │
│  │  Write to Delta  │    │  Write to Delta  │    │  Write to Delta  │ │
│  │  Store context   │    │  Store context   │    │  Store context   │ │
│  └──────────────────┘    └──────────────────┘    └──────────────────┘ │
│         │                     │                     │                  │
│         └─────────────────────┼─────────────────────┘                  │
│                               ▼                                         │
│                         Group 1 Complete                                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│  GROUP 2: Gold Steps                                                     │
│  ═════════════════════════════════════════════════════════════════════│
│                                                                          │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │       gold_enriched_analytics                               │      │
│  │                                                             │      │
│  │  Duration: 8s                                               │      │
│  │  Read: clean_events, clean_users, product_catalog          │      │
│  │  Aggregate & join                                           │      │
│  │  Write to Delta Lake                                        │      │
│  │                                                             │      │
│  └──────────────────────────────────────────────────────────────┘      │
│                                                                          │
│  Total Duration: 2s (group 0) + 6s (group 1) + 8s (group 2) = 16s     │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
"""


def create_diagram_5() -> str:
    """Create Diagram 5: LogWriter Integration."""
    return """
LogWriter Integration in Pipeline:
═══════════════════════════════════════════════════════════════════════════════

┌────────────────────────────────────────────────────────────────────────────┐
│                         User Code Setup                                   │
│  ──────────────────────────────────────────────────────────────────────   │
│                                                                            │
│  from pipeline_builder import LogWriter                                   │
│                                                                            │
│  # Initialize with database details                                       │
│  logger = LogWriter(                                                      │
│      jdbc_url="jdbc:postgresql://host:5432/analytics",                   │
│      username="pipeline_user",                                            │
│      password="***"                                                       │
│  )                                                                        │
│                                                                            │
│  # Create logging table (one-time setup)                                  │
│  logger.create_table()                                                    │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌────────────────────────────────────────────────────────────────────────────┐
│                      Pass LogWriter to PipelineRunner                      │
│  ──────────────────────────────────────────────────────────────────────   │
│                                                                            │
│  runner = PipelineRunner(pipeline, logger=logger)                        │
│  report = runner.run_incremental(bronze_sources)                         │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌────────────────────────────────────────────────────────────────────────────┐
│                     During Execution (Automatic Logging)                   │
│  ──────────────────────────────────────────────────────────────────────   │
│                                                                            │
│  After each step completes:                                               │
│                                                                            │
│  1. Capture step metrics:                                                 │
│     ┌─────────────────────────────────────────────────────────────────┐  │
│     │ • step_name: "silver_clean_events"                            │  │
│     │ • step_type: "silver"                                          │  │
│     │ • status: "success"                                            │  │
│     │ • rows_processed: 10000                                        │  │
│     │ • validation_rate: 98.5%                                       │  │
│     │ • duration_seconds: 5.2                                        │  │
│     │ • error_message: null                                          │  │
│     └─────────────────────────────────────────────────────────────────┘  │
│                                                                            │
│  2. Append to logging table:                                             │
│     logger.append(                                                        │
│         report_id=report.pipeline_id,                                     │
│         step_name="silver_clean_events",                                 │
│         step_type="silver",                                               │
│         status="success",                                                 │
│         rows_processed=10000,                                             │
│         validation_rate=98.5,                                             │
│         duration_seconds=5.2                                              │
│     )                                                                     │
│                                                                            │
│  3. SQL query log:                                                        │
│     INSERT INTO pipeline_execution_log (                                 │
│         report_id, step_name, step_type, status,                         │
│         rows_processed, validation_rate, duration_seconds                │
│     ) VALUES (...)                                                        │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌────────────────────────────────────────────────────────────────────────────┐
│                      LogWriter Analytics                                   │
│  ──────────────────────────────────────────────────────────────────────   │
│                                                                            │
│  After pipeline completion, query for insights:                           │
│                                                                            │
│  -- Recent executions                                                      │
│  SELECT * FROM pipeline_execution_log                                    │
│  WHERE execution_timestamp > NOW() - INTERVAL '7 days'                   │
│  ORDER BY execution_timestamp DESC;                                       │
│                                                                            │
│  -- Step performance over time                                            │
│  SELECT                                                                    │
│      step_name,                                                            │
│      AVG(duration_seconds) as avg_duration,                               │
│      AVG(validation_rate) as avg_validation_rate                         │
│  FROM pipeline_execution_log                                             │
│  WHERE step_type = 'silver'                                               │
│  GROUP BY step_name;                                                      │
│                                                                            │
│  -- Failure analysis                                                       │
│  SELECT * FROM pipeline_execution_log                                    │
│  WHERE status = 'failure'                                                 │
│  ORDER BY execution_timestamp DESC                                        │
│  LIMIT 10;                                                                │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
"""


def create_diagram_6() -> str:
    """Create Diagram 6: Databricks Architecture."""
    return """
Databricks Deployment Architecture:
═══════════════════════════════════════════════════════════════════════════════

┌────────────────────────────────────────────────────────────────────────────┐
│                        DATABRICKS JOB                                      │
│  ══════════════════════════════════════════════════════════════════════  │
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                        Job Config                                    │ │
│  │  ──────────────────────────────────────────────────────────────     │ │
│  │                                                                      │ │
│  │  • Job Name: "medallion-pipeline"                                  │ │
│  │  • Schedule: Daily at 2 AM                                          │ │
│  │  • Cluster: Job Cluster (or Shared Cluster)                         │ │
│  │  • Notebook Paths: bronze.py, silver.py, gold.py                   │ │
│  │  • Dependencies: [bronze → silver → gold]                           │ │
│  │                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                │                                            │
│                                ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │              TASK 1: Bronze Ingestion                                │ │
│  │  ─────────────────────────────────────────────────────────────     │ │
│  │                                                                      │ │
│  │  Cluster: databricks-runtime                                        │ │
│  │  Notebook: bronze_ingestion.py                                     │ │
│  │                                                                      │ │
│  │  ┌────────────────────────────────────────────────────────────┐    │ │
│  │  │ # bronze_ingestion.py                                     │    │ │
│  │  │                                                             │    │ │
│  │  │ from pipeline_builder import PipelineBuilder              │    │ │
│  │  │                                                             │    │ │
│  │  │ builder = PipelineBuilder(spark, schema="analytics")      │    │ │
│  │  │ builder.with_bronze_rules(name="events", rules={...})    │    │ │
│  │  │                                                             │    │ │
│  │  │ pipeline = builder.to_pipeline()                           │    │ │
│  │  │ runner = PipelineRunner(pipeline)                          │    │ │
│  │  │ report = runner.run_incremental(bronze_sources={...})    │    │ │
│  │  │                                                             │    │ │
│  │  │ # Output stored in context for next task                   │    │ │
│  │  └────────────────────────────────────────────────────────────┘    │ │
│  │                                                                      │ │
│  │  Outputs: context["events"]                                         │ │
│  │                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                │                                            │
│                                ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │              TASK 2: Silver Processing                               │ │
│  │  ─────────────────────────────────────────────────────────────     │ │
│  │                                                                      │ │
│  │  Cluster: databricks-runtime                                        │ │
│  │  Notebook: silver_processing.py                                    │ │
│  │  Depends On: TASK 1                                                 │ │
│  │                                                                      │ │
│  │  ┌────────────────────────────────────────────────────────────┐    │ │
│  │  │ # silver_processing.py                                    │    │ │
│  │  │                                                             │    │ │
│  │  │ from pipeline_builder import PipelineBuilder              │    │ │
│  │  │                                                             │    │ │
│  │  │ builder = PipelineBuilder(spark, schema="analytics")      │    │ │
│  │  │ builder.add_silver_transform(...)                         │    │ │
│  │  │                                                             │    │ │
│  │  │ pipeline = builder.to_pipeline()                           │    │ │
│  │  │ runner = PipelineRunner(pipeline)                          │    │ │
│  │  │ report = runner.run_incremental(bronze_sources={...})    │    │ │
│  │  │                                                             │    │ │
│  │  │ # Writes to Delta Lake: analytics.clean_events            │    │ │
│  │  └────────────────────────────────────────────────────────────┘    │ │
│  │                                                                      │ │
│  │  Writes: analytics.clean_events (Delta Lake)                       │ │
│  │                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                │                                            │
│                                ▼                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │              TASK 3: Gold Analytics                                  │ │
│  │  ─────────────────────────────────────────────────────────────     │ │
│  │                                                                      │ │
│  │  Cluster: databricks-runtime                                        │ │
│  │  Notebook: gold_analytics.py                                       │ │
│  │  Depends On: TASK 2                                                 │ │
│  │                                                                      │ │
│  │  ┌────────────────────────────────────────────────────────────┐    │ │
│  │  │ # gold_analytics.py                                       │    │ │
│  │  │                                                             │    │ │
│  │  │ from pipeline_builder import PipelineBuilder              │    │ │
│  │  │                                                             │    │ │
│  │  │ builder = PipelineBuilder(spark, schema="analytics")      │    │ │
│  │  │ builder.add_gold_transform(...)                           │    │ │
│  │  │                                                             │    │ │
│  │  │ pipeline = builder.to_pipeline()                           │    │ │
│  │  │ runner = PipelineRunner(pipeline)                          │    │ │
│  │  │ report = runner.run_incremental(bronze_sources={...})    │    │ │
│  │  │                                                             │    │ │
│  │  │ # Writes to Delta Lake: analytics.daily_metrics           │    │ │
│  │  └────────────────────────────────────────────────────────────┘    │ │
│  │                                                                      │ │
│  │  Writes: analytics.daily_metrics (Delta Lake)                      │ │
│  │                                                                      │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
│  Delta Lake Storage:                                                       │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │  analytics.clean_events        (Silver layer)                       │ │
│  │  analytics.daily_metrics       (Gold layer)                         │ │
│  │  analytics.user_summary        (Gold layer)                         │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
"""


def main() -> None:
    """Generate all diagrams."""
    diagrams = {
        "Diagram 1A": create_box_diagram_1a,
        "Diagram 1B": create_box_diagram_1b,
        "Diagram 2A": create_diagram_2a,
        "Diagram 2B": create_diagram_2b,
        "Diagram 3": create_diagram_3,
        "Diagram 4": create_diagram_4,
        "Diagram 5": create_diagram_5,
        "Diagram 6": create_diagram_6,
    }
    
    print("Diagram Generation Utility")
    print("=" * 80)
    print()
    
    for name, func in diagrams.items():
        diagram = func()
        print(f"{name}:")
        print(diagram)
        print("\n" + "=" * 80 + "\n")
    
    print("All diagrams generated successfully!")


if __name__ == "__main__":
    main()

