# PipelineBuilder Architecture

## 1. Purpose and Scope

This document defines the reference architecture for PipelineBuilder, a production-ready data pipeline framework for Apache Spark & Delta Lake. PipelineBuilder implements the Medallion Architecture pattern (Bronze → Silver → Gold) with comprehensive validation, automatic dependency management, and enterprise-grade features.

The goal of this architecture is to ensure:
- **Data quality** through explicit validation stages at each layer
- **Scalability and maintainability** through clean, modular design
- **Observability and auditability** through comprehensive logging
- **Clear separation of concerns** between storage, execution, and validation
- **70% code reduction** by eliminating boilerplate

---

## 2. Architectural Overview

The PipelineBuilder architecture is composed of three primary layers:

1. **Data Storage Layer** – Delta Lake tables (Bronze, Silver, Gold)
2. **Pipeline Execution Layer** – Validation, transformation, and orchestration
3. **Monitoring Layer** – Logging, metrics, and observability

Data progresses through Bronze → Silver → Gold tables, with validation enforced at every transition and centralized logging of all pipeline events.

### Core Components

```
PipelineBuilder
├── PipelineBuilder (pipeline/builder.py)
│   ├── Fluent API for pipeline construction
│   ├── Step definition and validation
│   └── Pipeline configuration
├── PipelineRunner (pipeline/runner.py)
│   ├── Initial load execution
│   ├── Incremental execution
│   └── Validation-only execution
├── Validation System (validation/)
│   ├── Data quality validation
│   ├── Pipeline validation
│   └── Progressive quality gates
├── Dependency Management (dependencies/)
│   ├── Dependency analyzer
│   ├── Dependency graph
│   └── Parallel execution planning
└── LogWriter (writer/)
    ├── Execution logging
    ├── Analytics and monitoring
    └── Performance tracking
```

---

## 3. Core Concepts

### 3.1 Medallion Architecture

PipelineBuilder implements the Medallion Architecture with three distinct layers:

| Layer | Description | Quality Standard | Intended Use |
|-------|-------------|------------------|--------------|
| **Bronze** | Raw, ingested data | 90% validation rate | Lineage, auditing, reprocessing |
| **Silver** | Cleaned, standardized data | 95% validation rate | Reuse, integration, business logic |
| **Gold** | Curated, business-ready data | 98% validation rate | Reporting, analytics, dashboards |

### 3.2 Validation-First Design

Validation is treated as a first-class architectural component:

- **No transformation writes data without successful validation**
- **Validation failures do not silently fail** – they prevent downstream writes
- **All failures and warnings are logged** to the LogWriter
- **Progressive quality gates** ensure data quality improves at each layer
- **Early validation** – invalid configurations are rejected during construction

### 3.3 Automatic Dependency Management

PipelineBuilder automatically:
- **Detects dependencies** between Bronze, Silver, and Gold steps
- **Validates dependency chains** to prevent circular references
- **Plans parallel execution** of independent steps
- **Optimizes execution order** for maximum parallelism

### 3.4 Separation of Concerns

- **Delta Lake tables** are passive storage (no business logic)
- **PipelineBuilder** owns all business logic and state transitions
- **Validation** is separate from transformation
- **Logging** is independent of pipeline success/failure

---

## 4. Single-Source Pipeline Architecture

### 4.1 Description

The single-source architecture supports one upstream data producer feeding a linear pipeline. It is suitable for isolated domains or early-stage data products.

### 4.2 Component Breakdown

#### Bronze Layer
- **Bronze Table**: Stores raw ingested data with minimal constraints
- **Bronze Validation**: Validates required columns, structural integrity, basic data sanity rules
- **Incremental Processing**: Optional timestamp-based filtering for incremental updates
- **Errors**: Recorded to LogWriter

#### Silver Layer
- **Transformation (Bronze → Silver)**: Type normalization, column renaming, standardization, light derivations
- **Silver Validation**: Schema conformance, domain-specific constraints
- **Silver Table**: Cleaned and normalized data, suitable for reuse
- **Incremental Updates**: Supports both initial load and incremental modes

#### Gold Layer
- **Transformation (Silver → Gold)**: Business rules, aggregations, enrichment
- **Gold Validation**: Business-level correctness, downstream consumption requirements
- **Gold Table**: Analytics-ready dataset, trusted source for reporting
- **Always Overwrite**: Gold tables use overwrite mode for idempotency

#### Monitoring Layer
- **LogWriter**: Tracks validation errors, transformation metadata, pipeline execution metrics
- **Performance Metrics**: Execution time, row counts, validation rates
- **Analytics**: Query execution history, success rates, performance trends

---

## 5. Multi-Source Pipeline Architecture

### 5.1 Description

The multi-source architecture supports multiple independent data inputs that converge into a unified Gold dataset. Each source is isolated until post-Silver processing.

### 5.2 Source Isolation

Each source includes:
- **Independent Bronze table** with source-specific validation
- **Source-specific validation logic** tailored to data characteristics
- **Separate Silver tables** for each source

This ensures:
- **Fault isolation** – one source failure doesn't affect others
- **Source-specific rule enforcement** – different validation rules per source
- **Parallel execution** – independent sources process concurrently

### 5.3 Convergence Design

After Silver validation:
- **Multiple Silver tables** feed a shared Gold transformation
- **Data is merged, joined, or aligned** based on business logic
- **Cross-source constraints** are applied at the Gold layer
- **Unified Gold table** represents consolidated business view

### 5.4 Unified Gold Layer

- **Represents consolidated business view** from all sources
- **Enforces global consistency rules** across sources
- **Serves downstream analytics** with unified schema

---

## 6. Validation Strategy

### 6.1 Progressive Quality Gates

Validation stages are enforced at every state transition:

| Stage | Purpose | Quality Threshold |
|-------|---------|-------------------|
| **Bronze Validation** | Detect malformed input | 90% pass rate |
| **Silver Validation** | Verify transformation assumptions | 95% pass rate |
| **Gold Validation** | Ensure business readiness | 98% pass rate |

### 6.2 Validation Rules

PipelineBuilder supports two types of validation rules:

#### String Rules (Human-Readable)
```python
rules = {
    "user_id": ["not_null"],
    "age": ["gt", 0],
    "status": ["in", ["active", "inactive"]],
    "score": ["between", 0, 100]
}
```

#### PySpark Expressions (Advanced)
```python
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age") > 0]
}
```

### 6.3 Validation Behavior

Validation failures:
- **Prevent downstream writes** – no data is written if validation fails
- **Are logged with context** – detailed error messages with actionable suggestions
- **Do not overwrite existing tables** – failed runs don't corrupt data
- **Provide detailed metrics** – validation rates, failure counts, error details

---

## 7. Execution Modes

### 7.1 Initial Load

Processes all data from scratch:
- **Bronze**: Validates and writes all input data
- **Silver**: Transforms all Bronze data, uses overwrite mode
- **Gold**: Aggregates all Silver data, uses overwrite mode

### 7.2 Incremental Processing

Processes only new/changed data:
- **Bronze**: Filters data using `incremental_col` timestamp
- **Silver**: Appends new data to existing Silver tables
- **Gold**: Recomputes from all Silver data (always overwrite)

### 7.3 Validation Only

Checks data quality without writing:
- **Validates all layers** without writing to tables
- **Useful for testing** and quality checks
- **Logs validation results** to LogWriter

---

## 8. Parallel Execution

### 8.1 Automatic Parallelization

PipelineBuilder automatically:
- **Analyzes dependencies** between steps
- **Identifies independent steps** that can run in parallel
- **Plans execution groups** for optimal parallelism
- **Executes steps concurrently** using thread pool

### 8.2 Benefits

- **3-5x faster execution** for typical pipelines
- **Automatic dependency analysis** – no manual configuration
- **Thread-safe execution** – safe concurrent writes
- **Configurable worker count** (1-16+ workers)

### 8.3 Example

```python
# These 3 bronze steps run in parallel
builder.with_bronze_rules(name="events_a", ...)
builder.with_bronze_rules(name="events_b", ...)
builder.with_bronze_rules(name="events_c", ...)

# These 3 silver steps also run in parallel (after bronze completes)
builder.add_silver_transform(name="clean_a", source_bronze="events_a", ...)
builder.add_silver_transform(name="clean_b", source_bronze="events_b", ...)
builder.add_silver_transform(name="clean_c", source_bronze="events_c", ...)
```

---

## 9. Logging and Observability

### 9.1 LogWriter Responsibilities

The LogWriter captures:
- **Validation failures and warnings** with detailed context
- **Record counts** (processed, written, failed)
- **Execution timestamps** and duration
- **Source identifiers** and pipeline run IDs
- **Performance metrics** (execution time, validation rates)

### 9.2 LogWriter Features

- **Independent logging** – logs written even if pipeline fails
- **Observability during partial failures** – see what succeeded/failed
- **Centralized auditing** – all pipeline runs tracked
- **Analytics queries** – query execution history, success rates, trends

### 9.3 Usage

```python
from pipeline_builder import LogWriter

writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Log execution
writer.create_table(result_initial)
writer.append(result_incremental)

# Query logs
logs = spark.table("monitoring.pipeline_logs")
```

---

## 10. Scalability and Extensibility

### 10.1 Adding New Sources

To add a new source:
1. Create a new Bronze step with `with_bronze_rules()`
2. Define source-specific validation rules
3. Add a Silver transform with `add_silver_transform()`
4. Extend the Gold transformation to include the new source

### 10.2 Evolving Business Logic

- **Changes are isolated** to transformation stages
- **Validation rules evolve independently** from transformations
- **Schema evolution** supported via Delta Lake
- **Backward compatibility** maintained through validation

### 10.3 Performance Optimization

- **Parallel execution** for independent steps
- **Incremental processing** for large datasets
- **Delta Lake optimizations** (Z-ordering, compaction)
- **Configurable worker pools** for resource management

---

## 11. Operational Considerations

### 11.1 Idempotency

- **Pipelines are idempotent** – safe to rerun
- **Bronze is never mutated** – append-only semantics
- **Silver and Gold writes are atomic** – all or nothing
- **Failed runs don't corrupt data** – existing tables preserved

### 11.2 Error Handling

- **Comprehensive error messages** with actionable suggestions
- **Graceful failure handling** – partial failures don't crash pipeline
- **Detailed error logging** to LogWriter
- **Retry strategies** can be implemented at orchestration level

### 11.3 Schema Management

- **Schema override support** for explicit control
- **Delta Lake schema evolution** for flexible schemas
- **Schema validation** at each layer
- **Type safety** through validation rules

---

## 12. Non-Goals

This architecture does not prescribe:
- **Specific orchestration tools** (e.g., Airflow, Databricks Jobs, Prefect)
- **Specific storage formats** beyond Delta Lake
- **Specific compute engines** beyond Spark (though mock-spark supported for testing)
- **Specific monitoring tools** beyond LogWriter

These are implementation decisions left to the user.

---

## 13. Summary

PipelineBuilder enforces strong data quality guarantees while remaining flexible and scalable. It supports:

- **Incremental adoption** – start simple, add complexity as needed
- **Fault isolation** – multi-source pipelines with independent processing
- **Enterprise-grade observability** – comprehensive logging and monitoring
- **70% code reduction** – eliminate boilerplate, focus on business logic
- **Automatic optimization** – parallel execution, dependency management

This architecture is suitable for both early-stage pipelines and mature data platforms, providing a solid foundation for production data engineering workflows.

---

**Version:** 2.3.0  
**Last Updated:** December 2024
