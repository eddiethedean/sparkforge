# SparkForge

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). Build scalable data pipelines with built-in parallel execution, comprehensive validation, and enterprise-grade monitoring.

## 🚀 Quick Start

```bash
pip install sparkforge
```

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Initialize Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("SparkForge Example") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple pipeline
builder = PipelineBuilder(spark=spark, schema="my_schema")

def silver_transform(spark, bronze_df):
    return bronze_df.filter(F.col("status") == "active")

def gold_transform(spark, silvers):
    events_df = silvers["silver_events"]
    return events_df.groupBy("category").count()

# Build and run pipeline
pipeline = (builder
    .with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]}
    )
    .add_silver_transform(
        name="silver_events",
        source_bronze="events",
        transform=silver_transform,
        rules={"status": [F.col("status").isNotNull()]},
        table_name="silver_events"
    )
    .add_gold_transform(
        name="gold_summary",
        transform=gold_transform,
        rules={"category": [F.col("category").isNotNull()]},
        table_name="gold_summary"
    )
    .to_pipeline()
)

result = pipeline.initial_load(bronze_sources={"events": source_df})
print(f"Pipeline completed: {result.success}")

# Debug individual steps
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
silver_result = pipeline.execute_silver_step("silver_events")
print(f"Step debugging: Bronze={bronze_result.status.value}, Silver={silver_result.status.value}")
```

## ✨ Key Features

- **🏗️ Medallion Architecture**: Bronze → Silver → Gold data layering with automatic dependency management
- **⚡ Parallel Execution**: Independent Silver steps run concurrently for maximum performance
- **🔍 Step-by-Step Debugging**: Execute individual pipeline steps independently for troubleshooting
- **✅ Data Validation**: Configurable validation thresholds and comprehensive quality checks
- **🔄 Incremental Processing**: Watermarking and incremental updates with Delta Lake
- **📅 Flexible Bronze Tables**: Support for Bronze tables with or without datetime columns
- **💧 Delta Lake Integration**: Full support for ACID transactions, time travel, and schema evolution
- **📊 Structured Logging**: Detailed execution logging, timing, and monitoring
- **🛡️ Error Handling**: Comprehensive error handling, recovery, and retry mechanisms

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Java 8+ (for PySpark 3.2.4)
- PySpark 3.2.4+
- Delta Lake 1.2.0+

### Install from PyPI

```bash
pip install sparkforge
```

### Install from Source

```bash
git clone https://github.com/eddiethedean/sparkforge.git
cd sparkforge
pip install -e .
```

### Verify Installation

```python
import sparkforge
print(f"SparkForge version: {sparkforge.__version__}")

# Test basic functionality
from sparkforge import PipelineBuilder
print("✅ SparkForge installed successfully!")
```

## 📖 Usage Examples

### Basic Pipeline

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder \
    .appName("SparkForge Example") \
    .master("local[*]") \
    .getOrCreate()

# Create pipeline
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Define transforms
def silver_transform(spark, bronze_df):
    return bronze_df.filter(F.col("status") == "active")

def gold_transform(spark, silvers):
    events_df = silvers["silver_events"]
    return events_df.groupBy("category").count()

# Build and run pipeline
pipeline = (builder
    .with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]}
    )
    .add_silver_transform(
        name="silver_events",
        source_bronze="events",
        transform=silver_transform,
        rules={"status": [F.col("status").isNotNull()]},
        table_name="silver_events"
    )
    .add_gold_transform(
        name="gold_summary",
        transform=gold_transform,
        rules={"category": [F.col("category").isNotNull()]},
        table_name="gold_summary"
    )
    .to_pipeline()
)

result = pipeline.initial_load(bronze_sources={"events": source_df})
```

### Bronze Tables Without Datetime Columns

SparkForge supports Bronze tables without datetime columns, which forces Silver tables to use overwrite mode for full refresh on each run:

```python
# Bronze step WITHOUT incremental column
builder.with_bronze_rules(
    name="events_no_datetime",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "value": [F.col("value").isNotNull()]
    }
    # Note: No incremental_col parameter - forces full refresh
)

# Silver step will automatically use overwrite mode
builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events_no_datetime",
    transform=lambda spark, df, prior_silvers: df.withColumn("processed_at", F.current_timestamp()),
    rules={"processed_at": [F.col("processed_at").isNotNull()]},
    table_name="enriched_events"
    # No watermark_col needed since Bronze has no datetime column
)

# Even in incremental mode, Silver will use overwrite due to Bronze having no datetime
result = pipeline.run_incremental(bronze_sources={"events_no_datetime": source_df})
```

### Delta Lake Integration

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Delta Lake pipeline with ACID transactions
builder = PipelineBuilder(spark=spark, schema="my_schema")

def silver_transform(spark, bronze_df):
    # Clean and validate data
    return (bronze_df
        .filter(F.col("status").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )

def gold_transform(spark, silvers):
    # Aggregate data for business intelligence
    events_df = silvers["silver_events"]
    return (events_df
        .groupBy("category", "date")
        .agg(F.count("*").alias("event_count"))
    )

# Run with Delta Lake support
pipeline = (builder
    .with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]}
    )
    .add_silver_transform(
        name="silver_events",
        source_bronze="events",
        transform=silver_transform,
        rules={"status": [F.col("status").isNotNull()]},
        table_name="silver_events"
    )
    .add_gold_transform(
        name="gold_summary",
        transform=gold_transform,
        rules={"category": [F.col("category").isNotNull()]},
        table_name="gold_summary"
    )
    .to_pipeline()
)

result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Access Delta Lake features
print(f"Delta Lake tables created: {result.totals['tables_created']}")
```

## 🔧 Configuration

### Pipeline Configuration

```python
from sparkforge import PipelineBuilder

# Configure validation thresholds
builder = PipelineBuilder(
    spark=spark, 
    schema="my_schema",
    min_bronze_rate=95.0,    # 95% data quality threshold for Bronze
    min_silver_rate=90.0,    # 90% data quality threshold for Silver
    min_gold_rate=85.0,      # 85% data quality threshold for Gold
    enable_parallel_silver=True,  # Enable parallel Silver execution
    max_parallel_workers=4,       # Maximum parallel workers
    verbose=True
)
```

### Execution Modes

```python
# Different execution modes
pipeline = builder.to_pipeline()

pipeline.initial_load(bronze_sources={"events": source_df})        # Full refresh
pipeline.run_incremental(bronze_sources={"events": source_df})     # Incremental processing
pipeline.run_full_refresh(bronze_sources={"events": source_df})    # Force full refresh
pipeline.run_validation_only(bronze_sources={"events": source_df}) # Validation only
```

### Unified Dependency-Aware Execution

For maximum performance, enable unified execution where all steps run in parallel based on their actual dependencies:

```python
# Enable unified execution
pipeline = (builder
    .enable_unified_execution(
        max_workers=8,                    # Maximum parallel workers
        enable_parallel_execution=True,   # Enable parallel execution
        enable_dependency_optimization=True  # Optimize based on dependencies
    )
    .to_pipeline()
)

# Run with dependency-aware parallel execution
result = pipeline.run_unified(bronze_sources={"events": source_df})

print(f"Parallel efficiency: {result.metrics.parallel_efficiency:.2f}%")
print(f"Total duration: {result.metrics.total_duration:.2f}s")
```

**Benefits of Unified Execution:**
- **Cross-layer parallelization**: Bronze, Silver, and Gold steps can run in parallel
- **Dependency-aware scheduling**: Steps run as soon as their dependencies are satisfied
- **Optimal resource utilization**: Maximum parallelization based on actual dependencies
- **Better performance**: Significantly faster execution for complex pipelines

### Step-by-Step Execution for Troubleshooting

Debug and troubleshoot individual pipeline steps without running the entire pipeline. This powerful feature allows you to inspect intermediate outputs, modify transforms, and iterate quickly during development.

```python
from sparkforge import PipelineBuilder, StepType, StepStatus

# Build your pipeline
pipeline = (builder
    .with_bronze_rules(name="events", rules={"user_id": [F.col("user_id").isNotNull()]})
    .add_silver_transform(name="silver_events", source_bronze="events", ...)
    .add_gold_transform(name="gold_summary", source_silvers=["silver_events"], ...)
    .to_pipeline()
)

# Execute individual steps for debugging
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
silver_result = pipeline.execute_silver_step("silver_events")
gold_result = pipeline.execute_gold_step("gold_summary")

# Inspect results
print(f"Bronze validation passed: {bronze_result.validation_result.validation_passed}")
print(f"Silver output rows: {silver_result.output_count}")
print(f"Gold duration: {gold_result.duration_seconds:.2f}s")
```

**Step Execution Features:**
- **🔍 Independent Execution**: Run any Bronze, Silver, or Gold step in isolation
- **📊 Inspect Intermediate Data**: Examine data at each stage with full schema and content
- **🛠️ Modify and Re-run**: Change transform functions and re-execute specific steps
- **⚡ Fast Iteration**: No need to rerun previous steps when debugging downstream issues
- **📈 Performance Insights**: Detailed timing and validation metrics for each step

**Advanced Step Execution:**

```python
# Get step information and dependencies
step_info = pipeline.get_step_info("silver_events")
print(f"Step type: {step_info['type']}")
print(f"Dependencies: {step_info['dependencies']}")
print(f"Dependents: {step_info['dependents']}")

# List all available steps
steps = pipeline.list_steps()
print(f"Bronze steps: {steps['bronze']}")
print(f"Silver steps: {steps['silver']}")
print(f"Gold steps: {steps['gold']}")

# Get step output for detailed inspection
executor = pipeline.create_step_executor()
silver_output = executor.get_step_output("silver_events")
silver_output.show()
silver_output.printSchema()

# Check execution state
completed_steps = executor.list_completed_steps()
failed_steps = executor.list_failed_steps()
print(f"Completed: {completed_steps}")
print(f"Failed: {failed_steps}")

# Clear execution state for fresh start
executor.clear_execution_state()
```

**Troubleshooting Workflow:**

```python
# 1. Execute Bronze step and inspect
bronze_result = pipeline.execute_bronze_step("events", input_data=source_df)
if not bronze_result.validation_result.validation_passed:
    print(f"Bronze validation failed: {bronze_result.validation_result.validation_rate:.2f}%")
    # Fix data quality issues and re-run

# 2. Execute Silver step with custom input (if needed)
silver_result = pipeline.execute_silver_step("silver_events", force_input=True)

# 3. Modify transform function and re-run
def improved_silver_transform(spark, bronze_df):
    return bronze_df.withColumn("enhanced_field", F.lit("processed"))

pipeline.silver_steps["silver_events"].transform = improved_silver_transform
silver_result_2 = pipeline.execute_silver_step("silver_events")

# 4. Continue with Gold step
gold_result = pipeline.execute_gold_step("gold_summary")
```

**Key Benefits:**
- **Rapid Debugging**: Isolate and fix issues without full pipeline runs
- **Interactive Development**: Experiment with transforms and validations step-by-step
- **Production Troubleshooting**: Debug specific failing steps in production pipelines
- **Performance Optimization**: Profile individual steps for bottlenecks
- **Data Quality Investigation**: Deep dive into validation failures at any stage

## 📊 Monitoring & Logging

### Execution Monitoring

```python
# Get detailed execution results
result = pipeline.run_incremental(bronze_sources={"events": source_df})

# Access execution metrics
print(f"Success: {result.success}")
print(f"Total rows written: {result.totals['total_rows_written']}")
print(f"Execution time: {result.totals['total_duration_secs']:.2f}s")

# Access stage-specific metrics
bronze_stats = result.stage_stats['bronze']
print(f"Bronze validation rate: {bronze_stats.validation_rate:.2f}%")
```

### Structured Logging

```python
from sparkforge import LogWriter

# Configure logging
log_writer = LogWriter(
    spark=spark,
    table_name="my_schema.pipeline_logs",
    use_delta=True  # Use Delta Lake for logs
)

# Log pipeline execution
log_writer.log_pipeline_execution(result)
```

## 🧪 Testing

Run the comprehensive test suite with 400+ tests using our optimized parallel execution:

```bash
# Fast parallel tests (recommended for development)
python tests/run_tests_parallel.py --workers 4

# Run all tests with coverage
pytest --cov=sparkforge --cov-report=html

# Run specific test categories
pytest -m "not slow"                    # Skip slow tests
pytest -m "delta"                       # Delta Lake tests only
pytest tests/test_integration_*.py      # Integration tests only

# Run tests with verbose output
pytest -v --tb=short
```

### Parallel Test Execution

SparkForge includes an intelligent parallel test runner that automatically categorizes tests for optimal performance:

```bash
# Fast parallel tests (4x speedup)
python tests/run_tests_parallel.py --workers 4

# All parallel-compatible tests
python tests/run_tests_parallel.py --all-parallel --workers 4

# Sequential tests (for complete coverage)
python -m pytest tests/test_config.py tests/test_performance.py [other sequential tests]
```

**Performance Benefits:**
- **4x speedup** for core tests (22s vs 2+ minutes)
- **Smart categorization** of parallel vs sequential tests
- **Zero failures** with reliable parallel execution
- **Optimized test suite** with no duplicate tests
- **Optimal development feedback** for rapid iteration

### Test Categories

- **Parallel Tests**: Core functionality (4 files, ~22s)
- **Sequential Tests**: Complex integration tests (17 files, ~3.5min)
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end pipeline testing
- **Delta Lake Tests**: Delta Lake specific features
- **Performance Tests**: Load and performance validation
- **Error Handling Tests**: Comprehensive error scenario testing

**Clean Test Suite**: 27 test files (optimized, no duplicates)

### Test Optimization

The test suite has been optimized for maximum efficiency:

- **Duplicate Removal**: Eliminated 3 redundant test files
- **Smart Categorization**: Tests automatically grouped by parallel compatibility
- **Performance Tuning**: Core tests run in 22s with 4x speedup
- **Zero Maintenance**: No duplicate test maintenance overhead

Expected output: **400+ tests passed** ✅

## 🏗️ Architecture

### Medallion Architecture

1. **Bronze Layer**: Raw data ingestion with basic validation
2. **Silver Layer**: Cleaned and enriched data with business logic
3. **Gold Layer**: Aggregated and business-ready datasets

### Parallel Execution

- **Dependency Analysis**: Automatically analyzes Silver step dependencies
- **Parallel Processing**: Independent steps run concurrently
- **Resource Management**: Configurable worker limits

### Data Validation

- **Configurable Thresholds**: Set quality thresholds per layer
- **Spark-Native Validation**: Uses Spark's built-in validation
- **Detailed Reporting**: Comprehensive validation reports

## 🚀 Production Deployment

### Databricks

SparkForge is optimized for Databricks environments:

```python
# In Databricks notebook
from sparkforge import PipelineBuilder

# Spark session is automatically available
builder = PipelineBuilder(
    spark=spark, 
    schema="production_schema",
    min_bronze_rate=99.0,
    min_silver_rate=95.0,
    min_gold_rate=90.0,
    enable_parallel_silver=True,
    max_parallel_workers=8,
    verbose=True
)
```

### AWS EMR / Azure Synapse

```python
# For cloud environments
from sparkforge import PipelineBuilder

# Configure for cloud storage
builder = PipelineBuilder(spark=spark, schema="my_schema")
pipeline = builder.to_pipeline()
result = pipeline.run_incremental(bronze_sources={"events": source_df})
```

### Local Development

```bash
# Install in development mode
pip install -e .

# Run tests
pytest

# Run examples
python examples/basic_pipeline.py
python examples/step_by_step_execution.py

# Build package
python -m build
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Quick Start for Contributors

1. **Fork the repository**
2. **Clone your fork**: `git clone https://github.com/eddiethedean/sparkforge.git`
3. **Install in development mode**: `pip install -e .`
4. **Run fast parallel tests**: `python tests/run_tests_parallel.py --workers 4`
5. **Create a feature branch**: `git checkout -b feature/amazing-feature`
6. **Make your changes and add tests**
7. **Run tests**: `python tests/run_tests_parallel.py --workers 4`
8. **Submit a pull request**

### Development Setup

```bash
# Clone and setup
git clone https://github.com/eddiethedean/sparkforge.git
cd sparkforge
pip install -e .

# Install development dependencies
pip install pytest pytest-cov pytest-xdist black flake8 mypy

# Run code quality checks
black sparkforge/ tests/
flake8 sparkforge/ tests/
mypy sparkforge/

# Run fast parallel tests (recommended for development)
python tests/run_tests_parallel.py --workers 4

# Run tests with coverage
pytest --cov=sparkforge --cov-report=html
```

## 🏆 Acknowledgments

- Built on top of [Apache Spark](https://spark.apache.org/)
- Powered by [Delta Lake](https://delta.io/)
- Inspired by the Medallion Architecture pattern
- Thanks to the PySpark and Delta Lake communities

---

**Made with ❤️ for the data engineering community**