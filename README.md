# SparkForge

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). Build scalable data pipelines with built-in parallel execution, comprehensive validation, and enterprise-grade monitoring.

[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://sparkforge.readthedocs.io/)
[![PyPI version](https://badge.fury.io/py/sparkforge.svg)](https://badge.fury.io/py/sparkforge)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Start Here - Choose Your Path

### New to SparkForge? 
**[📖 5-Minute Quick Start](https://sparkforge.readthedocs.io/en/latest/quick_start_5_min.html)** - Get running in under 5 minutes ⭐ **START HERE**
**[🌍 Hello World Example](examples/hello_world.py)** - The absolute simplest pipeline (just 3 lines!)

### Know What You Want to Build?
- **[🛒 E-commerce Analytics](https://sparkforge.readthedocs.io/en/latest/usecase_ecommerce.html)** - Order processing, customer analytics, revenue tracking
- **[📡 IoT Data Processing](https://sparkforge.readthedocs.io/en/latest/usecase_iot.html)** - Sensor data, anomaly detection, real-time analytics  
- **[📊 Business Intelligence](https://sparkforge.readthedocs.io/en/latest/usecase_bi.html)** - Dashboards, KPIs, reporting pipelines

### Want to Learn Step-by-Step?
**[📚 User Guide](https://sparkforge.readthedocs.io/en/latest/user_guide.html)** - Complete guide to all features and patterns
**[💡 Progressive Examples](https://sparkforge.readthedocs.io/en/latest/progressive_examples.html)** - Learn step-by-step with increasing complexity

### Need Help?
**[🔧 Troubleshooting](https://sparkforge.readthedocs.io/en/latest/troubleshooting.html)** - Common issues and solutions
**[📋 Quick Reference](https://sparkforge.readthedocs.io/en/latest/quick_reference.html)** - Essential syntax and patterns

## ⚡ Super Quick Start (30 seconds)

```bash
pip install sparkforge
python examples/hello_world.py
```

That's it! You just ran a complete Bronze → Silver → Gold pipeline.

## 🚀 Advanced Features Quick Start

### 🛡️ **Security Features (Automatic)**
```python
from sparkforge import PipelineBuilder, SecurityConfig, get_security_manager

# Security is enabled automatically - no code changes needed!
builder = PipelineBuilder(spark=spark, schema="my_schema")
# All inputs are automatically validated and protected

# Optional: Configure advanced security
security_config = SecurityConfig(
    enable_input_validation=True,
    enable_sql_injection_protection=True,
    enable_audit_logging=True
)
security_manager = get_security_manager(security_config)
```

### ⚡ **Performance Optimization (Automatic)**
```python
from sparkforge import PipelineBuilder, CacheConfig, get_performance_cache

# Performance caching is enabled automatically!
builder = PipelineBuilder(spark=spark, schema="my_schema")
# Validation results are automatically cached for better performance

# Optional: Configure advanced caching
cache_config = CacheConfig(
    max_size_mb=512,
    ttl_seconds=3600,
    strategy=CacheStrategy.LRU
)
cache = get_performance_cache(cache_config)
```

### 🔧 **Dynamic Parallel Execution (Optional)**
```python
from sparkforge import (
    DynamicParallelExecutor, ExecutionTask, TaskPriority, 
    create_execution_task
)

# Advanced parallel execution for complex workloads
executor = DynamicParallelExecutor()

# Create tasks with priorities
tasks = [
    create_execution_task("critical_task", critical_function, priority=TaskPriority.CRITICAL),
    create_execution_task("normal_task", normal_function, priority=TaskPriority.NORMAL)
]

# Execute with dynamic optimization
result = executor.execute_parallel(tasks)
print(f"Executed {result['metrics']['successful_tasks']} tasks successfully!")
```

### 🎯 **Auto-Inference & Simplified API (New!)**
```python
from sparkforge import PipelineBuilder

# Quick setup with preset configurations
builder = PipelineBuilder.for_development(spark=spark, schema="my_schema")

# Add bronze step with helper methods
builder.with_bronze_rules(
    name="events", 
    rules=PipelineBuilder.not_null_rules(["user_id", "timestamp"])
)

# Add silver step - source_bronze auto-inferred!
builder.add_silver_transform(
    name="clean_events",
    transform=lambda spark, df, silvers: df.filter(F.col("user_id").isNotNull()),
    rules=PipelineBuilder.not_null_rules(["user_id"]),
    table_name="clean_events"
)

# Add gold step - source_silvers auto-inferred!
builder.add_gold_transform(
    name="daily_analytics",
    transform=lambda spark, silvers: list(silvers.values())[0].groupBy("date").count(),
    rules=PipelineBuilder.not_null_rules(["date"]),
    table_name="daily_analytics"
)
```

## 🎯 What Makes SparkForge Special?

- **🏗️ Medallion Architecture**: Bronze → Silver → Gold data layering with automatic dependency management
- **⚡ Advanced Parallel Execution**: Dynamic worker allocation, intelligent task prioritization, and adaptive optimization
- **🎯 Auto-Inference**: Automatically infers source dependencies, reducing boilerplate by 70%
- **🛠️ Preset Configurations**: One-line setup for development, production, and testing environments
- **🔧 Validation Helpers**: Built-in methods for common validation patterns (not_null, positive_numbers, etc.)
- **📊 Smart Detection**: Automatic timestamp column detection for watermarking
- **🔍 Step-by-Step Debugging**: Execute individual pipeline steps independently for troubleshooting
- **✅ Enhanced Data Validation**: Configurable validation thresholds with automatic security validation and performance caching
- **🔄 Incremental Processing**: Watermarking and incremental updates with Delta Lake
- **💧 Delta Lake Integration**: Full support for ACID transactions, time travel, and schema evolution

## 🚀 New in v0.4.0 - Enterprise Features

### 🛡️ **Enterprise Security**
- **Input Validation**: Automatic validation of all user inputs with configurable rules
- **SQL Injection Protection**: Built-in protection against SQL injection attacks
- **Access Control**: Role-based access control with permission management
- **Audit Logging**: Comprehensive audit trails for compliance and security monitoring

### ⚡ **Performance Optimization**
- **Intelligent Caching**: TTL and LRU-based caching with automatic memory management
- **Dynamic Worker Allocation**: Automatically adjusts parallel workers based on workload and system resources
- **Resource Monitoring**: Real-time CPU and memory usage tracking
- **Adaptive Optimization**: Learns from execution patterns to optimize performance

### 🔧 **Advanced Parallel Execution**
- **Task Prioritization**: Critical, High, Normal, Low, and Background priority levels
- **Dependency Management**: Intelligent task scheduling based on dependencies
- **Work-Stealing Algorithms**: Optimal resource utilization across workers
- **Performance Metrics**: Detailed execution statistics and optimization recommendations

## ✨ Key Features

- **🏗️ Medallion Architecture**: Bronze → Silver → Gold data layering with automatic dependency management
- **⚡ Advanced Parallel Execution**: Dynamic worker allocation, intelligent task prioritization, and adaptive optimization
- **🔍 Step-by-Step Debugging**: Execute individual pipeline steps independently for troubleshooting
- **✅ Enhanced Data Validation**: Configurable validation thresholds with automatic security validation and performance caching
- **🔄 Incremental Processing**: Watermarking and incremental updates with Delta Lake
- **📅 Flexible Bronze Tables**: Support for Bronze tables with or without datetime columns
- **💧 Delta Lake Integration**: Full support for ACID transactions, time travel, and schema evolution
- **🛡️ Enterprise Security**: Input validation, SQL injection protection, access control, and audit logging
- **⚡ Performance Optimization**: Intelligent caching with TTL, LRU eviction, and memory management
- **📊 Structured Logging**: Detailed execution logging, timing, and monitoring
- **🛡️ Error Handling**: Comprehensive error handling, recovery, and retry mechanisms
- **📖 Professional Documentation**: Complete Read the Docs documentation with search, examples, and API reference
- **🎯 Enhanced User Experience**: Progressive learning paths, troubleshooting guides, and comprehensive examples

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

## 📚 Documentation

**📖 [Complete Documentation](https://sparkforge.readthedocs.io/)** - Professional documentation with search, navigation, and examples

### 🆕 Enhanced Documentation Experience

SparkForge now features **professional-grade documentation** hosted on Read the Docs with:

- **🔍 Interactive Search**: Find any function, class, or concept instantly
- **📱 Mobile-Friendly**: Responsive design works on all devices
- **🎯 Progressive Learning**: Structured learning paths from beginner to advanced
- **💡 Rich Examples**: Complete, runnable code examples for every feature
- **🔧 Troubleshooting**: Comprehensive problem-solving guides
- **📋 Quick Reference**: Essential syntax and patterns at your fingertips
- **🏗️ Use Case Guides**: Real-world examples for e-commerce, IoT, and BI
- **🔗 Cross-References**: Smart linking between related concepts
- **📊 API Documentation**: Auto-generated from enhanced docstrings
- **🌐 Always Up-to-Date**: Automatically updated with every code change

### 🆕 New Examples and Use Cases

- **🚀 [Dynamic Parallel Execution](examples/dynamic_parallel_execution.py)** - Advanced parallel processing with worker allocation
- **🛒 [E-commerce Analytics](examples/ecommerce_analytics.py)** - Complete e-commerce data pipeline
- **📡 [IoT Sensor Pipeline](examples/iot_sensor_pipeline.py)** - Real-time sensor data processing
- **🔍 [Step-by-Step Debugging](examples/step_by_step_debugging.py)** - Advanced debugging techniques

### 📖 Documentation Highlights

- **5-Minute Quick Start**: Get running in under 5 minutes with a complete example
- **Progressive Examples**: Learn step-by-step with increasing complexity
- **E-commerce Pipeline**: Complete end-to-end example with Bronze, Silver, and Gold layers
- **Troubleshooting Guide**: Solutions to common issues and error patterns
- **Decision Trees**: Choose the right configuration for your use case
- **Migration Guides**: Migrate from other frameworks with confidence

### 🎯 Getting Started
- **[5-Minute Quick Start](https://sparkforge.readthedocs.io/en/latest/quick_start_5_min.html)** - Get running in under 5 minutes ⭐ **START HERE**
- **[Hello World](examples/hello_world.py)** - Simplest possible example
- **[Getting Started Guide](https://sparkforge.readthedocs.io/en/latest/getting_started.html)** - Original quick start guide

### 🏗️ Use Case Guides  
- **[E-commerce Analytics](https://sparkforge.readthedocs.io/en/latest/usecase_ecommerce.html)** - Order processing, customer analytics
- **[IoT Data Processing](https://sparkforge.readthedocs.io/en/latest/usecase_iot.html)** - Sensor data, anomaly detection
- **[Business Intelligence](https://sparkforge.readthedocs.io/en/latest/usecase_bi.html)** - Dashboards, KPIs, reporting

### 📖 Reference Documentation
- **[User Guide](https://sparkforge.readthedocs.io/en/latest/user_guide.html)** - Comprehensive guide to all features
- **[Quick Reference](https://sparkforge.readthedocs.io/en/latest/quick_reference.html)** - Quick reference for developers
- **[API Reference](https://sparkforge.readthedocs.io/en/latest/api_reference.html)** - Complete API documentation with enhanced docstrings

### 🔧 Enhanced API Documentation

All SparkForge classes and methods now feature **comprehensive docstrings** with:

- **📝 Detailed Parameter Descriptions**: Clear explanations of all parameters with type information
- **💡 Practical Examples**: Real-world code examples for every method
- **⚠️ Error Documentation**: Clear documentation of exceptions and error conditions
- **🎯 Usage Patterns**: Best practices and common usage scenarios
- **🔗 Cross-References**: Links to related methods and concepts
- **📊 Return Value Details**: Comprehensive documentation of return types and structures

**Example Enhanced Docstring**:
```python
def add_silver_transform(self, *, name: str, source_bronze: str, 
                        transform: Callable[..., DataFrame], 
                        rules: Dict[str, List[Any]], 
                        table_name: str, 
                        watermark_col: Optional[str] = None,
                        description: Optional[str] = None,
                        depends_on: Optional[List[str]] = None) -> 'PipelineBuilder':
    """
    Add Silver layer transformation step for data cleaning and enrichment.
    
    Silver steps represent the second layer of the Medallion Architecture,
    transforming raw Bronze data into clean, business-ready datasets.
    
    Args:
        name: Unique identifier for this Silver step
        transform: Transformation function with signature:
                 (spark: SparkSession, bronze_df: DataFrame, prior_silvers: Dict[str, DataFrame]) -> DataFrame
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        table_name: Target Delta table name where results will be stored
        watermark_col: Column name for watermarking (e.g., "timestamp", "updated_at").
                      If provided, enables incremental processing with append mode.
        depends_on: List of other Silver step names that must complete before this step.
    
    Example:
        >>> def clean_user_events(spark, bronze_df, prior_silvers):
        ...     return (bronze_df
        ...         .filter(F.col("user_id").isNotNull())
        ...         .withColumn("event_date", F.date_trunc("day", "timestamp"))
        ...     )
        >>> 
        >>> builder.add_silver_transform(
        ...     name="clean_events",
        ...     source_bronze="user_events",
        ...     transform=clean_user_events,
        ...     rules={"user_id": [F.col("user_id").isNotNull()]},
        ...     table_name="clean_user_events",
        ...     watermark_col="timestamp"
        ... )
    """
```

### 💡 Examples & Learning
- **[Examples](examples/)** - Working code examples
- **[Progressive Examples](https://sparkforge.readthedocs.io/en/latest/progressive_examples.html)** - Learn step-by-step
- **[Jupyter Notebooks](notebooks/)** - Interactive examples
- **[Visual Guide](https://sparkforge.readthedocs.io/en/latest/visual_guide.html)** - Diagrams and flowcharts
- **[Decision Trees](https://sparkforge.readthedocs.io/en/latest/decision_trees.html)** - Make the right choices

### 🔧 Advanced Topics
- **[Migration Guides](https://sparkforge.readthedocs.io/en/latest/migration_guides.html)** - Migrate from other tools
- **[Troubleshooting](https://sparkforge.readthedocs.io/en/latest/troubleshooting.html)** - Common issues and solutions

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