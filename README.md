# SparkForge

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). SparkForge provides a powerful, flexible framework for building scalable data pipelines with built-in parallel execution, comprehensive validation, and enterprise-grade monitoring.

## 🚀 Features

- **🏗️ Medallion Architecture**: Bronze → Silver → Gold data layering with automatic dependency management
- **⚡ Parallel Execution**: Independent Silver steps run concurrently for maximum performance
- **✅ Data Validation**: Configurable validation thresholds and comprehensive quality checks
- **🔄 Incremental Processing**: Watermarking and incremental updates with Delta Lake
- **📊 Structured Logging**: Detailed execution logging, timing, and monitoring
- **🛡️ Error Handling**: Comprehensive error handling, recovery, and retry mechanisms
- **⚙️ Configuration Management**: Flexible configuration with Pydantic models
- **🏔️ Delta Lake Integration**: Full support for ACID transactions, time travel, and schema evolution
- **🧪 Comprehensive Testing**: 280+ tests with real Spark integration and Delta Lake support
- **📦 Production Ready**: Complete Python package with proper distribution and documentation

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Java 11+
- PySpark 3.2.4+
- Delta Lake 2.0.2+

### Install from PyPI (Recommended)

```bash
pip install sparkforge
```

### Install from Source

```bash
# Clone the repository
git clone https://github.com/your-username/sparkforge.git
cd sparkforge

# Install in development mode
pip install -e .

# Or build and install
python -m build
pip install dist/sparkforge-*.whl
```

### Verify Installation

```python
import sparkforge
print(f"SparkForge version: {sparkforge.__version__}")

# Test basic functionality
from sparkforge import PipelineBuilder, ExecutionMode
print("✅ SparkForge installed successfully!")
```

## 🧪 Testing

Run the comprehensive test suite with 280+ tests:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=sparkforge --cov-report=html

# Run specific test categories
pytest -m "not slow"                    # Skip slow tests
pytest -m "delta"                       # Delta Lake tests only
pytest tests/test_integration_*.py      # Integration tests only

# Run tests with verbose output
pytest -v --tb=short
```

### Test Categories

- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end pipeline testing
- **Delta Lake Tests**: Delta Lake specific features
- **Performance Tests**: Load and performance validation
- **Error Handling Tests**: Comprehensive error scenario testing

Expected output: **280+ tests passed** ✅

## 📖 Usage

### Basic Pipeline

```python
from sparkforge import PipelineBuilder, ExecutionMode
from pyspark.sql import functions as F

# Initialize Spark (if not already done)
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("SparkForge Example") \
    .master("local[*]") \
    .getOrCreate()

# Create pipeline with fluent API
builder = PipelineBuilder(spark=spark)

# Define transforms
def silver_transform(spark, bronze_df):
    return bronze_df.filter(F.col("status") == "active")

def gold_transform(spark, silvers):
    events_df = silvers["silver_events"]
    return events_df.groupBy("category").count()

# Build and run pipeline
result = (builder
    .with_bronze_rules(min_quality_rate=95.0)
    .add_silver_transform("silver_events", silver_transform)
    .add_gold_transform("gold_summary", gold_transform)
    .run(mode=ExecutionMode.INITIAL)
)

print(f"Pipeline completed: {result.success}")
print(f"Rows written: {result.totals['total_rows_written']}")
```

### Advanced Pipeline with Configuration

```python
from sparkforge import PipelineBuilder, PipelineConfig, ValidationThresholds, ParallelConfig

# Custom configuration
config = PipelineConfig(
    schema="my_schema",
    thresholds=ValidationThresholds(bronze=95.0, silver=90.0, gold=85.0),
    parallel=ParallelConfig(enabled=True, max_workers=4),
    verbose=True
)

builder = PipelineBuilder(spark=spark, config=config)

# Add multiple silver steps (run in parallel)
result = (builder
    .with_bronze_rules(min_quality_rate=95.0)
    .add_silver_transform("silver_events", events_transform)
    .add_silver_transform("silver_users", users_transform)
    .add_gold_transform("gold_summary", gold_transform)
    .run(mode=ExecutionMode.INCREMENTAL)
)
```

### Delta Lake Integration

```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Delta Lake pipeline with ACID transactions
builder = PipelineBuilder(spark=spark)

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
result = (builder
    .with_bronze_rules(min_quality_rate=95.0)
    .add_silver_transform("silver_events", silver_transform)
    .add_gold_transform("gold_summary", gold_transform)
    .run(mode=ExecutionMode.INCREMENTAL)
)

# Access Delta Lake features
print(f"Delta Lake tables created: {result.totals['tables_created']}")
```

## 🔧 Configuration

### Pipeline Configuration

```python
from sparkforge import PipelineConfig, ValidationThresholds, ParallelConfig

# Custom configuration with Pydantic models
thresholds = ValidationThresholds(
    bronze=95.0,    # 95% data quality threshold for Bronze
    silver=90.0,    # 90% data quality threshold for Silver
    gold=85.0       # 85% data quality threshold for Gold
)

parallel = ParallelConfig(
    enabled=True,       # Enable parallel Silver execution
    max_workers=4,      # Maximum parallel workers
    timeout_secs=300    # Timeout for parallel operations
)

config = PipelineConfig(
    schema="my_schema",
    thresholds=thresholds,
    parallel=parallel,
    verbose=True
)

builder = PipelineBuilder(spark=spark, config=config)
```

### Execution Modes

```python
from sparkforge import ExecutionMode

# Different execution modes
builder.run(mode=ExecutionMode.INITIAL)        # Full refresh
builder.run(mode=ExecutionMode.INCREMENTAL)    # Incremental processing
builder.run(mode=ExecutionMode.FULL_REFRESH)   # Force full refresh
builder.run(mode=ExecutionMode.VALIDATION_ONLY) # Validation only
```

## 📊 Monitoring & Logging

SparkForge provides comprehensive monitoring and logging capabilities:

### Execution Monitoring

```python
# Get detailed execution results
result = builder.run()

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

### Key Monitoring Features

- **⏱️ Execution Timing**: Detailed timing for each stage and step
- **📈 Data Quality Metrics**: Validation results, row counts, and quality rates
- **🛡️ Error Handling**: Detailed error messages, stack traces, and recovery info
- **📊 Structured Logging**: JSON-formatted logs for monitoring systems
- **🔍 Delta Lake Integration**: Time travel, history, and metadata access
- **📋 Performance Metrics**: Memory usage, processing rates, and optimization hints

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
from sparkforge import PipelineBuilder, PipelineConfig

# Spark session is automatically available
config = PipelineConfig(
    schema="production_schema",
    thresholds=ValidationThresholds(bronze=99.0, silver=95.0, gold=90.0),
    parallel=ParallelConfig(enabled=True, max_workers=8),
    verbose=True
)

builder = PipelineBuilder(spark=spark, config=config)
```

### AWS EMR / Azure Synapse

```python
# For cloud environments
from sparkforge import PipelineBuilder

# Configure for cloud storage
builder = PipelineBuilder(spark=spark)
result = builder.run(mode=ExecutionMode.INCREMENTAL)
```

### Local Development

```bash
# Install in development mode
pip install -e .

# Run tests
pytest

# Run examples
python examples/basic_pipeline.py

# Build package
python -m build
```

### Docker Deployment

```dockerfile
FROM python:3.8-slim

# Install Java and Spark dependencies
RUN apt-get update && apt-get install -y openjdk-11-jdk

# Install SparkForge
COPY . /app
WORKDIR /app
RUN pip install -e .

# Run pipeline
CMD ["python", "examples/basic_pipeline.py"]
```

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Quick Start for Contributors

1. **Fork the repository**
2. **Clone your fork**: `git clone https://github.com/your-username/sparkforge.git`
3. **Install in development mode**: `pip install -e .`
4. **Run tests**: `pytest`
5. **Create a feature branch**: `git checkout -b feature/amazing-feature`
6. **Make your changes and add tests**
7. **Run tests**: `pytest`
8. **Submit a pull request**

### Development Setup

```bash
# Clone and setup
git clone https://github.com/your-username/sparkforge.git
cd sparkforge
pip install -e .

# Install development dependencies
pip install pytest pytest-cov black flake8 mypy

# Run code quality checks
black sparkforge/ tests/
flake8 sparkforge/ tests/
mypy sparkforge/

# Run tests with coverage
pytest --cov=sparkforge --cov-report=html
```

## 📞 Support

- **Documentation**: [Full documentation](https://sparkforge.readthedocs.io)
- **Issues**: [GitHub Issues](https://github.com/your-username/sparkforge/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-username/sparkforge/discussions)
- **Email**: support@sparkforge.dev

## 🏆 Acknowledgments

- Built on top of [Apache Spark](https://spark.apache.org/)
- Powered by [Delta Lake](https://delta.io/)
- Inspired by the Medallion Architecture pattern
- Thanks to the PySpark and Delta Lake communities

## 📈 Roadmap

- [ ] **v0.2.0**: Enhanced Delta Lake features (MERGE, VACUUM, OPTIMIZE)
- [ ] **v0.3.0**: Streaming pipeline support
- [ ] **v0.4.0**: ML pipeline integration
- [ ] **v0.5.0**: Cloud-native optimizations
- [ ] **v1.0.0**: Production-ready release

---

**Made with ❤️ for the data engineering community**