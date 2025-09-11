# Pipeline Builder

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold).

## 🚀 Features

- **Medallion Architecture**: Bronze → Silver → Gold data layering
- **Parallel Execution**: Independent Silver steps run in parallel
- **Data Validation**: Configurable validation thresholds for all stages
- **Incremental Processing**: Watermarking and incremental updates
- **Structured Logging**: Detailed execution logging and timing
- **Error Handling**: Comprehensive error handling and recovery
- **Configuration Management**: Flexible configuration options
- **Delta Lake Integration**: Full support for Delta tables

## 📁 Project Structure

```
pipeline_builder/
├── pipeline_builder/          # Main package
│   ├── __init__.py           # Package exports
│   ├── pipeline_builder.py   # Main PipelineBuilder class
│   ├── models.py             # Data models and dataclasses
│   ├── config.py             # Configuration management
│   ├── logger.py             # Logging and timing utilities
│   ├── utils.py              # Utility functions
│   ├── dependency_analyzer.py # Parallel execution analysis
│   └── execution_engine.py   # Pipeline execution engine
├── tests/                    # Test suite
│   ├── pipeline_tests.py     # Comprehensive test suite
│   └── run_tests.py          # Test runner with Spark setup
├── examples/                 # Usage examples
│   └── example_usage.py      # Example implementations
├── scripts/                  # Setup and utility scripts
│   ├── setup_delta_spark.py  # Delta Lake setup script
│   ├── setup_python38.sh     # Python 3.8 environment setup
│   └── activate_python38.sh  # Quick environment activation
├── jars/                     # Delta Lake JAR files
│   ├── delta-core_2.12-2.0.2.jar
│   └── delta-storage-2.0.2.jar
├── requirements.txt          # Python dependencies
└── README.md                # This file
```

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Java 11
- PySpark 3.2.4
- Delta Lake 2.0.2

### Quick Setup

1. **Set up Python 3.8 environment:**
   ```bash
   chmod +x scripts/setup_python38.sh
   ./scripts/setup_python38.sh
   ```

2. **Activate environment:**
   ```bash
   source scripts/activate_python38.sh
   ```

3. **Test Delta Lake setup:**
   ```bash
   python scripts/setup_delta_spark.py
   ```

## 🧪 Testing

Run the comprehensive test suite:

```bash
# Activate environment
source scripts/activate_python38.sh

# Run tests
python tests/run_tests.py
```

Expected output: **7/7 tests passed** ✅

## 📖 Usage

### Basic Pipeline

```python
from pipeline_builder import PipelineBuilder
from pyspark.sql import functions as F

# Create pipeline
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    min_bronze_rate=95.0,
    min_silver_rate=90.0,
    min_gold_rate=85.0,
    enable_parallel_silver=True,
    max_parallel_workers=4,
    verbose=True
)

# Define transforms
def silver_transform(spark, bronze_df):
    return bronze_df.filter(F.col("status") == "active")

def gold_transform(spark, silvers):
    events_df = silvers["silver_events"]
    return events_df.groupBy("category").count()

# Build and run pipeline
result = (builder
    .add_bronze_source("bronze_events", bronze_df)
    .add_silver_step("silver_events", silver_transform)
    .add_gold_step("gold_summary", gold_transform)
    .run()
)

print(f"Pipeline completed: {result.totals['total_rows_written']} rows written")
```

### Parallel Silver Execution

```python
# Silver steps without dependencies run in parallel
builder = PipelineBuilder(
    spark=spark,
    schema="my_schema",
    enable_parallel_silver=True,
    max_parallel_workers=4
)

result = (builder
    .add_bronze_source("bronze_events", bronze_df)
    .add_silver_step("silver_events", events_transform)      # Runs in parallel
    .add_silver_step("silver_users", users_transform)        # Runs in parallel
    .add_gold_step("gold_summary", gold_transform)
    .run()
)
```

## 🔧 Configuration

### Pipeline Configuration

```python
from pipeline_builder import PipelineConfig, ValidationThresholds, ParallelConfig

# Custom configuration
thresholds = ValidationThresholds(
    bronze=95.0,    # 95% data quality threshold for Bronze
    silver=90.0,    # 90% data quality threshold for Silver
    gold=85.0       # 85% data quality threshold for Gold
)

parallel = ParallelConfig(
    enabled=True,       # Enable parallel Silver execution
    max_workers=4       # Maximum parallel workers
)

config = PipelineConfig(
    schema="my_schema",
    thresholds=thresholds,
    parallel=parallel,
    verbose=True
)

builder = PipelineBuilder(spark=spark, config=config)
```

## 📊 Monitoring

The pipeline provides comprehensive monitoring:

- **Execution Timing**: Detailed timing for each stage
- **Data Quality Metrics**: Validation results and row counts
- **Error Handling**: Detailed error messages and stack traces
- **Structured Logging**: JSON-formatted logs for monitoring systems

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

The pipeline is designed for Databricks environments:

```python
# In Databricks notebook
from pipeline_builder import PipelineBuilder

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

### Local Development

For local development and testing:

```bash
# Set up environment
source scripts/activate_python38.sh

# Run tests
python tests/run_tests.py

# Run examples
python examples/example_usage.py
```

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📞 Support

For questions and support, please open an issue in the repository.