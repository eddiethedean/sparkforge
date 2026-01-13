# Pipeline Builder ⚡

> **The modern data pipeline framework for Apache Spark & Delta Lake**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://sparkforge.readthedocs.io/)
[![Tests](https://img.shields.io/badge/tests-1545%20passed-brightgreen.svg)](https://github.com/eddiethedean/sparkforge)
[![Coverage](https://img.shields.io/badge/coverage-83%25-brightgreen.svg)](https://github.com/eddiethedean/sparkforge)
[![Type Safety](https://img.shields.io/badge/type%20safety-100%25-brightgreen.svg)](https://github.com/eddiethedean/sparkforge)
[![CI/CD](https://github.com/eddiethedean/sparkforge/workflows/Tests/badge.svg)](https://github.com/eddiethedean/sparkforge/actions)

**Pipeline Builder** is a production-ready data pipeline framework that transforms complex Spark + Delta Lake development into clean, maintainable code. Built on the proven Medallion Architecture (Bronze → Silver → Gold), it eliminates boilerplate while providing enterprise-grade features.

## ✨ Why Pipeline Builder?

| **Before Pipeline Builder** | **With Pipeline Builder** |
|----------------------|-------------------|
| 200+ lines of complex Spark code | 20 lines of clean, readable code |
| Manual dependency management | Automatic inference & validation |
| Scattered validation logic | Centralized, configurable rules |
| Hard-to-debug pipelines | Step-by-step execution & debugging |
| No built-in error handling | Comprehensive error management |
| Manual schema management | Multi-schema support out-of-the-box |

## 🚀 Quick Start

### Installation

**Install from repository:**
```bash
# Clone the repository
git clone https://github.com/eddiethedean/sparkforge.git
cd sparkforge

# Install in development mode
pip install -e ".[pyspark]"
```

**For testing/development with mock-spark:**
```bash
pip install -e ".[mock]"
```

**For PySpark compatibility testing:**
```bash
pip install -e ".[compat-test]"
```

**Note:** PipelineBuilder now supports both PySpark and mock-spark. Choose the installation method that best fits your use case. The framework automatically detects which engine is available.

### Quick Start Example
```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession

# Initialize Spark and configure engine (required!)
spark = SparkSession.builder.appName("QuickStart").getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# Sample data
data = [
    ("user1", "prod1", 2, 29.99),
    ("user2", "prod2", 1, 49.99),
    ("user3", "prod1", 3, 29.99),
]
df = spark.createDataFrame(data, ["user_id", "product_id", "quantity", "price"])

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="quickstart")
builder.with_bronze_rules(
    name="raw_orders",
    rules={"user_id": [F.col("user_id").isNotNull()], "quantity": [F.col("quantity") > 0]}
)

def clean_orders(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return bronze_df.filter(F.col("quantity") > 0)

builder.add_silver_transform(
    name="clean_orders",
    source_bronze="raw_orders",
    transform=clean_orders,
    rules={"quantity": [F.col("quantity") > 0]},
    table_name="clean_orders"
)

def summary(spark, silvers):
    F = get_default_functions()
    return silvers["clean_orders"].groupBy("product_id").agg(
        F.sum("quantity").alias("total")
    )

builder.add_gold_transform(
    name="summary",
    transform=summary,
    rules={"product_id": [F.col("product_id").isNotNull()]},
    table_name="summary",
    source_silvers=["clean_orders"]
)

# Execute pipeline
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"raw_orders": df})
print(f"Status: {result.status.value}")
```

### Your First Pipeline
```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession

# Initialize Spark and configure engine (required!)
spark = SparkSession.builder.appName("EcommerceAnalytics").getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

# Sample e-commerce data
events_data = [
    ("user_123", "purchase", "2024-01-15 10:30:00", 99.99, "electronics"),
    ("user_456", "view", "2024-01-15 11:15:00", 49.99, "clothing"),
    ("user_123", "add_to_cart", "2024-01-15 12:00:00", 29.99, "books"),
    ("user_789", "purchase", "2024-01-15 14:30:00", 199.99, "electronics"),
]
source_df = spark.createDataFrame(events_data, ["user_id", "action", "timestamp", "price", "category"])

# Build the pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze: Raw event ingestion with validation
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "action": [F.col("action").isNotNull()],
        "price": [F.col("price") > 0]
    },
    incremental_col="timestamp"
)

# Silver: Clean and enrich the data
def enrich_events(spark, bronze_df, prior_silvers):
    F = get_default_functions()
    return (
        bronze_df.withColumn("event_date", F.to_date("timestamp"))
          .withColumn("hour", F.hour("timestamp"))
          .withColumn("is_purchase", F.col("action") == "purchase")
          .filter(F.col("user_id").isNotNull())
    )

builder.add_silver_transform(
    name="enriched_events",
    source_bronze="events",
    transform=enrich_events,
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_date": [F.col("event_date").isNotNull()]
    },
    table_name="enriched_events"
)

# Gold: Business analytics
def daily_revenue(spark, silvers):
    F = get_default_functions()
    return (
        silvers["enriched_events"]
        .filter(F.col("is_purchase"))
        .groupBy("event_date")
        .agg(
            F.count("*").alias("total_purchases"),
            F.sum("price").alias("total_revenue"),
            F.countDistinct("user_id").alias("unique_customers")
        )
        .orderBy("event_date")
    )

builder.add_gold_transform(
    name="daily_revenue",
    transform=daily_revenue,
    rules={
        "event_date": [F.col("event_date").isNotNull()],
        "total_revenue": [F.col("total_revenue") >= 0]
    },
    table_name="daily_revenue",
    source_silvers=["enriched_events"]
)

# Execute the pipeline
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": source_df})

print(f"✅ Pipeline completed: {result.status.value}")
print(f"📊 Processed {result.metrics.total_rows_written} rows")
```

## ⚡ Dependency-Aware Sequential Execution

PipelineBuilder automatically analyzes your pipeline dependencies and executes steps in the correct order based on their dependencies. Steps are executed sequentially within dependency groups for predictable, deterministic execution.

### How It Works

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions

configure_engine(spark=spark)
F = get_default_functions()

# Your pipeline with multiple steps
builder = PipelineBuilder(spark=spark, schema="analytics")

# These 3 bronze steps execute sequentially in dependency order
builder.with_bronze_rules(name="events_a", rules={"id": [F.col("id").isNotNull()]})
builder.with_bronze_rules(name="events_b", rules={"id": [F.col("id").isNotNull()]})
builder.with_bronze_rules(name="events_c", rules={"id": [F.col("id").isNotNull()]})

# These 3 silver steps execute sequentially after their bronze dependencies
builder.add_silver_transform(name="clean_a", source_bronze="events_a", ...)
builder.add_silver_transform(name="clean_b", source_bronze="events_b", ...)
builder.add_silver_transform(name="clean_c", source_bronze="events_c", ...)

# Gold step runs after all silver steps complete
builder.add_gold_transform(name="analytics", source_silvers=["clean_a", "clean_b", "clean_c"], ...)

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={...})

# Check execution metrics
print(f"📊 Total rows written: {result.metrics.total_rows_written}")
print(f"⏱️  Execution time: {result.duration_seconds:.2f}s")
```

### Execution Flow

```
Timeline (with 3 steps, 2s each):

Sequential Execution:
┌──────┐
│Step A│ 2s
├──────┤
│Step B│ 2s
├──────┤
│Step C│ 2s
└──────┘
Total: 6s

Deterministic and predictable execution order!
```

### Key Benefits

- 🧠 **Dependency-aware** - Automatically respects step dependencies
- 🔒 **Deterministic** - Predictable execution order
- 🐛 **Easy debugging** - Sequential execution simplifies troubleshooting
- 📊 **Observable** - Detailed metrics for each step
- 🎯 **No race conditions** - Thread-safe execution

## 🎨 String Rules - Human-Readable Validation

PipelineBuilder supports both PySpark expressions and human-readable string rules:

```python
# String rules (automatically converted to PySpark expressions)
rules = {
    "user_id": ["not_null"],                    # F.col("user_id").isNotNull()
    "age": ["gt", 0],                          # F.col("age") > 0
    "status": ["in", ["active", "inactive"]],  # F.col("status").isin(["active", "inactive"])
    "score": ["between", 0, 100],              # F.col("score").between(0, 100)
    "email": ["like", "%@%.%"]                 # F.col("email").like("%@%.%")
}

# Or use PySpark expressions directly
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age") > 0],
    "status": [F.col("status").isin(["active", "inactive"])]
}
```

**Supported String Rules:**
- `"not_null"` → `F.col("column").isNotNull()`
- `"gt", value` → `F.col("column") > value`
- `"gte", value` → `F.col("column") >= value`
- `"lt", value` → `F.col("column") < value`
- `"lte", value` → `F.col("column") <= value`
- `"eq", value` → `F.col("column") == value`
- `"in", [values]` → `F.col("column").isin(values)`
- `"between", min, max` → `F.col("column").between(min, max)`
- `"like", pattern` → `F.col("column").like(pattern)`

## 🎯 Core Features

### 🏗️ **Medallion Architecture Made Simple**
- **Bronze Layer**: Raw data ingestion with validation
- **Silver Layer**: Cleaned, enriched, and transformed data
- **Gold Layer**: Business-ready analytics and metrics
- **Automatic dependency management** between layers

### ⚡ **Developer Experience**
- **70% less boilerplate** compared to raw Spark
- **Auto-inference** of data dependencies
- **Step-by-step debugging** for complex pipelines
- **Preset configurations** for dev/prod/test environments
- **Comprehensive error handling** with actionable messages

### 🛡️ **Production Ready**
- **Robust validation system** with early error detection
- **Configurable validation thresholds** (Bronze: 90%, Silver: 95%, Gold: 98%)
- **Delta Lake integration** with ACID transactions
- **Multi-schema support** for enterprise environments
- **Performance monitoring** and optimization
- **Comprehensive logging** and audit trails
- **83% test coverage** with 1,441 comprehensive tests
- **100% type safety** with mypy compliance
- **Security hardened** with zero security vulnerabilities

### 🔧 **Advanced Capabilities**
- **Service-oriented architecture** - Modular design with dedicated services for validation, storage, transformation, and reporting
- **Step executors** - Dedicated executors for Bronze, Silver, and Gold steps
- **String rules support** - Human-readable validation rules (`"not_null"`, `"gt", 0`, `"in", ["active", "inactive"]`)
- **Engine configuration** - Works with both real PySpark and mock Spark for testing
- **Incremental processing** with watermarking
- **Schema evolution** support
- **Time travel** and data versioning
- **Comprehensive error handling** with centralized error management

## 📚 Examples & Use Cases

### 🎯 **Core Examples**
- **[Hello World](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/core/hello_world.py)** - 3-line pipeline introduction
- **[Basic Pipeline](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/core/basic_pipeline.py)** - Complete Bronze → Silver → Gold flow
- **[Step-by-Step Debugging](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/core/step_by_step_execution.py)** - Debug individual steps

### 🚀 **Advanced Features**
- **[Auto-Inference](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/advanced/auto_infer_source_bronze_simple.py)** - Automatic dependency detection
- **[Multi-Schema Support](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/advanced/multi_schema_pipeline.py)** - Cross-schema data flows
- **[Column Filtering](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/specialized/column_filtering_behavior.py)** - Control data preservation

### 🏢 **Real-World Use Cases**
- **[E-commerce Analytics](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/usecases/ecommerce_analytics.py)** - Order processing, customer insights
- **[IoT Sensor Data](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/usecases/iot_sensor_pipeline.py)** - Real-time sensor processing
- **[Business Intelligence](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/usecases/step_by_step_debugging.py)** - KPI dashboards, reporting

## 📊 LogWriter - Pipeline Execution Tracking

Track and analyze your pipeline executions with the simplified LogWriter API:

### Quick Example

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.writer import LogWriter
from pipeline_builder.engine_config import configure_engine

# Configure engine (required!)
configure_engine(spark=spark)

# Build and run your pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")
# ... add steps ...
pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": df})

# Initialize LogWriter (simple API - just schema and table name!)
writer = LogWriter(spark=spark, schema="logs", table_name="pipeline_execution")

# Create log table from first report
writer.create_table(result)

# Append subsequent runs
result2 = pipeline.run_incremental(bronze_sources={"events": df2})
writer.append(result2)

# Query your logs
logs = spark.table("logs.pipeline_execution")
logs.show()
```

### Key Features

- ✅ **Simple initialization** - Just provide `schema` and `table_name`
- ✅ **Works with PipelineReport** - Direct integration with pipeline results
- ✅ **Easy methods** - `create_table()` and `append()` for intuitive workflow
- ✅ **Comprehensive metrics** - Tracks rows processed, durations, success rates
- ✅ **Detailed metadata** - Layer durations, warnings, recommendations
- ✅ **Emoji-rich output** - Visual feedback during execution (📊✅❌)

### What Gets Logged

Each pipeline execution is logged with:
- **Run information**: run_id, mode (initial/incremental), timestamps
- **Execution metrics**: total steps, successful/failed counts, durations by layer
- **Data metrics**: rows processed, rows written, validation rates
- **Performance**: execution groups, dependency analysis
- **Status**: success/failure, error messages, warnings, recommendations

### Example Log Query

```python
# Get recent pipeline runs
recent_runs = spark.sql("""
    SELECT run_id, run_mode, success, rows_written, duration_secs
    FROM logs.pipeline_execution
    WHERE run_started_at >= current_date() - 7
    ORDER BY run_started_at DESC
""")

# Analyze performance trends
performance = spark.sql("""
    SELECT 
        DATE(run_started_at) as date,
        COUNT(*) as runs,
        AVG(duration_secs) as avg_duration,
        SUM(rows_written) as total_rows
    FROM logs.pipeline_execution
    WHERE success = true
    GROUP BY DATE(run_started_at)
    ORDER BY date DESC
""")
```

See **[examples/specialized/logwriter_simple_example.py](https://github.com/eddiethedean/pipeline_builder/blob/main/examples/specialized/logwriter_simple_example.py)** for a complete working example.

## 🛠️ Installation & Setup

### Prerequisites
- **Python 3.8+** (tested with 3.8, 3.9, 3.10, 3.11)
- **Java 17** (for Spark 3.5)
- **PySpark 3.5+**
- **Delta Lake 3.0.0+**

### Quick Install
```bash
# Clone the repository
git clone https://github.com/eddiethedean/sparkforge.git
cd sparkforge

# Install in development mode
pip install -e .

# Verify installation
python -c "import pipeline_builder; print(f'PipelineBuilder {pipeline_builder.__version__} installed!')"
```

### Development Install
```bash
# Clone the repository
git clone https://github.com/eddiethedean/sparkforge.git
cd sparkforge

# Setup Python 3.8 environment with PySpark 3.5
python3.8 -m venv venv38
source venv38/bin/activate
pip install --upgrade pip

# Install with all dependencies
pip install -e ".[dev,test,docs]"

# Verify installation
python test_environment.py
```

**Quick Setup Script** (Recommended):
```bash
bash setup.sh  # Automated setup for development environment
```

See [QUICKSTART.md](https://github.com/eddiethedean/sparkforge/blob/main/QUICKSTART.md) for detailed setup instructions.

## 📖 Documentation

### 📚 **Complete Documentation**
- **[📖 Full Documentation](https://pipeline_builder.readthedocs.io/)** - Comprehensive guides and API reference
- **[⚡ 5-Minute Quick Start](https://pipeline_builder.readthedocs.io/en/latest/quick_start_5_min.html)** - Get running fast
- **[🎯 User Guide](https://pipeline_builder.readthedocs.io/en/latest/user_guide.html)** - Complete feature walkthrough
- **[🔧 API Reference](https://pipeline_builder.readthedocs.io/en/latest/api_reference.html)** - Detailed API documentation

### 🎯 **Use Case Guides**
- **[🛒 E-commerce Analytics](https://pipeline_builder.readthedocs.io/en/latest/usecase_ecommerce.html)** - Order processing, customer analytics
- **[📡 IoT Data Processing](https://pipeline_builder.readthedocs.io/en/latest/usecase_iot.html)** - Sensor data, anomaly detection
- **[📊 Business Intelligence](https://pipeline_builder.readthedocs.io/en/latest/usecase_bi.html)** - Dashboards, KPIs, reporting

## 🧪 Testing & Quality

PipelineBuilder includes a comprehensive test suite with **1,441 tests** covering all functionality:

```bash
# Run all tests with coverage and type checking (recommended)
make test

# Run all tests (standard)
pytest tests/ -v

# Run by category
pytest tests/unit/ -v              # Unit tests
pytest tests/integration/ -v       # Integration tests
pytest tests/system/ -v            # System tests

# Run with coverage
pytest tests/ --cov=pipeline_builder --cov-report=html

# Activate environment
source activate_env.sh             # Loads Python 3.8 + PySpark 3.5

# Verify environment
python scripts/test_python38_environment.py  # Comprehensive environment check

# Code quality checks
make format                        # Format code with Black and isort
make lint                          # Run ruff and pylint
make type-check                    # Type checking with mypy
make security                      # Security scan with bandit
```

**Quality Metrics**:
- ✅ **1,441 tests passed** (100% pass rate)
- ✅ **83% test coverage** across all modules
- ✅ **100% type safety** with mypy compliance (43 source files)
- ✅ **Zero security vulnerabilities** (bandit clean)
- ✅ **Code formatting** compliant (Black + isort + ruff)
- ✅ **Python 3.8-3.11 compatible**

## 🤝 Contributing

We welcome contributions! Here's how to get started:

### Quick Start for Contributors
1. **Fork the repository**
2. **Clone your fork**: `git clone https://github.com/yourusername/pipeline_builder.git`
3. **Setup environment**: `bash setup.sh` or see [QUICKSTART.md](https://github.com/eddiethedean/pipeline_builder/blob/main/QUICKSTART.md)
4. **Activate environment**: `source activate_env.sh`
5. **Run tests**: `make test` (1,441 tests, 100% pass rate)
6. **Create a feature branch**: `git checkout -b feature/amazing-feature`
7. **Make your changes and add tests**
8. **Format code**: `make format`
9. **Submit a pull request**

### Development Guidelines
- Follow the existing code style (Black formatting + isort + ruff)
- Add tests for new features (aim for 90%+ coverage)
- Ensure type safety with mypy compliance
- Run security scan with bandit
- Update documentation as needed
- Ensure all tests pass: `make test`
- Python 3.8 required for development (as per project standards)

## 📊 Performance & Benchmarks

| Metric | PipelineBuilder | Raw Spark | Improvement |
|--------|------------|-----------|-------------|
| **Lines of Code** | 20 lines | 200+ lines | **90% reduction** |
| **Development Time** | 30 minutes | 4+ hours | **87% faster** |
| **Execution Speed** | Sequential (dependency-aware) | Manual | **Deterministic** |
| **Test Coverage** | 83% (1,400 tests) | Manual | **Comprehensive** |
| **Type Safety** | 100% mypy compliant | None | **Production-ready** |
| **Security** | Zero vulnerabilities | Manual | **Enterprise-grade** |
| **Error Handling** | Built-in + Early Validation | Manual | **Production-ready** |
| **Debugging** | Step-by-step | Complex | **Developer-friendly** |
| **Validation** | Automatic + Configurable | Manual | **Enterprise-grade** |

### Service-Oriented Architecture

SparkForge uses a service-oriented architecture for better maintainability:

```
ExecutionEngine
├── ExecutionValidator - Data quality validation
├── TableService - Table operations and schema management
├── WriteService - Write operations to Delta Lake
├── TransformService - Transformation logic
├── ExecutionReporter - Execution reporting
└── ErrorHandler - Centralized error handling
```

Each service has a single responsibility and can be tested independently.

## 🚀 What's New in v2.1.2

### 📈 Logging & Observability
- ✅ **Accurate table totals** – `table_total_rows` now refreshes after every write, keeping incremental append/overwrite runs in sync with the latest Spark counts
- ✅ **Additional regression tests** – Coverage added to ensure totals are recalculated whenever log rows are written
- ✅ **Gold overwrite guarantees** – Gold-layer steps always perform `overwrite` writes (and log them accordingly), even during incremental runs, preventing duplicate aggregates

### 🛠 Environment & Runtime Support
- ✅ **PySpark 3.5 + Java 17 by default** – Configuration, docs, and tooling aligned to the new runtime baseline
- ✅ **Updated dependency ranges** – `pyspark` and `delta-spark` optional extras track the latest stable 3.5/3.0 lines
- ✅ **Engine configuration** – Required engine configuration for both real PySpark and mock Spark

### 🧪 Reliability & Tooling
- ✅ **Parallel test stability** – Warehouse schemas are randomized per test to eliminate Delta collisions under `pytest -n`
- ✅ **Continuous quality gates** – `ruff check pipeline_builder` and `mypy pipeline_builder` pass cleanly as part of CI guardrails

## 🚀 What's New in v1.2.0

### 📊 **NEW: Enhanced Logging with Rich Metrics**
- ✅ **Unified logging format** - Consistent timestamps, emojis, and formatting
- ✅ **Detailed metrics** - Rows processed, rows written, invalid counts, validation rates
- ✅ **Visual indicators** - 🚀 Starting, ✅ Completed, ❌ Failed with clear status
- ✅ **Smart formatting** - Bronze shows "processed", Silver/Gold show "written"
- ✅ **Execution insights** - Duration tracking, group information

```
13:08:09 - PipelineRunner - INFO - 🚀 Starting BRONZE step: bronze_events
13:08:09 - PipelineRunner - INFO - ✅ Completed BRONZE step: bronze_events (0.51s, 1,000 rows processed, validation: 100.0%)
13:08:12 - PipelineRunner - INFO - 🚀 Starting SILVER step: silver_purchases
13:08:13 - PipelineRunner - INFO - ✅ Completed SILVER step: silver_purchases (0.81s, 350 rows processed, 4 invalid, validation: 98.9%)
```

### ⚡ **Service-Oriented Architecture**
- ✅ **Modular design** - Dedicated services for validation, storage, transformation, and reporting
- ✅ **Step executors** - Dedicated executors for Bronze, Silver, and Gold steps
- ✅ **Dependency-aware execution** - Automatically respects step dependencies
- ✅ **Deterministic execution** - Predictable, sequential execution order
- ✅ **Easy debugging** - Sequential execution simplifies troubleshooting
- ✅ **Comprehensive error handling** - Centralized error management with detailed context

### 🎯 **Quality & Reliability**
- ✅ **100% type safety** - Complete mypy compliance across all 43 source files
- ✅ **Security hardened** - Zero vulnerabilities (bandit clean)
- ✅ **83% test coverage** - Comprehensive test suite with 1,441 tests
- ✅ **Code quality** - Black formatting + isort + ruff linting
- ✅ **Production ready** - All quality gates passed

### 🔧 **Enhanced Features**
- ✅ **Robust validation system** - Early error detection with clear messages
- ✅ **String rules support** - Human-readable validation rules
- ✅ **Comprehensive error handling** - Detailed error context and suggestions
- ✅ **Improved documentation** - Updated docstrings with examples
- ✅ **Mock Functions compatibility** - Enhanced mock-spark support for testing
- ✅ **Better test alignment** - Tests now reflect actual intended behavior
- ✅ **Optimized test runner** - Type checking only on source code, not tests

## 🏆 What Makes PipelineBuilder Different?

### ✅ **Built for Production**
- **Enterprise-grade error handling** with detailed context
- **Configurable validation thresholds** for data quality
- **Multi-schema support** for complex environments
- **Performance monitoring** and optimization
- **100% type safety** with comprehensive mypy compliance
- **Security hardened** with zero vulnerabilities
- **83% test coverage** with 1,284 comprehensive tests

### ✅ **Developer-First Design**
- **Clean, readable API** that's easy to understand
- **Comprehensive documentation** with real-world examples
- **Step-by-step debugging** for complex pipelines
- **Auto-inference** reduces boilerplate by 70%

### ✅ **Modern Architecture**
- **Service-oriented design** with dedicated services for each concern
- **Delta Lake integration** with ACID transactions
- **Medallion Architecture** best practices built-in
- **Schema evolution** and time travel support
- **Incremental processing** with watermarking
- **Engine configuration** supporting both real PySpark and mock Spark

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](https://github.com/eddiethedean/pipeline_builder/blob/main/LICENSE) file for details.

## 🙏 Acknowledgments

- Built on top of [Apache Spark](https://spark.apache.org/) - the industry standard for big data processing
- Powered by [Delta Lake](https://delta.io/) - reliable data lakehouse storage
- Inspired by the Medallion Architecture pattern for data lakehouse design
- Thanks to the PySpark and Delta Lake communities for their excellent work

---

<div align="center">

**Made with ❤️ for the data engineering community**

[⭐ Star us on GitHub](https://github.com/eddiethedean/pipeline_builder) • [📖 Read the docs](https://pipeline_builder.readthedocs.io/) • [🐛 Report issues](https://github.com/eddiethedean/pipeline_builder/issues) • [💬 Join discussions](https://github.com/eddiethedean/pipeline_builder/discussions)

</div>
