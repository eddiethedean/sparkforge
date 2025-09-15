# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

# SparkForge

A production-ready PySpark + Delta Lake pipeline engine with the Medallion Architecture (Bronze → Silver → Gold). Build scalable data pipelines with built-in parallel execution, comprehensive validation, and enterprise-grade monitoring.

[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://sparkforge.readthedocs.io/)
[![PyPI version](https://badge.fury.io/py/sparkforge.svg)](https://badge.fury.io/py/sparkforge)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Quick Start

### Installation
```bash
pip install sparkforge
```

### Minimal Example (3 lines!)
```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyPipeline").getOrCreate()
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Bronze → Silver → Gold pipeline
pipeline = (builder
    .with_bronze_rules(name="events", rules={"user_id": [F.col("user_id").isNotNull()]})
    .add_silver_transform(name="clean_events", source_bronze="events",
                         transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
                         rules={"status": [F.col("status").isNotNull()]}, table_name="clean_events")
    .add_gold_transform(name="daily_metrics", transform=lambda spark, silvers:
                       list(silvers.values())[0].groupBy("date").count(),
                       rules={"date": [F.col("date").isNotNull()]}, table_name="daily_metrics")
    .to_pipeline()
)

result = pipeline.initial_load(bronze_sources={"events": source_df})
```

## 📚 Feature Examples

### Core Features
- **[Hello World](examples/core/hello_world.py)** - Absolute simplest pipeline
- **[Basic Pipeline](examples/core/basic_pipeline.py)** - Standard Bronze → Silver → Gold flow
- **[Step-by-Step Execution](examples/core/step_by_step_execution.py)** - Debug individual steps

### Advanced Features
- **[Multi-Schema Support](examples/advanced/multi_schema_pipeline.py)** - Cross-schema data flows
- **[Dynamic Parallel Execution](examples/advanced/dynamic_parallel_execution.py)** - Advanced parallel processing
- **[Auto-Inference](examples/advanced/auto_infer_source_bronze_simple.py)** - Automatic dependency detection
- **[Column Filtering](examples/specialized/column_filtering_behavior.py)** - Control column preservation

### Use Case Examples
- **[E-commerce Analytics](examples/usecases/ecommerce_analytics.py)** - Order processing, customer analytics
- **[IoT Sensor Pipeline](examples/usecases/iot_sensor_pipeline.py)** - Real-time sensor data processing
- **[Step-by-Step Debugging](examples/usecases/step_by_step_debugging.py)** - Advanced debugging techniques

### Specialized Examples
- **[Bronze Without Datetime](examples/specialized/bronze_no_datetime_example.py)** - Full refresh pipelines
- **[Improved UX](examples/advanced/improved_user_experience.py)** - Enhanced user experience features

### 📖 [Complete Examples Guide](examples/README.md) - Organized by feature categories with learning paths

## 🎯 Key Features

- **🏗️ Medallion Architecture**: Bronze → Silver → Gold data layering with automatic dependency management
- **⚡ Advanced Parallel Execution**: Dynamic worker allocation, intelligent task prioritization, and adaptive optimization
- **🎯 Auto-Inference**: Automatically infers source dependencies, reducing boilerplate by 70%
- **🛠️ Preset Configurations**: One-line setup for development, production, and testing environments
- **🔧 Validation Helpers**: Built-in methods for common validation patterns (not_null, positive_numbers, etc.)
- **📊 Smart Detection**: Automatic timestamp column detection for watermarking
- **🏢 Multi-Schema Support**: Cross-schema data flows for multi-tenant, environment separation, and compliance
- **🔍 Step-by-Step Debugging**: Execute individual pipeline steps independently for troubleshooting
- **✅ Enhanced Data Validation**: Configurable validation thresholds with automatic security validation and performance caching
- **🎛️ Column Filtering Control**: Explicit control over which columns are preserved after validation
- **🔄 Incremental Processing**: Watermarking and incremental updates with Delta Lake
- **💧 Delta Lake Integration**: Full support for ACID transactions, time travel, and schema evolution

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
```

## 📖 Documentation

**📖 [Complete Documentation](https://sparkforge.readthedocs.io/)** - Professional documentation with search, navigation, and examples

### Quick Links
- **[5-Minute Quick Start](https://sparkforge.readthedocs.io/en/latest/quick_start_5_min.html)** - Get running in under 5 minutes ⭐ **START HERE**
- **[User Guide](https://sparkforge.readthedocs.io/en/latest/user_guide.html)** - Complete guide to all features
- **[API Reference](https://sparkforge.readthedocs.io/en/latest/api_reference.html)** - Complete API documentation
- **[Troubleshooting](https://sparkforge.readthedocs.io/en/latest/troubleshooting.html)** - Common issues and solutions

### Use Case Guides
- **[E-commerce Analytics](https://sparkforge.readthedocs.io/en/latest/usecase_ecommerce.html)** - Order processing, customer analytics
- **[IoT Data Processing](https://sparkforge.readthedocs.io/en/latest/usecase_iot.html)** - Sensor data, anomaly detection
- **[Business Intelligence](https://sparkforge.readthedocs.io/en/latest/usecase_bi.html)** - Dashboards, KPIs, reporting

## 🧪 Testing

Run the comprehensive test suite with 500+ tests:

```bash
# Fast parallel tests (recommended for development)
python tests/run_tests_parallel.py --workers 4

# Run all tests with coverage
pytest --cov=sparkforge --cov-report=html

# Run specific test categories
pytest -m "not slow"                    # Skip slow tests
pytest -m "delta"                       # Delta Lake tests only
pytest tests/test_integration_*.py      # Integration tests only
```

**Performance Benefits:**
- **4x speedup** for core tests (22s vs 2+ minutes)
- **Smart categorization** of parallel vs sequential tests
- **Zero failures** with reliable parallel execution
- **Optimized test suite** with no duplicate tests

## 🚀 Production Deployment

### Databricks
```python
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
from sparkforge import PipelineBuilder

# Configure for cloud storage
builder = PipelineBuilder(spark=spark, schema="my_schema")
pipeline = builder.to_pipeline()
result = pipeline.run_incremental(bronze_sources={"events": source_df})
```

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

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏆 Acknowledgments

- Built on top of [Apache Spark](https://spark.apache.org/)
- Powered by [Delta Lake](https://delta.io/)
- Inspired by the Medallion Architecture pattern
- Thanks to the PySpark and Delta Lake communities

---

**Made with ❤️ for the data engineering community**
