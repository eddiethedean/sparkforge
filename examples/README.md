# SparkForge Examples

This directory contains practical examples demonstrating SparkForge's capabilities, organized by feature categories.

## 🚀 Quick Start

### Absolute Beginner
**[Hello World](core/hello_world.py)** - The simplest possible pipeline (just 3 lines!)

### Getting Started
**[Basic Pipeline](core/basic_pipeline.py)** - Standard Bronze → Silver → Gold flow
**[Step-by-Step Execution](core/step_by_step_execution.py)** - Debug individual steps

## 📁 Example Categories

### Core Features (`core/`)
Essential SparkForge functionality for getting started.

- **[Hello World](core/hello_world.py)** ⭐ **START HERE**
  - Simplest possible pipeline (3 lines!)
  - Bronze → Silver → Gold flow
  - Basic data validation

- **[Basic Pipeline](core/basic_pipeline.py)**
  - Standard Bronze → Silver → Gold flow
  - Data validation and transformation
  - Complete working example

- **[Step-by-Step Execution](core/step_by_step_execution.py)**
  - Debug individual pipeline steps
  - Inspect intermediate data
  - Troubleshooting techniques

### Advanced Features (`advanced/`)
Advanced SparkForge capabilities for complex use cases.

- **[Multi-Schema Support](advanced/multi_schema_pipeline.py)** ⭐ **NEW!**
  - Cross-schema data flows
  - Multi-tenant applications
  - Environment separation

- **[Dynamic Parallel Execution](advanced/dynamic_parallel_execution.py)**
  - Advanced parallel processing
  - Worker allocation optimization
  - Performance monitoring

- **[Auto-Inference](advanced/auto_infer_source_bronze_simple.py)**
  - Automatic dependency detection
  - Simplified API usage
  - Reduced boilerplate

- **[Improved User Experience](advanced/improved_user_experience.py)**
  - Enhanced API features
  - Preset configurations
  - Validation helpers

### Use Case Examples (`usecases/`)
Real-world business scenarios and industry-specific pipelines.

- **[E-commerce Analytics](usecases/ecommerce_analytics.py)**
  - Order processing and customer analytics
  - Revenue tracking and business metrics
  - Complete business intelligence pipeline

- **[IoT Sensor Pipeline](usecases/iot_sensor_pipeline.py)**
  - Real-time sensor data processing
  - Anomaly detection and alerting
  - Time-series data analysis

- **[Step-by-Step Debugging](usecases/step_by_step_debugging.py)**
  - Advanced debugging techniques
  - Data quality analysis
  - Performance profiling

### Specialized Examples (`specialized/`)
Specific features and edge cases.

- **[Bronze Without Datetime](specialized/bronze_no_datetime_example.py)**
  - Full refresh pipelines
  - Bronze tables without timestamp columns
  - Overwrite mode behavior

- **[Column Filtering](specialized/column_filtering_behavior.py)**
  - Control column preservation
  - Validation behavior options
  - Downstream step requirements

## 🏃‍♂️ Running Examples

### Prerequisites
```bash
# Install SparkForge
pip install sparkforge

# Install dependencies
pip install pyspark delta-spark pandas numpy

# Ensure Java 8+ is installed
java -version
```

### Quick Run
```bash
# Start with the simplest example
python examples/core/hello_world.py

# Try a complete business pipeline
python examples/usecases/ecommerce_analytics.py

# Explore advanced features
python examples/advanced/multi_schema_pipeline.py
```

### All Examples
```bash
# Core features
python examples/core/hello_world.py
python examples/core/basic_pipeline.py
python examples/core/step_by_step_execution.py

# Advanced features
python examples/advanced/multi_schema_pipeline.py
python examples/advanced/dynamic_parallel_execution.py
python examples/advanced/auto_infer_source_bronze_simple.py
python examples/advanced/improved_user_experience.py

# Use cases
python examples/usecases/ecommerce_analytics.py
python examples/usecases/iot_sensor_pipeline.py
python examples/usecases/step_by_step_debugging.py

# Specialized
python examples/specialized/bronze_no_datetime_example.py
python examples/specialized/column_filtering_behavior.py
```

## 🎯 Learning Path

### Beginner (Start Here)
1. **[Hello World](core/hello_world.py)** - Simplest possible example
2. **[Basic Pipeline](core/basic_pipeline.py)** - Standard workflow
3. **[Step-by-Step Execution](core/step_by_step_execution.py)** - Debugging basics

### Intermediate
4. **[E-commerce Analytics](usecases/ecommerce_analytics.py)** - Real business scenario
5. **[Auto-Inference](advanced/auto_infer_source_bronze_simple.py)** - Simplified API
6. **[Improved UX](advanced/improved_user_experience.py)** - Enhanced features

### Advanced
7. **[Multi-Schema Support](advanced/multi_schema_pipeline.py)** - Cross-schema flows
8. **[Dynamic Parallel Execution](advanced/dynamic_parallel_execution.py)** - Performance optimization
9. **[IoT Sensor Pipeline](usecases/iot_sensor_pipeline.py)** - Complex real-time processing

### Expert
10. **[Step-by-Step Debugging](usecases/step_by_step_debugging.py)** - Advanced debugging
11. **[Column Filtering](specialized/column_filtering_behavior.py)** - Fine-grained control
12. **[Bronze Without Datetime](specialized/bronze_no_datetime_example.py)** - Edge cases

## 🔧 Customizing Examples

### Using Your Own Data
```python
# Replace sample data with your own
your_df = spark.read.parquet("path/to/your/data.parquet")
result = pipeline.initial_load(bronze_sources={"your_table": your_df})
```

### Adding Custom Transformations
```python
def your_transform(spark, bronze_df, prior_silvers):
    return bronze_df.withColumn("new_field", F.lit("value"))

builder.add_silver_transform(
    name="your_step",
    source_bronze="source_table",
    transform=your_transform,
    rules={"new_field": [F.col("new_field").isNotNull()]},
    table_name="your_table"
)
```

### Custom Validation Rules
```python
rules = {
    "email": [
        F.col("email").isNotNull(),
        F.col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")
    ],
    "age": [
        F.col("age").isNotNull(),
        F.col("age").between(0, 120)
    ]
}
```

## 🆘 Troubleshooting

### Common Issues
- **Java not found**: Install Java 8+ and set JAVA_HOME
- **Memory issues**: Increase Spark driver memory with `--driver-memory 4g`
- **Delta Lake errors**: Ensure Delta Lake is installed: `pip install delta-spark`
- **Permission errors**: Check write permissions for warehouse directory

### Getting Help
- [Complete Documentation](https://sparkforge.readthedocs.io/)
- [User Guide](https://sparkforge.readthedocs.io/en/latest/user_guide.html)
- [Troubleshooting Guide](https://sparkforge.readthedocs.io/en/latest/troubleshooting.html)

## 🤝 Contributing Examples

We welcome new examples! To contribute:

1. Create a new Python file in the appropriate category directory
2. Follow the naming convention: `descriptive_name.py`
3. Include comprehensive docstrings and comments
4. Add a description to this README
5. Test thoroughly and submit a pull request

### Example Template
```python
#!/usr/bin/env python3
"""
Your Example Name

Brief description of what this example demonstrates.
"""

from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

def main():
    """Main function to run the example."""

    print("Your Example")
    print("=" * 50)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Your Example") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # Your example code here
        pass

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Cleanup
        spark.stop()

if __name__ == "__main__":
    main()
```

---

**Happy Learning! 🚀**
