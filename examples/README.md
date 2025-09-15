# SparkForge Examples

This directory contains practical examples demonstrating SparkForge's capabilities.

## Examples Overview

### 0. Hello World (`hello_world.py`) ⭐ **START HERE**
**Perfect for absolute beginners!**

The simplest possible SparkForge pipeline - just 3 lines of pipeline code! This demonstrates the Bronze → Silver → Gold flow with minimal complexity.

**Features:**
- Simplest possible pipeline
- Bronze → Silver → Gold flow
- Basic data validation
- Step-by-step execution

**Run:**
```bash
python examples/hello_world.py
```

### 1. E-commerce Analytics Pipeline (`ecommerce_analytics.py`)

A complete e-commerce analytics pipeline that processes order data through Bronze → Silver → Gold layers.

**Features:**
- Order data ingestion and validation
- Customer profile creation
- Daily sales analytics
- Customer segmentation and analytics
- Revenue analysis by product category

**Key Concepts:**
- Bronze layer data validation
- Silver layer data enrichment
- Gold layer business analytics
- Customer profiling and segmentation
- Revenue analysis and reporting

**Run:**
```bash
python examples/ecommerce_analytics.py
```

### 2. IoT Sensor Data Pipeline (`iot_sensor_pipeline.py`)

An IoT sensor data processing pipeline with anomaly detection and real-time analytics.

**Features:**
- Sensor data ingestion (temperature, humidity, pressure, vibration)
- Anomaly detection and classification
- Sensor health monitoring
- Zone-based analytics
- Data quality assessment

**Key Concepts:**
- Time-series data processing
- Anomaly detection algorithms
- Sensor health monitoring
- Zone-based aggregations
- Data quality metrics

**Run:**
```bash
python examples/iot_sensor_pipeline.py
```

### 3. Step-by-Step Debugging (`step_by_step_debugging.py`)

Demonstrates how to debug individual pipeline steps using SparkForge's debugging capabilities.

**Features:**
- Individual step execution
- Step validation debugging
- Data quality inspection
- Execution state monitoring
- Performance profiling

**Key Concepts:**
- Step-by-step execution
- Validation debugging
- Data quality inspection
- Performance monitoring
- Error handling and recovery

**Run:**
```bash
python examples/step_by_step_debugging.py
```

## Running the Examples

### Prerequisites

1. **Install SparkForge:**
   ```bash
   pip install sparkforge
   ```

2. **Install Dependencies:**
   ```bash
   pip install pyspark delta-spark pandas numpy
   ```

3. **Java 8+** (required for PySpark)

### Running Examples

1. **Navigate to the project directory:**
   ```bash
   cd sparkforge
   ```

2. **Run any example:**
   ```bash
   python examples/ecommerce_analytics.py
   python examples/iot_sensor_pipeline.py
   python examples/step_by_step_debugging.py
   ```

### Example Output

Each example will:
- Create sample data
- Build a complete pipeline
- Execute the pipeline
- Display results and analytics
- Show performance metrics
- Clean up resources

## Learning Path

### Beginner
1. Start with `hello_world.py` for the simplest possible example
2. Try `step_by_step_debugging.py` to understand basic concepts
3. Run `ecommerce_analytics.py` to see a complete business pipeline

### Intermediate
1. Modify the examples to use your own data
2. Experiment with different validation rules
3. Try different execution modes (incremental, full refresh)
4. Add custom transformations

### Advanced
1. Implement custom validation functions
2. Add complex Silver-to-Silver dependencies
3. Optimize for performance with parallel execution
4. Integrate with your existing data infrastructure

## Customizing Examples

### Using Your Own Data

Replace the sample data creation with your own data:

```python
# Instead of create_sample_data(spark)
your_df = spark.read.parquet("path/to/your/data.parquet")

# Use in pipeline
result = pipeline.initial_load(bronze_sources={"your_table": your_df})
```

### Adding Custom Transformations

```python
def your_custom_transform(spark, bronze_df, prior_silvers):
    # Your custom logic here
    return bronze_df.withColumn("new_column", F.lit("value"))

builder.add_silver_transform(
    name="your_step",
    source_bronze="source_table",
    transform=your_custom_transform,
    rules={"new_column": [F.col("new_column").isNotNull()]},
    table_name="your_table"
)
```

### Custom Validation Rules

```python
# Add complex validation rules
rules = {
    "email": [
        F.col("email").isNotNull(),
        F.col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")
    ],
    "age": [
        F.col("age").isNotNull(),
        F.col("age").between(0, 120)
    ],
    "amount": [
        F.col("amount").isNotNull(),
        F.col("amount") > 0,
        F.col("amount") < 1000000
    ]
}
```

### 7. Dynamic Parallel Execution (`dynamic_parallel_execution.py`)

Advanced parallel execution with dynamic worker allocation and intelligent optimization.

**Features:**
- Dynamic worker allocation based on workload analysis
- Intelligent task prioritization and dependency management
- Resource-aware execution planning
- Performance monitoring and optimization
- Work-stealing algorithms for optimal resource utilization
- Multiple worker allocation strategies

**Key Concepts:**
- Dynamic parallel execution
- Worker allocation optimization
- Task prioritization
- Resource management
- Performance monitoring
- Adaptive optimization

**Run:**
```bash
python examples/dynamic_parallel_execution.py
```

### 8. E-commerce Analytics (`ecommerce_analytics.py`)

Complete e-commerce data pipeline with Bronze, Silver, and Gold layers.

**Features:**
- Order processing and customer analytics
- Revenue tracking and business metrics
- Data quality validation and monitoring
- Incremental processing with Delta Lake
- Performance optimization and caching

**Key Concepts:**
- Real-world data pipeline
- Business intelligence
- Customer analytics
- Revenue tracking
- Data quality management

**Run:**
```bash
python examples/ecommerce_analytics.py
```

### 9. IoT Sensor Pipeline (`iot_sensor_pipeline.py`)

Real-time IoT sensor data processing with anomaly detection.

**Features:**
- Time-series data processing
- Anomaly detection and alerting
- Real-time analytics and monitoring
- Sensor data validation and quality checks
- Performance optimization for high-volume data

**Key Concepts:**
- IoT data processing
- Time-series analysis
- Anomaly detection
- Real-time processing
- Sensor data validation

**Run:**
```bash
python examples/iot_sensor_pipeline.py
```

### 10. Step-by-Step Debugging (`step_by_step_debugging.py`)

Advanced debugging techniques for complex pipeline issues.

**Features:**
- Individual step execution and inspection
- Data quality analysis and validation
- Performance profiling and optimization
- Error diagnosis and troubleshooting
- Pipeline state inspection

**Key Concepts:**
- Pipeline debugging
- Step-by-step execution
- Data quality analysis
- Performance profiling
- Error diagnosis

**Run:**
```bash
python examples/step_by_step_debugging.py
```

## Troubleshooting

### Common Issues

1. **Java not found:**
   - Install Java 8+ and set JAVA_HOME environment variable

2. **Memory issues:**
   - Increase Spark driver memory: `--driver-memory 4g`

3. **Delta Lake errors:**
   - Ensure Delta Lake is properly installed: `pip install delta-spark`

4. **Permission errors:**
   - Check write permissions for the warehouse directory

### Getting Help

- Check the [User Guide](../USER_GUIDE.md) for detailed documentation
- Review the [API Reference](../API_REFERENCE.md) for complete API documentation
- Look at the [Quick Reference](../QUICK_REFERENCE.md) for common patterns

## Contributing Examples

We welcome contributions! To add a new example:

1. Create a new Python file in this directory
2. Follow the naming convention: `descriptive_name.py`
3. Include comprehensive docstrings and comments
4. Add a description to this README
5. Test the example thoroughly
6. Submit a pull request

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
