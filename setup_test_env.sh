#!/bin/bash

# Setup script for SparkForge test environment
echo "🔧 Setting up SparkForge test environment..."

# Activate virtual environment
source venv38/bin/activate

# Set Java environment
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
export PATH=$JAVA_HOME/bin:$PATH

# Set Spark environment variables
export SPARK_HOME=$(python -c "import pyspark; print(pyspark.__file__.replace('/__init__.py', ''))")
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip:$PYTHONPATH

# Set additional Spark configuration
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_DRIVER_HOST=127.0.0.1
export SPARK_DRIVER_BIND_ADDRESS=127.0.0.1

echo "✅ Environment setup complete!"
echo "Java version: $(java -version 2>&1 | head -1)"
echo "Python version: $(python --version)"
echo "PySpark version: $(python -c "import pyspark; print(pyspark.__version__)")"

# Run tests
echo "🧪 Running tests..."
python -m pytest tests/test_constants.py tests/test_models.py tests/test_exceptions.py tests/test_execution_engine.py tests/test_logger.py tests/test_performance.py tests/test_performance_cache_basic.py tests/test_pipeline_builder.py tests/test_reporting.py tests/test_security_basic.py -v --tb=short
