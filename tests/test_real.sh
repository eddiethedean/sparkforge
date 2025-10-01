#!/bin/bash
# Run tests with real Spark
export SPARK_MODE=real
echo "🔧 Running tests with REAL Spark"
python -m pytest "$@"
