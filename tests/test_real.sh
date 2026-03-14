#!/bin/bash
# Run tests with PySpark backend
export SPARKLESS_TEST_MODE=pyspark
echo "🔧 Running tests with PYSPARK backend"
python -m pytest "$@"
