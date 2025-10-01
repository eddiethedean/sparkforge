#!/bin/bash
# Run tests with mock Spark (default)
export SPARK_MODE=mock
echo "🔧 Running tests with MOCK Spark"
python -m pytest "$@"
