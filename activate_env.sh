#!/bin/bash
# PipelineBuilder Environment Activation Script
# Run this to activate the environment in new terminal sessions

# Set Java environment (Java 11 is required for PySpark 3.5)
if [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@11
    export PATH=$JAVA_HOME/bin:$PATH
elif [ -d "/usr/local/opt/openjdk@11" ]; then
    export JAVA_HOME=/usr/local/opt/openjdk@11
    export PATH=$JAVA_HOME/bin:$PATH
else
    # Try to find any Java installation
    java_home=$(/usr/libexec/java_home 2>/dev/null || echo "")
    if [ -n "$java_home" ]; then
        export JAVA_HOME="$java_home"
        export PATH=$JAVA_HOME/bin:$PATH
    fi
fi

# Activate virtual environment
source venv38/bin/activate

echo "🚀 PipelineBuilder environment activated!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Environment Details:"
echo "  Java: $(java -version 2>&1 | head -n 1)"
echo "  Python: $(python --version)"
echo "  PySpark: $(python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'Not found')"
echo "  PipelineBuilder: $(python -c 'import pipeline_builder; print(pipeline_builder.__version__)' 2>/dev/null || echo 'Not found')"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Quick Commands:"
echo "  python test_environment.py              # Verify environment"
echo "  python -m pytest tests/ -v              # Run all tests"
echo "  python -m pytest tests/unit/ -v         # Run unit tests only"
echo "  python -m pytest tests/integration/ -v  # Run integration tests"
echo "  python -m pytest tests/system/ -v       # Run system tests"
echo "  python -m pytest --cov=src/pipeline_builder       # Run with coverage"
echo ""
echo "Code Quality:"
echo "  black src/                              # Format code"
echo "  mypy src/                                # Type checking"
echo "  flake8 src/                              # Linting"
echo "  ruff check src/                          # Fast linting"
echo ""
