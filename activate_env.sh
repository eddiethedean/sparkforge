#!/bin/bash
# SparkForge Environment Activation Script
# Run this to activate the environment in new terminal sessions

# Set Java environment
if [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@11
elif [ -d "/usr/local/opt/openjdk@11" ]; then
    export JAVA_HOME=/usr/local/opt/openjdk@11
else
    java_home=$(/usr/libexec/java_home -v 11 2>/dev/null || echo "")
    if [ -n "$java_home" ]; then
        export JAVA_HOME="$java_home"
    fi
fi

export PATH=$JAVA_HOME/bin:$PATH

# Activate virtual environment
source venv38/bin/activate

echo "🚀 SparkForge environment activated!"
echo "Java: $(java -version 2>&1 | head -n 1)"
echo "Python: $(python --version)"
echo ""
echo "Available commands:"
echo "  python -m pytest tests/ -v          # Run all tests"
echo "  python -m pytest tests/unit/ -v     # Run unit tests only"
echo "  python -m pytest tests/integration/ -v  # Run integration tests only"
echo "  python -m pytest tests/system/ -v   # Run system tests only"
echo "  make test                           # Run tests with Makefile"
echo "  make test-cov                       # Run tests with coverage"
echo "  make quality                        # Run quality checks"
