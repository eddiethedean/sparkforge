#!/bin/bash

# SparkForge Environment Setup Script

echo "🚀 Setting up SparkForge development environment..."

# Check if Python 3.8 is available
if ! command -v python3.8 &> /dev/null; then
    echo "❌ Python 3.8 is required but not found"
    echo "Please install Python 3.8 and try again"
    exit 1
fi

# Check if Java 11 is available
if ! command -v java &> /dev/null; then
    echo "❌ Java is required but not found"
    echo "Please install Java 11 and try again"
    exit 1
fi

# Set Java environment
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28
export PATH=$JAVA_HOME/bin:$PATH

# Activate virtual environment
if [ ! -d "venv38" ]; then
    echo "📦 Creating Python 3.8 virtual environment..."
    python3.8 -m venv venv38
fi

echo "🔧 Activating virtual environment..."
source venv38/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt
pip install pytest pytest-cov pytest-xdist mypy black isort ruff bandit

# Verify installation
echo "✅ Verifying installation..."
python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
python -c "import delta; print('Delta Lake available')"
python -c "import pytest; print('Pytest version:', pytest.__version__)"
python -c "import mypy; print('Mypy version:', mypy.__version__)"

echo "🎉 Environment setup complete!"
echo "To activate the environment in the future, run:"
echo "  source setup_env.sh"
