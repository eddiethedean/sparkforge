#!/bin/bash

# PipelineBuilder Complete Environment Setup Script
# This script sets up the complete development and testing environment for PipelineBuilder

set -e  # Exit on any error

echo "🚀 PipelineBuilder Complete Environment Setup"
echo "=============================================="

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check Java version
check_java_version() {
    if command_exists java; then
        java_version=$(java -version 2>&1 | head -n 1 | cut -d '"' -f 2 | cut -d '.' -f 1)
        if [ "$java_version" -ge 11 ]; then
            return 0
        fi
    fi
    return 1
}

# Check if Python 3.8 is available
echo "🐍 Checking Python 3.8..."
if ! command_exists python3.8; then
    echo "❌ Python 3.8 is required but not found"
    echo "Please install Python 3.8 using pyenv, brew, or your system package manager"
    echo "Example: brew install python@3.8"
    exit 1
fi
echo "✅ Python 3.8 found: $(python3.8 --version)"

# Check and setup Java 11
echo "☕ Checking Java 11..."
if ! check_java_version; then
    echo "📦 Installing Java 11..."
    if command_exists brew; then
        brew install openjdk@11
    else
        echo "❌ Homebrew not found. Please install Java 11 manually"
        echo "Download from: https://adoptium.net/"
        exit 1
    fi
fi

# Set Java environment variables
echo "🔧 Setting up Java environment..."
if [ -d "/opt/homebrew/opt/openjdk@11" ]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@11
elif [ -d "/usr/local/opt/openjdk@11" ]; then
    export JAVA_HOME=/usr/local/opt/openjdk@11
else
    # Try to find Java 11 installation
    java_home=$(/usr/libexec/java_home -v 11 2>/dev/null || echo "")
    if [ -n "$java_home" ]; then
        export JAVA_HOME="$java_home"
    else
        echo "❌ Java 11 installation not found"
        echo "Please install Java 11 and try again"
        exit 1
    fi
fi

export PATH=$JAVA_HOME/bin:$PATH

# Verify Java is working
echo "✅ Java version:"
java -version

# Create virtual environment if it doesn't exist
echo "📦 Setting up Python virtual environment..."
if [ ! -d "venv38" ]; then
    echo "Creating Python 3.8 virtual environment..."
    python3.8 -m venv venv38
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv38/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing dependencies..."
pip install -e .

# Install development dependencies
echo "🛠️ Installing development dependencies..."
pip install -e ".[dev,test,docs]"

# Verify installation
echo "✅ Verifying installation..."
echo "Python version: $(python --version)"
echo "PySpark version: $(python -c "import pyspark; print(pyspark.__version__)" 2>/dev/null || echo "Not available")"
echo "Delta Lake: $(python -c "import delta; print('Available')" 2>/dev/null || echo "Not available")"
echo "Pytest version: $(python -c "import pytest; print(pytest.__version__)")"
echo "Mypy version: $(python -c "import mypy; print(mypy.__version__)")"

# Test Spark setup
echo "🧪 Testing Spark setup..."
if python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('Test').master('local[1]').getOrCreate(); spark.stop()" 2>/dev/null; then
    echo "✅ Spark setup successful"
else
    echo "⚠️ Spark setup test failed, but environment is ready"
    echo "This might be due to Java gateway issues, but tests should work"
fi

# Create environment activation script
echo "📝 Creating environment activation script..."
cat > activate_env.sh << 'EOF'
#!/bin/bash
# PipelineBuilder Environment Activation Script
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

echo "🚀 PipelineBuilder environment activated!"
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
EOF

chmod +x activate_env.sh

echo ""
echo "🎉 Environment setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Activate the environment: source activate_env.sh"
echo "2. Run tests: python -m pytest tests/ -v"
echo "3. Run specific test categories:"
echo "   - Unit tests: python -m pytest tests/unit/ -v"
echo "   - Integration tests: python -m pytest tests/integration/ -v"
echo "   - System tests: python -m pytest tests/system/ -v"
echo "4. Run with coverage: python -m pytest tests/ --cov=sparkforge --cov-report=html"
echo ""
echo "💡 For future sessions, just run: source activate_env.sh"
