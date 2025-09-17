#!/bin/bash

# Setup environment for running SparkForge tests
echo "🔧 Setting up test environment..."

# Set Java environment
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH

# Activate virtual environment
source venv38/bin/activate

# Verify Java is working
echo "✅ Java version:"
java -version

# Verify Python environment
echo "✅ Python version:"
python --version

echo "🎉 Environment setup complete!"
echo "You can now run tests with: python -m pytest tests/ -v"