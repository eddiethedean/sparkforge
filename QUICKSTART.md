# PipelineBuilder Quick Start Guide

## ✅ Environment Setup Complete!

Your Python 3.8 and PySpark 3.4 environment is ready to use.

## 🚀 Getting Started

### 1. Activate the Environment

Every time you open a new terminal, run:

```bash
cd /Users/odosmatthews/Documents/coding/pipeline_builder
source activate_env.sh
```

Or activate directly:

```bash
source venv38/bin/activate
```

### 2. Verify Everything Works

```bash
python test_environment.py
```

This will run comprehensive checks on all components.

### 3. Run Tests

```bash
# Quick test
python -m pytest tests/unit/test_constants_coverage.py -v

# All tests
python -m pytest tests/ -v

# With coverage
python -m pytest tests/ --cov=pipeline_builder --cov-report=html
```

## 📦 What's Installed

- ✅ **Python 3.8.18**
- ✅ **PySpark 3.4**
- ✅ **Delta Lake 1.2.1**
- ✅ **PipelineBuilder 0.8.0**
- ✅ **pytest, hypothesis, mock-spark** (testing)
- ✅ **black, mypy, isort, flake8, ruff** (dev tools)

## 💡 Common Commands

### Testing
```bash
python -m pytest tests/unit/ -v              # Unit tests
python -m pytest tests/integration/ -v       # Integration tests
python -m pytest tests/system/ -v            # System tests
python -m pytest -k "test_name" -v           # Specific test
```

### Code Quality
```bash
black pipeline_builder/                            # Format code
isort pipeline_builder/                            # Sort imports
mypy pipeline_builder/                             # Type checking
flake8 pipeline_builder/                           # Linting
ruff check pipeline_builder/                       # Fast linting
```

### Development
```bash
pip list                                     # Show installed packages
pip install -e ".[dev,test,docs]"           # Reinstall dependencies
python -c "import pipeline_builder; print(pipeline_builder.__version__)"  # Check version
```

## 🔍 Example Usage

```python
from pyspark.sql import SparkSession
from pipeline_builder.pipeline.builder import PipelineBuilder

# Create Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 35)
]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Use PipelineBuilder
pipeline = PipelineBuilder(spark)

# Build and execute your pipeline
# ... your code here ...

# Clean up
spark.stop()
```

## 📚 Documentation Locations

- **Main README**: `README.md`
- **Environment Details**: `ENVIRONMENT_INFO.md`
- **Documentation**: `docs/` directory
- **Examples**: `examples/` directory
- **Tests**: `tests/` directory

## 🔧 Troubleshooting

### Environment not activating?
```bash
# Recreate the environment
rm -rf venv38
python3.8 -m venv venv38
source venv38/bin/activate
pip install --upgrade pip
pip install -e ".[dev,test,docs]"
```

### Import errors?
```bash
# Ensure you're in the virtual environment
which python  # Should show: .../venv38/bin/python

# Reinstall in editable mode
pip install -e .
```

### Spark issues?
```bash
# Check Java is available
java -version

# Test Spark manually
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('Test').master('local[1]').getOrCreate(); print(spark.version); spark.stop()"
```

## 📝 Notes

- The warnings about "loopback address" and "native-hadoop library" are normal and can be ignored
- Java 8, 11, or later versions all work with PySpark 3.4
- The virtual environment must be activated in each new terminal session
- Tests use both real Spark and mock-spark for different test scenarios

## 🎯 Next Steps

1. ✅ Environment is set up
2. ✅ All tests pass
3. 🔄 Start developing your features
4. 🔄 Run tests frequently
5. 🔄 Use code quality tools before committing

---

**Need Help?**
- Check `ENVIRONMENT_INFO.md` for detailed information
- Run `python test_environment.py` to verify setup
- Review documentation in `docs/` directory

