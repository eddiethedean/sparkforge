# SparkForge Python 3.8 & PySpark 3.2 Environment

## ✅ Environment Setup Complete

### Installed Versions
- **Python**: 3.8.18
- **PySpark**: 3.2.4
- **Delta Lake**: 1.2.1
- **Java**: 1.8.0_461
- **SparkForge**: 0.8.0

### Virtual Environment Location
```bash
/Users/odosmatthews/Documents/coding/sparkforge/venv38/
```

## Quick Start

### Activate the Environment
```bash
source venv38/bin/activate
```

Or use the activation script:
```bash
source activate_env.sh
```

### Verify Installation
```bash
# Check Python version
python --version

# Check PySpark
python -c "import pyspark; print(f'PySpark: {pyspark.__version__}')"

# Check SparkForge
python -c "import sparkforge; print(f'SparkForge: {sparkforge.__version__}')"
```

### Run Tests
```bash
# Run all tests
python -m pytest tests/ -v

# Run unit tests only
python -m pytest tests/unit/ -v

# Run with coverage
python -m pytest tests/ --cov=sparkforge --cov-report=html

# Run specific test file
python -m pytest tests/unit/test_constants_coverage.py -v
```

### Development Commands
```bash
# Format code
black sparkforge/

# Sort imports
isort sparkforge/

# Type checking
mypy sparkforge/

# Linting
flake8 sparkforge/
ruff check sparkforge/

# Security checks
bandit -r sparkforge/
```

## Environment Details

### Key Dependencies
- **Core**: pyspark==3.2.4, delta-spark>=1.2.0, pandas>=1.3.0, psutil>=5.8.0
- **Testing**: pytest>=7.0.0, pytest-cov>=4.0.0, hypothesis>=6.0.0, mock-spark==1.0.0
- **Development**: black>=23.0.0, mypy>=1.3.0, isort>=5.12.0, ruff>=0.0.270
- **Documentation**: sphinx>=4.0.0, sphinx-rtd-theme>=1.0.0, myst-parser>=0.17.0

### Test Environment Status
✅ Python 3.8.18 installed and working
✅ PySpark 3.2.4 installed and working
✅ Delta Lake available
✅ Mock Spark available for unit testing
✅ Spark session creation successful
✅ Basic DataFrame operations working
✅ Test suite running successfully

### Java Configuration
- Current Java version: 1.8.0_461 (Java 8)
- Status: Working correctly with PySpark 3.2.4
- Note: Java 11 is recommended but Java 8 is fully compatible

## Troubleshooting

### If you see Java warnings
The warnings about loopback addresses and native Hadoop libraries are normal and can be ignored.

### Reactivating environment in new terminal
```bash
cd /Users/odosmatthews/Documents/coding/sparkforge
source venv38/bin/activate
```

### Reinstalling dependencies
```bash
source venv38/bin/activate
pip install -e ".[dev,test,docs]"
```

### Recreating the environment
```bash
# Remove old environment
rm -rf venv38

# Create new environment
python3.8 -m venv venv38
source venv38/bin/activate
pip install --upgrade pip
pip install -e ".[dev,test,docs]"
```

## Next Steps

1. **Run the full test suite**: `python -m pytest tests/ -v`
2. **Check code quality**: `make quality` (if Makefile is configured)
3. **Build documentation**: `cd docs && make html`
4. **Start developing**: Import SparkForge modules and begin coding!

## Example Usage

```python
# Import SparkForge
from sparkforge.pipeline.builder import PipelineBuilder
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()

# Use SparkForge
pipeline = PipelineBuilder(spark)
# ... build your pipeline ...

# Clean up
spark.stop()
```

---

**Environment created on**: October 8, 2025
**Last verified**: October 8, 2025

