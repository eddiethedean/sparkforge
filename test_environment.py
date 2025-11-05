#!/usr/bin/env python
"""
Environment Verification Script for SparkForge

This script verifies that the Python 3.8 and PySpark 3.2 environment
is correctly set up and all components are working.
"""

import sys


def test_python_version():
    """Verify Python version is 3.8.x"""
    print("=" * 60)
    print("Testing Python Version")
    print("=" * 60)
    
    version = sys.version_info
    print(f"✓ Python {version.major}.{version.minor}.{version.micro}")
    
    if version.major == 3 and version.minor == 8:
        print("✓ Python 3.8 confirmed")
        return True
    else:
        print(f"✗ Expected Python 3.8, got {version.major}.{version.minor}")
        return False


def test_pyspark():
    """Verify PySpark installation and version"""
    print("\n" + "=" * 60)
    print("Testing PySpark")
    print("=" * 60)
    
    try:
        import pyspark
        print(f"✓ PySpark {pyspark.__version__} imported")
        
        if pyspark.__version__.startswith("3.2"):
            print("✓ PySpark 3.2.x confirmed")
            return True
        else:
            print(f"✗ Expected PySpark 3.2.x, got {pyspark.__version__}")
            return False
    except ImportError as e:
        print(f"✗ Failed to import PySpark: {e}")
        return False


def test_spark_session():
    """Verify Spark session can be created and used"""
    print("\n" + "=" * 60)
    print("Testing Spark Session")
    print("=" * 60)
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("EnvironmentTest") \
            .master("local[1]") \
            .config("spark.ui.enabled", "false") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
        
        print(f"✓ Spark session created (version {spark.version})")
        
        # Create a test DataFrame
        data = [(1, "test1"), (2, "test2"), (3, "test3")]
        df = spark.createDataFrame(data, ["id", "value"])
        count = df.count()
        
        print(f"✓ Created DataFrame with {count} rows")
        
        # Test basic operations
        filtered = df.filter(df.id > 1)
        filtered_count = filtered.count()
        print(f"✓ Filtered DataFrame: {filtered_count} rows")
        
        spark.stop()
        print("✓ Spark session stopped successfully")
        
        return True
    except Exception as e:
        print(f"✗ Spark session test failed: {e}")
        return False


def test_delta_lake():
    """Verify Delta Lake is available"""
    print("\n" + "=" * 60)
    print("Testing Delta Lake")
    print("=" * 60)
    
    try:
        import delta
        print("✓ Delta Lake imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Failed to import Delta Lake: {e}")
        return False


def test_sparkforge():
    """Verify SparkForge package is installed"""
    print("\n" + "=" * 60)
    print("Testing SparkForge")
    print("=" * 60)
    
    try:
        import sparkforge
        print(f"✓ SparkForge {sparkforge.__version__} imported")
        
        from sparkforge.pipeline.builder import PipelineBuilder
        print("✓ PipelineBuilder imported")
        
        from sparkforge import execution
        print("✓ Execution module imported")
        
        from sparkforge import validation
        print("✓ Validation module imported")
        
        return True
    except Exception as e:
        print(f"✗ SparkForge test failed: {e}")
        return False


def test_testing_tools():
    """Verify testing tools are available"""
    print("\n" + "=" * 60)
    print("Testing Tools")
    print("=" * 60)
    
    success = True
    
    try:
        import pytest
        print(f"✓ pytest {pytest.__version__}")
    except ImportError:
        print("✗ pytest not available")
        success = False
    
    try:
        import hypothesis
        print(f"✓ hypothesis {hypothesis.__version__}")
    except ImportError:
        print("✗ hypothesis not available")
        success = False
    
    try:
        import mock_spark
        print("✓ mock-spark available")
    except ImportError:
        print("✗ mock-spark not available")
        success = False
    
    return success


def test_dev_tools():
    """Verify development tools are available"""
    print("\n" + "=" * 60)
    print("Development Tools")
    print("=" * 60)
    
    tools = [
        ("black", "black"),
        ("mypy", "mypy"),
        ("isort", "isort"),
        ("flake8", "flake8"),
        ("ruff", "ruff"),
    ]
    
    success = True
    for module_name, display_name in tools:
        try:
            module = __import__(module_name)
            version = getattr(module, "__version__", "unknown")
            print(f"✓ {display_name} {version}")
        except ImportError:
            print(f"✗ {display_name} not available")
            success = False
    
    return success


def main():
    """Run all environment tests"""
    print("\n")
    print("=" * 60)
    print("SparkForge Environment Verification")
    print("=" * 60)
    print()
    
    results = {
        "Python Version": test_python_version(),
        "PySpark": test_pyspark(),
        "Spark Session": test_spark_session(),
        "Delta Lake": test_delta_lake(),
        "SparkForge": test_sparkforge(),
        "Testing Tools": test_testing_tools(),
        "Dev Tools": test_dev_tools(),
    }
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name:.<40} {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    if all_passed:
        print("🎉 All tests passed! Environment is ready.")
    else:
        print("⚠️  Some tests failed. Please check the output above.")
    print("=" * 60)
    print()
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

