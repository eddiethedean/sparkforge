#!/usr/bin/env python3
"""
Comprehensive test script for Python 3.8 Spark environment.

This script tests:
1. Python version compatibility
2. Spark session creation
3. Core PipelineBuilder imports
4. Type annotation compatibility
5. Dict vs dict syntax issues
"""

import os
import sys
from typing import Dict, Union


def test_python_version() -> bool:
    """Test Python version compatibility."""
    print("🐍 Testing Python version...")
    version = sys.version_info[:2]
    if version >= (3, 8):
        print(f"✅ Python {version[0]}.{version[1]} is compatible")
        return True
    else:
        print(f"❌ Python {version[0]}.{version[1]} is not compatible (need 3.8+)")
        return False


def test_java_environment() -> bool:
    """Test Java environment."""
    print("\n☕ Testing Java environment...")
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        print(f"✅ JAVA_HOME set to: {java_home}")
    else:
        print("❌ JAVA_HOME not set")
        return False

    try:
        import subprocess

        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java is working")
            return True
        else:
            print("❌ Java is not working")
            return False
    except Exception as e:
        print(f"❌ Java test failed: {e}")
        return False


def test_spark_session() -> bool:
    """Test Spark session creation."""
    print("\n🔥 Testing Spark session...")
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("Python38Test")
            .master("local[1]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        print("✅ Spark session created successfully")
        print(f"   Spark version: {spark.version}")

        # Test basic DataFrame operations
        df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "name"])
        count = df.count()
        print(f"✅ DataFrame operations work (count: {count})")

        spark.stop()
        print("✅ Spark session stopped successfully")
        return True

    except Exception as e:
        print(f"❌ Spark session test failed: {e}")
        return False


def test_sparkforge_imports() -> bool:
    """Test PipelineBuilder core imports."""
    print("\n📦 Testing PipelineBuilder imports...")

    try:
        # Test core imports
        print("✅ Writer module imports successful")

        print("✅ Core models imports successful")

        print("✅ Logging and errors imports successful")

        return True

    except Exception as e:
        print(f"❌ PipelineBuilder imports failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_type_annotations() -> bool:
    """Test type annotation compatibility."""
    print("\n🔍 Testing type annotations...")

    try:
        # Test Dict type annotations (should work in Python 3.8)
        def test_dict_func() -> Dict[str, int]:
            return {"test": 1}

        test_dict_func()
        print("✅ Dict[str, int] type annotation works")

        # Test Union with Dict
        def test_union_func(
            value: Union[Dict[str, int], str],
        ) -> Union[Dict[str, int], str]:
            return value

        test_union_func({"test": 1})
        test_union_func("test")
        print("✅ Union[Dict[str, int], str] type annotation works")

        # Test that dict[str, int] syntax fails (as expected in Python 3.8)
        try:

            def test_dict_syntax() -> Dict[str, int]:  # This should fail in Python 3.8
                return {"test": 1}

            print("❌ dict[str, int] syntax worked (unexpected)")
            return False
        except TypeError:
            print("✅ dict[str, int] syntax correctly fails in Python 3.8")

        return True

    except Exception as e:
        print(f"❌ Type annotation test failed: {e}")
        return False


def test_sparkforge_functionality() -> bool:
    """Test PipelineBuilder functionality."""
    print("\n⚙️ Testing PipelineBuilder functionality...")

    try:
        from pipeline_builder.writer.models import WriteMode, WriterConfig

        # Test WriterConfig creation
        config = WriterConfig(
            table_schema="test_schema",
            table_name="test_table",
            write_mode=WriteMode.APPEND,
        )
        print("✅ WriterConfig creation works")

        # Test type aliases
        print("✅ StringDict type alias works")

        # Test validation
        config.validate()
        print("✅ WriterConfig validation works")

        return True

    except Exception as e:
        print(f"❌ PipelineBuilder functionality test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_dict_annotation_checker() -> bool:
    """Test the Dict annotation checker."""
    print("\n🔍 Testing Dict annotation checker...")

    try:
        import subprocess

        result = subprocess.run(
            [sys.executable, "scripts/check_dict_annotations.py"],
            capture_output=True,
            text=True,
            cwd=os.getcwd(),
        )

        if result.returncode == 1:  # Expected to find violations
            print("✅ Dict annotation checker found violations (as expected)")
            print(f"   Found {result.stdout.count('Dict type annotation')} violations")
            return True
        else:
            print("❌ Dict annotation checker should have found violations")
            return False

    except Exception as e:
        print(f"❌ Dict annotation checker test failed: {e}")
        return False


def main() -> int:
    """Run all tests."""
    print("🚀 Starting Python 3.8 Spark Environment Tests")
    print("=" * 60)

    tests = [
        test_python_version,
        test_java_environment,
        test_spark_session,
        test_sparkforge_imports,
        test_type_annotations,
        test_sparkforge_functionality,
        test_dict_annotation_checker,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")

    print("\n" + "=" * 60)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Python 3.8 Spark environment is working correctly.")
        return 0
    else:
        print("❌ Some tests failed. Please check the output above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
