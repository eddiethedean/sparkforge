#!/usr/bin/env python3
"""
Validation script for sparkless bugs.

This script validates that the reported bugs are sparkless-specific by
testing equivalent code in both sparkless and PySpark.
"""

import sys
from typing import Tuple


def test_combined_and_expression() -> Tuple[bool, str]:
    """Test combined expressions with & (AND) operator."""
    print("\n" + "=" * 80)
    print("TEST: Combined ColumnOperation expressions with & (AND) operator")
    print("=" * 80)

    # Test with sparkless
    print("\n--- Testing with sparkless 3.17.0 ---")
    try:
        from sparkless import SparkSession, functions as F  # noqa: I001

        spark = SparkSession("test")
        df = spark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": None, "b": 3}, {"a": 5, "b": 0}], "a int, b int"
        )

        expr1 = F.col("a").isNotNull()
        expr2 = F.col("b") > 0
        combined = expr1 & expr2

        print(f"Expr1 type: {type(expr1)}")
        print(f"Expr2 type: {type(expr2)}")
        print(f"Combined type: {type(combined)}")
        print(f"Combined str: {str(combined)}")

        result = df.filter(combined)
        count = result.count()
        print(f"✅ sparkless filter() succeeded: {count} rows")
        sparkless_works = True
        sparkless_error = None
    except Exception as e:
        print(f"❌ sparkless filter() FAILED: {e}")
        sparkless_works = False
        sparkless_error = str(e)

    # Test with PySpark
    print("\n--- Testing with PySpark (for comparison) ---")
    try:
        import os

        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        from pyspark.sql import SparkSession as PySparkSession, functions as PyF  # noqa: I001

        pyspark = (
            PySparkSession.builder.appName("test").master("local[1]").getOrCreate()
        )
        pdf = pyspark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": None, "b": 3}, {"a": 5, "b": 0}], "a int, b int"
        )

        pexpr1 = PyF.col("a").isNotNull()
        pexpr2 = PyF.col("b") > 0
        pcombined = pexpr1 & pexpr2

        print(f"PySpark Expr1 type: {type(pexpr1)}")
        print(f"PySpark Expr2 type: {type(pexpr2)}")
        print(f"PySpark Combined type: {type(pcombined)}")
        print(f"PySpark Combined str: {str(pcombined)}")

        presult = pdf.filter(pcombined)
        pcount = presult.count()
        print(f"✅ PySpark filter() succeeded: {pcount} rows")

        pyspark.stop()
        pyspark_works = True
    except Exception as e:
        print(f"⚠️  PySpark test skipped: {e}")
        pyspark_works = None

    # Determine if this is a bug
    is_bug = not sparkless_works and pyspark_works
    status = "❌ CONFIRMED BUG" if is_bug else "✅ NOT A BUG"
    print(f"\n{status}")

    return is_bug, sparkless_error


def test_combined_or_expression() -> Tuple[bool, str]:
    """Test combined expressions with | (OR) operator."""
    print("\n" + "=" * 80)
    print("TEST: Combined ColumnOperation expressions with | (OR) operator")
    print("=" * 80)

    # Test with sparkless
    print("\n--- Testing with sparkless 3.17.0 ---")
    try:
        from sparkless import SparkSession, functions as F  # noqa: I001

        spark = SparkSession("test")
        df = spark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": None, "b": 3}, {"a": 5, "b": 0}], "a int, b int"
        )

        expr1 = F.col("a").isNotNull()
        expr2 = F.col("b") > 0
        combined = expr1 | expr2

        print(f"Combined type: {type(combined)}")
        print(f"Combined str: {str(combined)}")

        result = df.filter(combined)
        count = result.count()
        print(f"✅ sparkless filter() succeeded: {count} rows")
        sparkless_works = True
        sparkless_error = None
    except Exception as e:
        print(f"❌ sparkless filter() FAILED: {e}")
        sparkless_works = False
        sparkless_error = str(e)

    # Test with PySpark
    print("\n--- Testing with PySpark (for comparison) ---")
    try:
        import os

        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        from pyspark.sql import SparkSession as PySparkSession, functions as PyF  # noqa: I001

        pyspark = (
            PySparkSession.builder.appName("test").master("local[1]").getOrCreate()
        )
        pdf = pyspark.createDataFrame(
            [{"a": 1, "b": 2}, {"a": None, "b": 3}, {"a": 5, "b": 0}], "a int, b int"
        )

        pexpr1 = PyF.col("a").isNotNull()
        pexpr2 = PyF.col("b") > 0
        pcombined = pexpr1 | pexpr2

        presult = pdf.filter(pcombined)
        pcount = presult.count()
        print(f"✅ PySpark filter() succeeded: {pcount} rows")

        pyspark.stop()
        pyspark_works = True
    except Exception as e:
        print(f"⚠️  PySpark test skipped: {e}")
        pyspark_works = None

    # Determine if this is a bug
    is_bug = not sparkless_works and pyspark_works
    status = "❌ CONFIRMED BUG" if is_bug else "✅ NOT A BUG"
    print(f"\n{status}")

    return is_bug, sparkless_error


def main():
    """Run all bug validation tests."""
    print("=" * 80)
    print("SPARKLESS BUG VALIDATION SCRIPT")
    print("=" * 80)
    print("\nThis script validates that reported bugs are sparkless-specific")
    print("by testing equivalent code in both sparkless and PySpark.\n")

    results = []

    # Test 1: Combined AND expressions
    is_bug, error = test_combined_and_expression()
    results.append(("Combined AND expressions", is_bug, error))

    # Test 2: Combined OR expressions
    is_bug, error = test_combined_or_expression()
    results.append(("Combined OR expressions", is_bug, error))

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    for name, is_bug, error in results:
        status = "❌ CONFIRMED BUG" if is_bug else "✅ NOT A BUG"
        print(f"{name}: {status}")
        if error:
            print(f"  Error: {error[:100]}...")

    # Exit code
    bugs_found = sum(1 for _, is_bug, _ in results if is_bug)
    if bugs_found > 0:
        print(f"\n⚠️  Found {bugs_found} confirmed bug(s)")
        sys.exit(1)
    else:
        print("\n✅ No bugs found")
        sys.exit(0)


if __name__ == "__main__":
    main()
