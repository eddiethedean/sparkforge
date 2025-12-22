"""
Bug 3: Type comparison error - "cannot compare string with numeric type (i32)"

This reproduces the bug where comparing a numeric column with a number fails due to type mismatch.
"""

from sparkless import SparkSession, functions as F

spark = SparkSession.builder.appName("bug_type_comparison").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: Type comparison error (string vs numeric)")
print("=" * 80)

# Create test data with numeric column
data = []
for i in range(10):
    data.append({
        "id": f"ID-{i:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
    })

df = spark.createDataFrame(data, ["id", "cost_per_impression"])

print("\nSTEP 1: Check column type")
print("=" * 80)
print(f"Columns: {df.columns}")
print(f"Schema: {df.schema}")

print("\nSTEP 2: Test comparison operation (gte)")
print("=" * 80)

try:
    # This is what validation does: F.col("cost_per_impression") >= 0
    result_df = df.filter(F.col("cost_per_impression") >= 0)
    count = result_df.count()
    print(f"✅ Comparison succeeded: {count} rows")
except Exception as e:
    error_msg = str(e)
    print(f"❌ BUG REPRODUCED: {error_msg[:300]}")
    
    if "cannot compare string with numeric" in error_msg.lower():
        print("\n" + "=" * 80)
        print("BUG CONFIRMED:")
        print("  - Column 'cost_per_impression' should be numeric")
        print("  - Comparison: cost_per_impression >= 0")
        print("  - Error: 'cannot compare string with numeric type (i32)'")
        print("  - sparkless treats numeric column as string")
        print("=" * 80)
    else:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

print("\nSTEP 3: Test with explicit cast")
print("=" * 80)

try:
    # Try with explicit cast
    result_df = df.filter(F.col("cost_per_impression").cast("double") >= 0)
    count = result_df.count()
    print(f"✅ Comparison with cast succeeded: {count} rows")
except Exception as e:
    print(f"❌ Comparison with cast also failed: {e}")

spark.stop()

