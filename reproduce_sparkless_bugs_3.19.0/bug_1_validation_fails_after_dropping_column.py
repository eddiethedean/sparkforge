"""
Bug 1: Validation fails (0.0% valid) after transform that drops a column

This reproduces the exact bug from the marketing pipeline test where
processed_impressions validation fails with 0.0% valid after a transform
that uses impression_date and then drops it.

Version: sparkless 3.19.0
Issue: Related to Issue #163
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_validation_fails").getOrCreate()

print("=" * 80)
print("BUG REPRODUCTION: Validation fails (0.0% valid) after dropping column")
print("=" * 80)
print("This reproduces the exact bug from test_marketing_pipeline.py")
print()

# Create test data (150 rows - same as test)
print("STEP 1: Create test data (150 rows)")
print("-" * 80)
data = []
for i in range(150):
    data.append({
        "impression_id": f"IMP-{i:08d}",
        "campaign_id": f"CAMP-{i % 10:02d}",
        "customer_id": f"CUST-{i % 40:04d}",
        "impression_date": (datetime.now() - timedelta(hours=i % 720)).isoformat(),
        "channel": ["google", "facebook", "twitter", "email", "display"][i % 5],
        "ad_id": f"AD-{i % 20:03d}",
        "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
        "device_type": ["desktop", "mobile", "tablet"][i % 3],
    })

bronze_df = spark.createDataFrame(data, [
    "impression_id",
    "campaign_id",
    "customer_id",
    "impression_date",
    "channel",
    "ad_id",
    "cost_per_impression",
    "device_type",
])

print(f"✅ Created DataFrame with {bronze_df.count()} rows")
print(f"   Columns: {bronze_df.columns}")

# Apply EXACT transform from test
print("\nSTEP 2: Apply transform (EXACT from test_marketing_pipeline.py)")
print("-" * 80)
print("Transform:")
print("  1. Uses 'impression_date' to create 'impression_date_parsed'")
print("  2. Creates 'hour_of_day' and 'day_of_week' from parsed date")
print("  3. Drops 'impression_date' via .select()")
print()

silver_df = (
    bronze_df.withColumn(
        "impression_date_parsed",
        F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ),
    )
    .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
    .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
    .withColumn("is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False))
    .select(
        "impression_id",
        "campaign_id",
        "customer_id",
        "impression_date_parsed",  # impression_date is DROPPED here
        "hour_of_day",
        "day_of_week",
        "channel",
        "ad_id",
        "cost_per_impression",
        "device_type",
        "is_mobile",
    )
)

print(f"✅ Transform completed")
print(f"   Columns after transform: {silver_df.columns}")
print(f"   'impression_date' in columns: {'impression_date' in silver_df.columns}")

# Check if to_timestamp worked
print("\nSTEP 3: Check to_timestamp() results")
print("-" * 80)
try:
    sample = silver_df.select("impression_id", "impression_date_parsed", "hour_of_day").limit(5).collect()
    none_count = sum(1 for row in sample if row[1] is None)
    print(f"Sample rows:")
    for i, row in enumerate(sample):
        print(f"  Row {i}: impression_date_parsed={row[1]}, hour_of_day={row[2]}")
    
    if none_count > 0:
        print(f"\n⚠️  WARNING: {none_count}/5 sample rows have None for impression_date_parsed")
        print("   This might be causing validation to fail!")
    else:
        print("\n✅ All sample rows have valid parsed dates")
except Exception as e:
    print(f"❌ Error checking results: {e}")

# Apply EXACT validation rules from test
print("\nSTEP 4: Apply validation rules (EXACT from test)")
print("-" * 80)
print("Validation rules:")
validation_rules = {
    "impression_id": ["not_null"],
    "impression_date_parsed": ["not_null"],
    "campaign_id": ["not_null"],
    "customer_id": ["not_null"],
    "channel": ["not_null"],
    "cost_per_impression": [["gte", 0]],  # This might trigger type comparison bug
    "hour_of_day": ["not_null"],
    "device_type": ["not_null"],
}

for col, rules in validation_rules.items():
    print(f"  {col}: {rules}")

# Build validation predicate (like apply_column_rules does)
print("\nSTEP 5: Build validation predicate and test")
print("-" * 80)

validation_predicate = None
for col_name, rules in validation_rules.items():
    for rule in rules:
        try:
            if rule == "not_null":
                rule_expr = F.col(col_name).isNotNull()
            elif isinstance(rule, list) and len(rule) == 2:
                op, value = rule
                if op == "gte":
                    rule_expr = F.col(col_name) >= value
                else:
                    continue
            else:
                continue
            
            if validation_predicate is None:
                validation_predicate = rule_expr
            else:
                validation_predicate = validation_predicate & rule_expr
        except Exception as e:
            print(f"❌ Error building rule for {col_name}: {e}")
            import traceback
            traceback.print_exc()

if validation_predicate is not None:
    print("✅ Validation predicate created")
    print("Attempting to filter with validation predicate...")
    
    try:
        valid_df = silver_df.filter(validation_predicate)
        invalid_df = silver_df.filter(~validation_predicate)
        
        total = silver_df.count()
        valid = valid_df.count()
        invalid = invalid_df.count()
        validation_rate = (valid / total * 100) if total > 0 else 0
        
        print(f"\n✅ Validation succeeded!")
        print(f"   Total rows: {total}")
        print(f"   Valid rows: {valid}")
        print(f"   Invalid rows: {invalid}")
        print(f"   Validation rate: {validation_rate:.1f}%")
        
        if validation_rate == 0.0:
            print("\n" + "=" * 80)
            print("BUG CONFIRMED: 0.0% valid (all rows fail validation)")
            print("=" * 80)
            print("This matches the error in test_marketing_pipeline.py:")
            print("  'Validation completed for pipeline.processed_impressions: 0.0% valid'")
            print("\nPossible causes:")
            print("  1. to_timestamp() returns None for all rows")
            print("  2. Column resolution issue during validation")
            print("  3. Type comparison error (cost_per_impression >= 0)")
            
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ BUG REPRODUCED: {error_msg[:400]}")
        
        if "cannot resolve 'impression_date'" in error_msg:
            print("\n" + "=" * 80)
            print("BUG CONFIRMED: cannot resolve 'impression_date'")
            print("=" * 80)
            print("sparkless tries to resolve 'impression_date' during validation")
            print("even though it was dropped in the transform.")
        elif "cannot compare string with numeric" in error_msg.lower():
            print("\n" + "=" * 80)
            print("BUG CONFIRMED: Type comparison error")
            print("=" * 80)
            print("sparkless treats cost_per_impression as string instead of numeric")
        else:
            print(f"\nUnexpected error: {e}")
            import traceback
            traceback.print_exc()

spark.stop()

