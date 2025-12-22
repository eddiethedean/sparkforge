"""
Bug 1: Validation fails with "cannot resolve" when validating after transform that drops columns

This reproduces the bug where sparkless tries to resolve a dropped column during validation.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("bug_validation_cannot_resolve").getOrCreate()

# Create test data (150 rows - bug only manifests with larger datasets)
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

print("=" * 80)
print("BUG REPRODUCTION: Validation fails with 'cannot resolve' dropped column")
print("=" * 80)

# Transform that uses impression_date then drops it
print("\nSTEP 1: Apply transform (uses 'impression_date', then drops it)")
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
        "impression_date_parsed",  # New column, original impression_date is DROPPED
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
print(f"   Columns: {silver_df.columns}")
print(f"   'impression_date' in columns: {'impression_date' in silver_df.columns}")

# Test validation (this triggers the bug)
print("\nSTEP 2: Apply validation rules (THIS TRIGGERS THE BUG)")
print("=" * 80)

validation_rules = {
    "impression_id": ["not_null"],
    "impression_date_parsed": ["not_null"],
    "campaign_id": ["not_null"],
    "customer_id": ["not_null"],
    "channel": ["not_null"],
    # Skip cost_per_impression to avoid type comparison bug
    "hour_of_day": ["not_null"],
    "device_type": ["not_null"],
}

# Build validation predicate (like apply_column_rules does)
validation_predicate = None
for col_name, rules in validation_rules.items():
    for rule in rules:
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

print("Validation predicate created")
print("Attempting to filter with validation predicate...")

try:
    valid_df = silver_df.filter(validation_predicate)
    invalid_df = silver_df.filter(~validation_predicate)
    
    total = silver_df.count()
    valid = valid_df.count()
    invalid = invalid_df.count()
    
    print(f"✅ Validation succeeded: {valid}/{total} valid, {invalid} invalid")
except Exception as e:
    error_msg = str(e)
    print(f"❌ BUG REPRODUCED: {error_msg[:300]}")
    
    if "cannot resolve 'impression_date'" in error_msg:
        print("\n" + "=" * 80)
        print("BUG CONFIRMED:")
        print("  - Transform uses 'impression_date' then drops it")
        print("  - Validation tries to validate columns that exist")
        print("  - sparkless tries to resolve 'impression_date' (dropped column)")
        print("  - Error: 'cannot resolve impression_date'")
        print("=" * 80)
    else:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()

spark.stop()

