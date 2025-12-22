"""
Investigate Validation Failure in sparkless 3.18.7

This script investigates why validation fails with 0.0% valid
for processed_impressions in the marketing pipeline test.
"""

from sparkless import SparkSession, functions as F
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("investigate_validation").getOrCreate()

# Create test data (150 rows like the test)
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

print("STEP 1: Apply transform (EXACT from test)")
print("=" * 80)

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
        "impression_date_parsed",
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

# Test validation rules (EXACT from test)
print("\nSTEP 2: Test validation rules (EXACT from test)")
print("=" * 80)

validation_rules = {
    "impression_id": ["not_null"],
    "impression_date_parsed": ["not_null"],
    "campaign_id": ["not_null"],
    "customer_id": ["not_null"],
    "channel": ["not_null"],
    "cost_per_impression": [["gte", 0]],
    "hour_of_day": ["not_null"],  # This might be the issue
    "device_type": ["not_null"],
}

print("Validation rules:")
for col, rules in validation_rules.items():
    print(f"  {col}: {rules}")

# Test each rule individually
print("\nSTEP 3: Test each validation rule individually")
print("=" * 80)

for col_name, rules in validation_rules.items():
    print(f"\nTesting: {col_name} with rules: {rules}")
    
    # Check if column exists
    if col_name not in silver_df.columns:
        print(f"  ❌ Column '{col_name}' does not exist!")
        continue
    
    # Test each rule
    for rule in rules:
        try:
            if rule == "not_null":
                # Test not_null rule
                valid_df = silver_df.filter(F.col(col_name).isNotNull())
                invalid_df = silver_df.filter(F.col(col_name).isNull())
                
                total = silver_df.count()
                valid = valid_df.count()
                invalid = invalid_df.count()
                
                print(f"    Rule 'not_null': {valid}/{total} valid, {invalid} invalid")
                
                if invalid > 0:
                    print(f"    ⚠️  Found {invalid} null values!")
                    # Show some invalid rows
                    try:
                        invalid_rows = invalid_df.select(col_name).limit(5).collect()
                        print(f"    Sample invalid values: {[str(row[0]) for row in invalid_rows]}")
                    except Exception as e:
                        print(f"    Could not collect invalid rows: {e}")
                        
            elif isinstance(rule, list) and len(rule) == 2:
                op, value = rule
                if op == "gte":
                    # Test gte rule
                    valid_df = silver_df.filter(F.col(col_name) >= value)
                    invalid_df = silver_df.filter(F.col(col_name) < value)
                    
                    total = silver_df.count()
                    valid = valid_df.count()
                    invalid = invalid_df.count()
                    
                    print(f"    Rule '{op} {value}': {valid}/{total} valid, {invalid} invalid")
                    
                    if invalid > 0:
                        print(f"    ⚠️  Found {invalid} values < {value}!")
        except Exception as e:
            print(f"    ❌ Error testing rule: {e}")
            import traceback
            traceback.print_exc()

# Test the combined validation predicate
print("\nSTEP 4: Test combined validation predicate")
print("=" * 80)

try:
    # Build combined predicate (like apply_column_rules does)
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
    
    if validation_predicate is not None:
        print("  ✅ Combined predicate created")
        
        valid_df = silver_df.filter(validation_predicate)
        invalid_df = silver_df.filter(~validation_predicate)
        
        total = silver_df.count()
        valid = valid_df.count()
        invalid = invalid_df.count()
        
        print(f"  Total rows: {total}")
        print(f"  Valid rows: {valid}")
        print(f"  Invalid rows: {invalid}")
        print(f"  Validation rate: {(valid/total*100) if total > 0 else 0}%")
        
        if invalid > 0:
            print(f"\n  ⚠️  {invalid} rows failed validation!")
            print(f"  Checking which columns have null values in invalid rows...")
            
            # Check each column for nulls in invalid rows
            for col_name in validation_rules.keys():
                null_count = invalid_df.filter(F.col(col_name).isNull()).count()
                if null_count > 0:
                    print(f"    {col_name}: {null_count} null values in invalid rows")
except Exception as e:
    print(f"  ❌ Error testing combined predicate: {e}")
    import traceback
    traceback.print_exc()

# Check actual data values
print("\nSTEP 5: Check actual data values")
print("=" * 80)

try:
    sample = silver_df.select("impression_id", "impression_date_parsed", "hour_of_day", "day_of_week").limit(5).collect()
    print("Sample rows:")
    for row in sample:
        print(f"  {row}")
except Exception as e:
    print(f"  ❌ Error collecting sample: {e}")
    import traceback
    traceback.print_exc()

spark.stop()

