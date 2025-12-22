#!/usr/bin/env python3
"""Analyze if all failures share the same root cause."""

failures = {
    "logging_tests": {
        "count": 14,
        "error": "invalid series dtype: expected `String`, got `datetime[μs]` for series with name `timestamp_str`",
        "root_cause": "Datetime type handling - explicit cast() doesn't work"
    },
    "marketing": {
        "error": "invalid series dtype: expected `String`, got `datetime[μs]` for series with name `impression_date_parsed`",
        "root_cause": "Datetime type handling - to_timestamp() creates datetime"
    },
    "streaming": {
        "error": "invalid series dtype: expected `String`, got `datetime[μs]` for series with name `event_timestamp_parsed`",
        "root_cause": "Datetime type handling - to_timestamp() creates datetime"
    },
    "healthcare": {
        "error": "0% valid rate - validation fails on datetime columns",
        "root_cause": "Datetime type handling - validation can't handle datetime types"
    },
    "supply_chain": {
        "error": "'DataFrame' object has no attribute 'snapshot_date'",
        "root_cause": "Column tracking - sparkless references original column after transform"
    },
    "data_quality": {
        "error": "unable to find column 'transaction_date_parsed' - sees 'date' instead",
        "root_cause": "Column tracking - sparkless doesn't track datetime column renames"
    }
}

print("=" * 60)
print("ROOT CAUSE ANALYSIS")
print("=" * 60)
print()

datetime_related = []
column_tracking = []

for name, info in failures.items():
    if "datetime" in info["root_cause"].lower():
        datetime_related.append(name)
    elif "column" in info["root_cause"].lower():
        column_tracking.append(name)

print(f"Datetime-related issues: {len(datetime_related)}")
for name in datetime_related:
    print(f"  - {name}: {failures[name]['error'][:60]}...")

print()
print(f"Column tracking issues: {len(column_tracking)}")
for name in column_tracking:
    print(f"  - {name}: {failures[name]['error'][:60]}...")

print()
print("=" * 60)
print("CONCLUSION")
print("=" * 60)
print()

if len(column_tracking) == 0:
    print("✅ ALL failures are datetime-related!")
    print("   Fixing datetime type handling should fix all 19 tests.")
elif len(datetime_related) > len(column_tracking):
    print(f"⚠️  Most failures ({len(datetime_related)}) are datetime-related")
    print(f"   {len(column_tracking)} failures may need separate fixes")
    print()
    print("   However, column tracking issues might also be caused by")
    print("   datetime column transformations not being tracked properly.")
    print("   Fixing datetime handling might fix these too.")
else:
    print("❌ Multiple distinct root causes identified")
    print("   Need separate fixes for different issues")

