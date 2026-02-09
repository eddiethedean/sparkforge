# Validation Rules Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Rule Syntax](#rule-syntax)
3. [Expression Rules](#expression-rules)
4. [String Rules](#string-rules)
5. [Helper Methods](#helper-methods)
6. [Common Patterns](#common-patterns)
7. [Validation Behavior and Thresholds](#validation-behavior-and-thresholds)
8. [Related Guides](#related-guides)

---

## Introduction

Validation rules define what “good” data looks like at each step. They are used in bronze (`with_bronze_rules`), silver (`add_silver_transform`, `with_silver_rules`), and gold (`add_gold_transform`, `with_gold_rules`). Rules can be **PySpark column expressions** or **string rules** that are converted to expressions. Rows that fail validation are filtered out; the framework reports validation rate and can fail the step if rate is below a threshold.

For validation-only steps (existing tables), see [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md).

## Rule Syntax

Rules are a **dictionary**: each key is a **column name**, and each value is a **list of rules** for that column. A row is kept only if it satisfies every rule for every column you care about.

```python
rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "value": [F.col("value") > 0, F.col("value") < 1e6],
}
```

- You can pass **PySpark Column expressions** (e.g. `F.col("x").isNotNull()`) or **string rules** (e.g. `["not_null"]`, `["gt", 0]`).
- Multiple rules for the same column are ANDed together.
- **Important for silver/gold:** Only columns that have at least one rule are kept in the output. Omit a column from `rules` and it will be dropped after the transform.

## Expression Rules

Use the same functions object you use elsewhere (e.g. `F = get_default_functions()`):

```python
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

rules = {
    "user_id": [F.col("user_id").isNotNull()],
    "age": [F.col("age").isNotNull(), F.col("age") >= 0, F.col("age") <= 120],
    "email": [F.col("email").isNotNull(), F.length(F.col("email")) > 0],
}
```

Any expression that evaluates to a boolean Column is valid.

## String Rules

String rules are converted to PySpark expressions internally. Use them for readability and to avoid importing `F` in simple cases.

### Single-token rules (column name only)

| String       | Meaning |
|-------------|---------|
| `"not_null"` | Column is not null |
| `"positive"` | Column > 0 |
| `"non_negative"` | Column >= 0 |
| `"non_zero"` | Column != 0 |

Example: `"user_id": ["not_null"]` → `F.col("user_id").isNotNull()`.

### Two-element list: operator + value

| Rule form   | Example | Meaning |
|------------|---------|---------|
| `["gt", v]` | `["value", 0]` | column > v |
| `["gte", v]` | `["age", 0]` | column >= v |
| `["lt", v]` | `["value", 1000]` | column < v |
| `["lte", v]` | `["count", 100]` | column <= v |
| `["eq", v]` | `["status", "active"]` | column == v |
| `["ne", v]` | `["deleted", 1]` | column != v |
| `["in", [v1,v2,...]]` | `["status", ["active","inactive"]]` | column in list |
| `["not_in", [v1,v2,...]]` | `["code", ["X","Y"]]` | column not in list |
| `["like", pattern]` | `["name", "%smith%"]` | column LIKE pattern (SQL) |

Example: `"status": ["in", ["active", "inactive"]]`.

### Three-element list: between

| Rule form | Example | Meaning |
|-----------|---------|---------|
| `["between", min, max]` | `["value", 0, 100]` | column between min and max (inclusive) |

Example:

```python
rules = {
    "user_id": ["not_null"],
    "age": ["gte", 0],
    "value": ["between", 0, 100],
    "status": ["in", ["active", "inactive"]],
}
```

## Helper Methods

`PipelineBuilder` provides static helpers that return a rules dict you can pass or merge:

| Method | Description | Use when |
|--------|-------------|----------|
| `not_null_rules(columns)` | Each column: `F.col(c).isNotNull()` | Many columns must be non-null |
| `positive_number_rules(columns)` | Each column: not null and > 0 | Counts, amounts, quantities |
| `string_not_empty_rules(columns)` | Each column: not null and length > 0 | Names, IDs, categories |
| `timestamp_rules(columns)` | Each column: not null (timestamp) | created_at, updated_at, etc. |

Example:

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions
F = get_default_functions()

# Not null for several columns
base = PipelineBuilder.not_null_rules(["user_id", "timestamp", "action"])

# Positive numbers for value columns
base["value"] = [F.col("value").isNotNull(), F.col("value") > 0]
# Or use:
# base.update(PipelineBuilder.positive_number_rules(["value"]))

builder.with_bronze_rules(name="raw_events", rules=base, incremental_col="timestamp")
```

## Common Patterns

**Not null:**

```python
{"user_id": [F.col("user_id").isNotNull()]}
# or
{"user_id": ["not_null"]}
```

**Numeric range:**

```python
{"age": [F.col("age").isNotNull(), F.col("age") >= 0, F.col("age") <= 120]}
# or
{"age": ["not_null"], "age": ["gte", 0]}  # need both; combine in one list:
{"age": [F.col("age").isNotNull(), F.col("age") >= 0, F.col("age") <= 120]}
```

**Allowed values:**

```python
{"status": ["in", ["active", "inactive", "pending"]]}
```

**Timestamp present:**

```python
{"created_at": ["not_null"]}
# or
PipelineBuilder.timestamp_rules(["created_at", "updated_at"])
```

**Combined:**

```python
rules = {
    "user_id": ["not_null"],
    "value": [F.col("value").isNotNull(), F.col("value") > 0, F.col("value") < 1e9],
    "category": ["in", ["A", "B", "C"]],
}
```

## Validation Behavior and Thresholds

- **Filtering:** Rows that fail any rule for a column are removed before writing (silver/gold) or before passing to downstream steps (bronze).
- **Validation rate:** For each step, the framework computes the fraction of rows that passed validation. This is stored in the result (e.g. `result.bronze_results["raw_events"]["validation_rate"]`).
- **Thresholds:** You can set minimum validation rates when constructing the builder (e.g. `min_bronze_rate`, `min_silver_rate`, `min_gold_rate`). If the rate for a step falls below the threshold, the run can fail.
- **Missing columns:** If a rule references a column that does not exist after a transform, that rule is typically dropped with a warning; avoid relying on columns you do not output.

## Related Guides

- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) — Where to pass rules (bronze, silver, gold).
- [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) — Validating existing tables with `with_silver_rules` and `with_gold_rules`.
- [Execution Modes](./EXECUTION_MODES_GUIDE.md) — How to read validation rate from the result object.
