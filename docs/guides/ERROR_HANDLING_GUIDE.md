# Error Handling Guide

**Version**: 2.8.0  
**Last Updated**: February 2025

## Table of Contents

1. [Introduction](#introduction)
2. [Common Errors](#common-errors)
3. [Exception Hierarchy](#exception-hierarchy)
4. [What to Do](#what-to-do)
5. [Handling Run Failures](#handling-run-failures)
6. [Related Guides](#related-guides)

---

## Introduction

SparkForge uses a small set of exception types for configuration and execution failures. This guide describes the most common errors, the exception hierarchy, and how to fix or debug them. All content is self-contained within the guides directory.

## Common Errors

**Engine not configured**

- **Symptom:** Error when importing or using PipelineBuilder (or other pipeline components) before the engine is set.
- **Cause:** You must call `configure_engine(spark=spark)` before using any pipeline components.
- **Fix:** Create your Spark session, then call `configure_engine(spark=spark)` immediately after; only then import and use PipelineBuilder. See [Getting Started](./GETTING_STARTED_GUIDE.md).

**Validation failures**

- **Symptom:** Pipeline or step fails; message may mention validation rate or threshold.
- **Cause:** Data did not pass validation rules, or validation rate fell below the minimum (e.g. `min_bronze_rate`).
- **Fix:** Check input data and validation rules. Use [Validation Rules](./VALIDATION_RULES_GUIDE.md) to ensure rules match the data. Lower thresholds temporarily for debugging if needed. Use [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) to run a single step and inspect the DataFrame.

**Missing or wrong columns**

- **Symptom:** Errors about missing columns, or columns disappearing after a transform.
- **Cause:** A transform does not output a column that has validation rules, or a downstream step expects a column that was dropped.
- **Fix:** Ensure every column you want to keep has at least one validation rule in the step that produces it. See [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) and [Validation Rules](./VALIDATION_RULES_GUIDE.md).

**Step dependency errors**

- **Symptom:** Error that a bronze or silver step was not found (e.g. `source_bronze` or `source_silvers` invalid).
- **Cause:** A silver step references a bronze name that does not exist, or a gold step references a silver name that does not exist.
- **Fix:** Match step names exactly. Ensure `with_bronze_rules` / `add_silver_transform` / `add_gold_transform` use the same names you pass in `source_bronze` and `source_silvers`.

**Empty or invalid configuration**

- **Symptom:** Error when adding a step (e.g. empty rules, empty name, invalid schema).
- **Cause:** Builder methods validate inputs; empty or invalid config raises.
- **Fix:** Provide non-empty step names, non-empty rules dicts, and a valid schema. Run `validate_pipeline()` after building and fix any reported errors.

**Delta Lake / table errors**

- **Symptom:** Failures when writing to a table (permissions, schema, path).
- **Cause:** Schema or table path not available, permissions, or Delta extensions not configured.
- **Fix:** Configure Spark with Delta extensions (see [Getting Started](./GETTING_STARTED_GUIDE.md) and [Deployment](./DEPLOYMENT_GUIDE.md)). Ensure the schema exists and the process has write access. See [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) for more.

## Exception Hierarchy

The framework uses a small set of exception types (names may vary by package; conceptually):

- **PipelineConfigurationError (or ConfigurationError):** Invalid pipeline or builder configuration (e.g. missing schema, invalid parameters). Raised during builder setup or validation.
- **PipelineExecutionError (or ExecutionError):** Pipeline run failed (e.g. a step failed, or execution was aborted). Raised or wrapped when `run_initial_load`, `run_incremental`, etc. fail.
- **StepError (or ExecutionError used as step error):** A specific step failed. May include step name and step type (bronze/silver/gold) in context or message.
- **ValidationError:** Invalid rules or validation logic (e.g. empty rules, invalid rule format).

When you catch these, check the error message and any `context` or `suggestions` attributes for details. Use [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) for step-by-step fixes.

## What to Do

1. **Configure the engine first:** Always call `configure_engine(spark=spark)` before using pipeline components.
2. **Validate the pipeline:** After building, call `builder.validate_pipeline()` and fix any returned errors before calling `to_pipeline()`.
3. **Check the result after each run:** Inspect `result.status.value` and `result.errors`. If failed, use `result.bronze_results`, `result.silver_results`, `result.gold_results` to see which step failed and why (e.g. validation_rate, error message).
4. **Debug step by step:** Use [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) to run a single step with the same inputs and inspect the output or error in isolation.
5. **Review rules and data:** Ensure validation rules match the columns your transforms produce and that source data is in the expected format. See [Validation Rules](./VALIDATION_RULES_GUIDE.md).

## Handling Run Failures

After a run, check the result and errors:

```python
result = pipeline.run_initial_load(bronze_sources={"raw_events": source_df})

if result.status.value != "completed":
    for err in result.errors:
        print(err)
    # Inspect per-step info
    for step_name, info in result.bronze_results.items():
        if info.get("error"):
            print(f"Bronze {step_name}: {info['error']}")
    for step_name, info in result.silver_results.items():
        if info.get("error"):
            print(f"Silver {step_name}: {info['error']}")
    for step_name, info in result.gold_results.items():
        if info.get("error"):
            print(f"Gold {step_name}: {info['error']}")
```

Catch configuration errors when building:

```python
from pipeline_builder.errors import PipelineConfigurationError, StepError

try:
    builder.with_bronze_rules(name="events", rules={...})
    # ... add more steps ...
    errors = builder.validate_pipeline()
    if errors:
        raise PipelineConfigurationError(f"Validation failed: {errors}")
    pipeline = builder.to_pipeline()
except (PipelineConfigurationError, StepError) as e:
    print(e)
    if hasattr(e, "suggestions") and e.suggestions:
        print("Suggestions:", e.suggestions)
    if hasattr(e, "context") and e.context:
        print("Context:", e.context)
```

## Related Guides

- [Getting Started](./GETTING_STARTED_GUIDE.md) — Engine configuration and first pipeline.
- [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) — Run one step at a time for debugging.
- [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) — Installation, engine, validation, and execution issues and fixes.
- [Validation Rules](./VALIDATION_RULES_GUIDE.md) — Rule syntax and behavior.
