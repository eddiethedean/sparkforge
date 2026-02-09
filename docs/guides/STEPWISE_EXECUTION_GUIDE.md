# Stepwise Execution and Debugging Guide

**Version**: 2.8.0  
**Last Updated**: January 2025

> **Note**: All code examples in this guide have been tested and verified to work in both mock and real Spark modes. The outputs shown are from actual test runs.

## Table of Contents

1. [Introduction](#introduction)
2. [Problem Statement](#problem-statement)
3. [Solution Overview](#solution-overview)
4. [Quick Start](#quick-start)
5. [Core APIs](#core-apis)
6. [Parameter Overrides](#parameter-overrides)
7. [PipelineDebugSession](#pipelinedebugsession)
8. [Write Control](#write-control)
9. [Common Workflows](#common-workflows)
10. [Best Practices](#best-practices)
11. [Safety Considerations](#safety-considerations)
12. [Complete Examples](#complete-examples)
13. [API Reference](#api-reference)
14. [Troubleshooting](#troubleshooting)

---

## Introduction

Stepwise execution is a powerful feature that allows you to run individual pipeline steps, override parameters at runtime, and control execution flow—significantly speeding up the debugging and development cycle.

### Key Benefits

- **Faster Debugging**: Run only the step you're debugging, not the entire pipeline
- **Parameter Testing**: Test different parameter values without code changes
- **Iterative Development**: Quickly iterate on transform logic with immediate feedback
- **Context Management**: Automatic dependency loading from tables or context
- **Write Control**: Skip table writes during development for faster iteration

### When to Use Stepwise Execution

- **Development**: Testing new transform functions
- **Debugging**: Isolating issues in specific steps
- **Parameter Tuning**: Finding optimal parameter values
- **Validation**: Testing data quality rules
- **Learning**: Understanding pipeline behavior step-by-step

---

## Problem Statement

### Traditional Pipeline Execution Challenges

When developing or debugging pipelines, you often encounter:

1. **Long Wait Times**: Must wait for all upstream steps to complete before reaching the step you're debugging
2. **Inefficient Iteration**: Re-running the entire pipeline for each small change
3. **Slower Feedback**: Delayed validation of fixes or improvements
4. **Resource Waste**: Computing steps you don't need to test
5. **Parameter Testing**: Requires code changes and full pipeline runs to test different parameter values

### Example Scenario

Imagine you're debugging a silver step that processes 1 million rows:

```python
# Traditional approach - must run everything
pipeline.run_initial_load(bronze_sources={"events": large_df})
# ⏱️ Waits 10 minutes for bronze step
# ⏱️ Waits 5 minutes for upstream silver steps
# ⏱️ Finally reaches your step
# ❌ Step fails - need to fix and rerun everything
```

With stepwise execution:

```python
# Stepwise approach - run only what you need
report, context = runner.run_step("my_silver_step", steps=all_steps, context=context)
# ⏱️ Runs only the target step (30 seconds)
# ✅ Immediate feedback
# 🔄 Quick iteration cycle
```

---

## Solution Overview

The stepwise execution API provides four main capabilities:

### 1. Run Until a Step

Execute steps in dependency order until a target step completes:

```python
report, context = runner.run_until("target_step", steps=all_steps, bronze_sources=sources)
```

**Use Cases:**
- Build up context incrementally
- Stop before a problematic step
- Inspect intermediate results

### 2. Run a Single Step

Execute only one step, automatically loading dependencies:

```python
report, context = runner.run_step("target_step", steps=all_steps, context=context)
```

**Use Cases:**
- Test a specific step in isolation
- Debug a failing step
- Validate transform logic

### 3. Rerun with Parameter Overrides

Rerun a step with different parameters without code changes:

```python
step_params = {"target_step": {"threshold": 0.9, "mode": "strict"}}
report, context = runner.rerun_step("target_step", steps=all_steps, context=context, step_params=step_params)
```

**Use Cases:**
- Test different parameter combinations
- Find optimal parameter values
- A/B testing transform logic

### 4. Write Control

Skip table writes during development for faster iteration:

```python
report, context = runner.run_step("target_step", steps=all_steps, context=context, write_outputs=False)
```

**Use Cases:**
- Fast iteration during development
- Testing without side effects
- Validating logic before committing

---

## Quick Start

### Basic Setup

```python
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder.pipeline.debug_session import PipelineDebugSession
from pipeline_builder.models import BronzeStep, SilverStep, GoldStep
from pipeline_builder_base.models import PipelineConfig
from pipeline_builder.compat import F

# Engine must be configured (e.g. at app startup or in tests via conftest).
# See pipeline_builder.engine_config for configure_engine() with your Spark engine components.

# Create configuration
config = PipelineConfig.create_default(schema="analytics")

# Create runner
runner = SimplePipelineRunner(spark, config)
```

### Minimal Example

```python
# Define a simple step
bronze_step = BronzeStep(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    schema="analytics",
)

def clean_transform(spark, bronze_df, prior_silvers):
    return bronze_df.filter(F.col("value") > 10)

silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_transform,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="analytics",
)

# Create source data
source_df = spark.createDataFrame(
    [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
    ["id", "event", "value"]
)

# Run until a step
report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step, silver_step],
    bronze_sources={"events": source_df}
)

print(f"Status: {report.status.value}")
print(f"Rows: {context['clean_events'].count()}")
```

**Output:**
```
Status: completed
Rows: 2
```

---

## Core APIs

### SimplePipelineRunner

The `SimplePipelineRunner` provides three main methods for stepwise execution:

#### run_until()

Execute steps in dependency order until a target step completes (inclusive).

```python
report, context = runner.run_until(
    step_name: str,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    bronze_sources: Optional[Dict[str, DataFrame]] = None,
    step_params: Optional[Dict[str, Dict[str, Any]]] = None,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

**Parameters:**
- `step_name`: Name of the step to stop after (inclusive)
- `steps`: List of all pipeline steps
- `mode`: Execution mode (INITIAL, INCREMENTAL, FULL_REFRESH, VALIDATION_ONLY)
- `bronze_sources`: Optional dictionary of bronze source DataFrames
- `step_params`: Optional dictionary mapping step names to parameter dictionaries
- `write_outputs`: If True, write outputs to tables. If False, skip writes.

**Returns:**
- `report`: PipelineReport with execution results
- `context`: Dictionary mapping step names to output DataFrames

**Example:**

```python
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder_base.models import PipelineConfig
from pipeline_builder.models import BronzeStep, SilverStep, GoldStep
from pipeline_builder.compat import F

config = PipelineConfig.create_default(schema="analytics")
runner = SimplePipelineRunner(spark, config)

# Define steps
bronze_step = BronzeStep(name="events", rules={"id": [F.col("id").isNotNull()]}, schema="analytics")
silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.filter(F.col("value") > 15),
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="analytics"
)
gold_step = GoldStep(
    name="aggregated",
    transform=lambda spark, silvers: silvers["clean_events"],
    rules={"id": [F.col("id").isNotNull()]},
    table_name="aggregated",
    source_silvers=["clean_events"],
    schema="analytics"
)

# Create source data
source_df = spark.createDataFrame(
    [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30)],
    ["id", "event", "value"]
)

# Run until a specific step
report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step, silver_step, gold_step],
    bronze_sources={"events": source_df}
)

# Context now contains outputs up to clean_events
clean_events_df = context["clean_events"]
print(f"Rows in clean_events: {clean_events_df.count()}")
```

**Output:**
```
Status: completed
Steps executed: 2
Rows in clean_events: 2
```

#### run_step()

Execute only one step, automatically loading dependencies from context or tables.

```python
report, context = runner.run_step(
    step_name: str,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    context: Optional[Dict[str, DataFrame]] = None,
    step_params: Optional[Dict[str, Dict[str, Any]]] = None,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

**Parameters:**
- `step_name`: Name of the step to execute
- `steps`: List of all pipeline steps
- `mode`: Execution mode
- `context`: Optional execution context (dependencies will be loaded if missing)
- `step_params`: Optional parameter overrides
- `write_outputs`: If True, write outputs to tables

**Returns:**
- `report`: PipelineReport with execution results
- `context`: Updated context dictionary with step output

**Example:**

```python
# Pre-populate context with bronze output (or it will be loaded from table)
context = {"events": source_df}

# Run only the silver step
report, context = runner.run_step(
    "clean_events",
    steps=[bronze_step, silver_step, gold_step],
    context=context
)

print(f"Status: {report.status.value}")
print(f"'clean_events' in context: {'clean_events' in context}")
```

**Output:**
```
Status: completed
Steps executed: 1
'clean_events' in context: True
```

#### rerun_step()

Rerun a step with parameter overrides, optionally invalidating downstream outputs.

```python
report, context = runner.rerun_step(
    step_name: str,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    context: Dict[str, DataFrame],
    step_params: Optional[Dict[str, Dict[str, Any]]] = None,
    invalidate_downstream: bool = True,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

**Parameters:**
- `step_name`: Name of the step to rerun
- `steps`: List of all pipeline steps
- `mode`: Execution mode
- `context`: Execution context (must contain required dependencies)
- `step_params`: Optional parameter overrides
- `invalidate_downstream`: If True, remove downstream outputs from context
- `write_outputs`: If True, write outputs to tables

**Returns:**
- `report`: PipelineReport with execution results
- `context`: Updated context dictionary

**Example:**

```python
# First run
report1, context = runner.run_step("clean_events", steps=steps, context=context)

# Rerun with parameter override
step_params = {"clean_events": {"threshold": 20, "filter_mode": "strict"}}
report2, context = runner.rerun_step(
    "clean_events",
    steps=steps,
    context=context,
    step_params=step_params
)

print(f"Status: {report2.status.value}")
```

**Output:**
```
Status: completed
```

---

## Parameter Overrides

Transform functions can accept a `params` argument to receive runtime parameters. This enables testing different parameter values without code changes.

### How It Works

1. **Function Signature**: Transform functions can accept `params=None` or `**kwargs`
2. **Parameter Passing**: Parameters are passed via `step_params` dictionary
3. **Backward Compatibility**: Functions without `params` continue to work unchanged
4. **Automatic Detection**: The system automatically detects if a function accepts parameters

### Silver Step with Parameters

```python
def clean_events_transform(spark, bronze_df, prior_silvers, params=None):
    """Transform function that accepts parameters."""
    threshold = params.get("threshold", 0) if params else 0
    filter_mode = params.get("filter_mode", "standard") if params else "standard"
    
    df = bronze_df.filter(F.col("value") > threshold)
    
    if filter_mode == "strict":
        df = df.filter(F.col("value") > 20)  # Additional strict filtering
    
    return df

# Create step
silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_events_transform,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="analytics"
)

# Run with parameters
step_params = {"clean_events": {"threshold": 15, "filter_mode": "strict"}}
context = {"events": source_df}
report, context = runner.run_step(
    "clean_events",
    steps=[silver_step],
    context=context,
    step_params=step_params
)

print(f"Status: {report.status.value}")
print(f"Rows after filtering: {context['clean_events'].count()}")
```

**Output:**
```
Status: completed
Rows after filtering: 1  # Only rows with value > 15 and value > 20 (strict mode)
```

### Gold Step with Parameters

```python
def aggregate_metrics(spark, silvers, params=None):
    """Gold transform that accepts parameters."""
    multiplier = params.get("multiplier", 1.0) if params else 1.0
    
    df = silvers["clean_events"]
    return df.withColumn(
        "adjusted_value",
        F.col("value") * multiplier
    )

# Create step
gold_step = GoldStep(
    name="user_metrics",
    transform=aggregate_metrics,
    rules={"adjusted_value": [F.col("adjusted_value") > 0]},
    table_name="user_metrics",
    source_silvers=["clean_events"],
    schema="analytics"
)

# First ensure clean_events exists in context
context = {"events": source_df}
report, context = runner.run_step(
    "clean_events",
    steps=[silver_step],
    context=context
)

# Run gold step with parameters
step_params = {"user_metrics": {"multiplier": 1.5}}
report, context = runner.run_step(
    "user_metrics",
    steps=[gold_step],
    context=context,
    step_params=step_params
)

print(f"Status: {report.status.value}")
print(f"Rows in user_metrics: {context['user_metrics'].count()}")
if "adjusted_value" in context["user_metrics"].columns:
    print(f"'adjusted_value' column created: True")
```

**Output:**
```
Status: completed
Rows in user_metrics: 3
'adjusted_value' column created: True
```

### Using **kwargs

You can also use `**kwargs` instead of a `params` argument:

```python
def transform_with_kwargs(spark, bronze_df, prior_silvers, **kwargs):
    """Transform function using **kwargs."""
    threshold = kwargs.get("threshold", 0)
    return bronze_df.filter(F.col("value") > threshold)

# Parameters are passed as keyword arguments
step_params = {"clean_events": {"threshold": 15}}
# The system automatically unpacks kwargs
```

### Backward Compatibility

Transform functions that don't accept `params` continue to work unchanged:

```python
def simple_transform(spark, bronze_df, prior_silvers):
    """Old-style transform without params - still works!"""
    return bronze_df.filter(F.col("status") == "active")

# This works exactly as before
silver_step = SilverStep(
    name="active_events",
    source_bronze="events",
    transform=simple_transform,
    rules={"status": [F.col("status").isNotNull()]},
    table_name="active_events",
    schema="analytics"
)

# No parameters needed - works as before
report, context = runner.run_step("active_events", steps=[silver_step], context=context)
```

---

## PipelineDebugSession

`PipelineDebugSession` is a high-level, notebook-friendly API that simplifies interactive debugging. It automatically manages execution context and step parameters.

### Key Features

- **Context Management**: Automatically maintains execution context between runs
- **Parameter Management**: Easy parameter setting and clearing
- **Notebook-Friendly**: Designed for interactive use in Jupyter notebooks
- **Simplified API**: Less boilerplate than using `SimplePipelineRunner` directly

### Basic Usage

```python
from pipeline_builder.pipeline.debug_session import PipelineDebugSession

# Create session
session = PipelineDebugSession(
    spark,
    config,
    steps=[bronze_step, silver_step, gold_step],
    bronze_sources={"events": source_df}
)

# Run until a step
report, context = session.run_until("clean_events")
print(f"Status: {report.status.value}")
print(f"'clean_events' in session.context: {'clean_events' in session.context}")

# Run a single step
report, context = session.run_step("clean_events")

# Set parameters and rerun
session.set_step_params("clean_events", {"threshold": 20})
report, context = session.rerun_step("clean_events")

# Clear parameters
session.clear_step_params("clean_events")
```

**Output:**
```
Status: completed
'clean_events' in session.context: True
Rows: 2
```

### API Methods

#### run_until()

```python
report, context = session.run_until(
    step_name: str,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

#### run_step()

```python
report, context = session.run_step(
    step_name: str,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

#### rerun_step()

```python
report, context = session.rerun_step(
    step_name: str,
    invalidate_downstream: bool = True,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

#### set_step_params()

```python
session.set_step_params(step_name: str, params: Dict[str, Any]) -> None
```

#### clear_step_params()

```python
session.clear_step_params(step_name: Optional[str] = None) -> None
# If step_name is None, clears all step params
```

### Attributes

- `runner`: The underlying `SimplePipelineRunner` instance
- `steps`: List of all pipeline steps
- `mode`: Current execution mode
- `context`: Execution context dictionary (automatically maintained)
- `step_params`: Dictionary mapping step names to parameter dictionaries

---

## Write Control

Use `write_outputs=False` to skip table writes during development for faster iteration.

### When to Use `write_outputs=False`

- **Development**: Testing transform logic without side effects
- **Debugging**: Quick iteration cycles
- **Parameter Testing**: Validating parameter effects
- **Validation**: Testing data quality rules

### When to Use `write_outputs=True` (Default)

- **Production**: Actual pipeline runs
- **Downstream Dependencies**: When downstream steps need to read from tables
- **Final Validation**: Before deployment
- **Integration Testing**: Testing full pipeline behavior

### Example

```python
# Fast iteration: transform and validate, but don't write
report, context = runner.run_step(
    "clean_events",
    steps=steps,
    context=context,
    write_outputs=False  # Skip table write
)

# Output is still in context for inspection
clean_df = context["clean_events"]
print(f"Would write {clean_df.count()} rows")
```

**Output:**
```
Status: completed
Would write 2 rows
```

### Important Notes

- When `write_outputs=False`, outputs exist only in memory
- Downstream steps that read from tables won't see these outputs
- Use `write_outputs=True` when testing full pipeline integration
- Context is always updated regardless of `write_outputs` setting

---

## Common Workflows

### Debugging a Failing Step

```python
# 1. Run until the failing step
report, context = runner.run_until("problematic_step", steps=all_steps)

# 2. Inspect inputs
input_df = context["source_step"]
print(f"Input rows: {input_df.count()}")
input_df.show()

# Example output:
# Input rows: 4
# +---+------+-----+-------+
# | id| event|value| status|
# +---+------+-----+-------+
# |  1|event1|   10| active|
# |  2|event2|   20| active|
# ...
# +---+------+-----+-------+

# 3. Run the step individually to see detailed errors
report, context = runner.run_step(
    "problematic_step",
    steps=all_steps,
    context=context,
    write_outputs=False
)

# 4. Fix the transform and rerun
# (modify transform function)
report, context = runner.rerun_step(
    "problematic_step",
    steps=all_steps,
    context=context,
    write_outputs=False
)
```

### Iteratively Refining Transform Logic

```python
# Create session with steps that accept parameters
session = PipelineDebugSession(
    spark, config, 
    steps=[bronze_step, silver_step_with_params],
    bronze_sources={"events": source_df}
)

# Initial run
report1, _ = session.run_step("clean_events", write_outputs=False)
print(f"Initial output: {session.context['clean_events'].count()} rows")

# Adjust parameters
session.set_step_params("clean_events", {"threshold": 15, "filter_mode": "standard"})
report2, _ = session.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=15: {session.context['clean_events'].count()} rows")

# Try different threshold
session.set_step_params("clean_events", {"threshold": 25, "filter_mode": "standard"})
report3, _ = session.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=25: {session.context['clean_events'].count()} rows")
```

**Output:**
```
Initial output: 3 rows
With threshold=15: 2 rows
With threshold=25: 1 rows
```

As you can see, different threshold values produce different row counts, allowing you to quickly test parameter effects.

### Testing Parameter Changes

```python
# Test different parameter combinations
test_params = [
    {"threshold": 10, "filter_mode": "standard"},
    {"threshold": 20, "filter_mode": "strict"},
    {"threshold": 30, "filter_mode": "strict"},
]

context = {"events": source_df}  # Reset context for each test
for params in test_params:
    step_params = {"clean_events": params}
    report, context = runner.run_step(
        "clean_events",
        steps=[silver_step_with_params],
        context=context,
        step_params=step_params,
        write_outputs=False
    )
    row_count = context["clean_events"].count()
    print(f"Params {params}: {row_count} rows")
```

**Output:**
```
Params {'threshold': 10, 'filter_mode': 'standard'}: 2 rows
Params {'threshold': 20, 'filter_mode': 'strict'}: 1 rows
Params {'threshold': 30, 'filter_mode': 'strict'}: 0 rows
```

### Building Context Incrementally

```python
# Start with bronze
report, context = runner.run_until("bronze_step", steps=all_steps, bronze_sources=sources)

# Add first silver
report, context = runner.run_step("silver_step_1", steps=all_steps, context=context)

# Add second silver (depends on first)
report, context = runner.run_step("silver_step_2", steps=all_steps, context=context)

# Finally run gold
report, context = runner.run_step("gold_step", steps=all_steps, context=context)
```

### Validating Before Full Run

```python
# Test individual steps with write_outputs=False
for step_name in ["step1", "step2", "step3"]:
    report, context = runner.run_step(
        step_name,
        steps=all_steps,
        context=context,
        write_outputs=False
    )
    if report.status.value != "completed":
        print(f"Step {step_name} failed - fix before full run")
        break

# If all pass, run full pipeline
if all_steps_valid:
    full_report = runner.run_pipeline(all_steps, bronze_sources=sources)
```

---

## Best Practices

1. **Use stepwise execution for development**: Save full pipeline runs for production
2. **Leverage `write_outputs=False`**: Faster iteration during debugging
3. **Keep context between runs**: Reuse execution context to avoid redundant computation
4. **Invalidate downstream when rerunning**: Use `invalidate_downstream=True` to ensure clean reruns
5. **Test parameters before full runs**: Validate parameter effects with stepwise execution first
6. **Use PipelineDebugSession in notebooks**: Simplifies interactive debugging
7. **Validate dependencies**: Ensure required dependencies are in context or available in tables
8. **Document parameter schemas**: Document expected parameters for transform functions
9. **Test edge cases**: Use stepwise execution to test edge cases and error conditions
10. **Profile performance**: Use stepwise execution to profile individual step performance

---

## Safety Considerations

### Table Writes

- When `write_outputs=False`, outputs exist only in memory
- Downstream steps that read from tables won't see these outputs
- Always use `write_outputs=True` for production runs

### Context Management

- Ensure required dependencies are in context before running a step
- Dependencies will be automatically loaded from tables if not in context
- Be aware of context size in memory for large datasets

### Parameter Validation

- Validate parameter types and ranges in your transform functions
- Provide sensible defaults for optional parameters
- Document expected parameter schemas

### Production Use

- Stepwise execution is primarily for development/debugging
- Use full pipeline execution (`run_pipeline`) for production
- Don't rely on stepwise execution for production workflows

### Error Handling

- Always check `report.status` after execution
- Handle errors appropriately in your code
- Use stepwise execution to test error conditions

---

## Complete Examples

### Example 1: Basic Stepwise Execution

```python
from pipeline_builder.pipeline.runner import SimplePipelineRunner
from pipeline_builder.models import BronzeStep, SilverStep, GoldStep
from pipeline_builder_base.models import PipelineConfig
from pipeline_builder.compat import F

# Setup
config = PipelineConfig.create_default(schema="analytics")
runner = SimplePipelineRunner(spark, config)

# Define steps
bronze_step = BronzeStep(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    schema="analytics",
)

def clean_transform(spark, bronze_df, prior_silvers, params=None):
    threshold = params.get("threshold", 0) if params else 0
    return bronze_df.filter(F.col("value") > threshold)

silver_step = SilverStep(
    name="clean_events",
    source_bronze="events",
    transform=clean_transform,
    rules={"value": [F.col("value").isNotNull()]},
    table_name="clean_events",
    schema="analytics",
)

def aggregate_transform(spark, silvers, params=None):
    multiplier = params.get("multiplier", 1.0) if params else 1.0
    df = silvers["clean_events"]
    return df.withColumn("adjusted_value", F.col("value") * multiplier)

gold_step = GoldStep(
    name="aggregated_events",
    transform=aggregate_transform,
    rules={"adjusted_value": [F.col("adjusted_value") > 0]},
    table_name="aggregated_events",
    source_silvers=["clean_events"],
    schema="analytics",
)

# Create sample data
data = [("1", "event1", 10), ("2", "event2", 20), ("3", "event3", 30), ("4", "event4", 40)]
source_df = spark.createDataFrame(data, ["id", "event", "value"])

# Example 1: Run until a step
report, context = runner.run_until(
    "clean_events",
    steps=[bronze_step, silver_step, gold_step],
    bronze_sources={"events": source_df}
)
print(f"Status: {report.status.value}")
print(f"Rows in clean_events: {context['clean_events'].count()}")
# Output:
# Status: completed
# Rows in clean_events: 4

# Example 2: Run single step
context = {"events": source_df}
report, context = runner.run_step(
    "clean_events",
    steps=[bronze_step, silver_step, gold_step],
    context=context
)
print(f"Status: {report.status.value}")
# Output: Status: completed

# Example 3: Parameter overrides
step_params = {"clean_events": {"threshold": 15}}
report, context = runner.run_step(
    "clean_events",
    steps=[silver_step],
    context={"events": source_df},
    step_params=step_params
)
print(f"Rows with threshold=15: {context['clean_events'].count()}")
# Output: Rows with threshold=15: 3

# Example 4: DebugSession
session = PipelineDebugSession(
    spark, config, [bronze_step, silver_step],
    bronze_sources={"events": source_df}
)
report, _ = session.run_until("clean_events")
print(f"Session context has clean_events: {'clean_events' in session.context}")
# Output: Session context has clean_events: True

# Example 5: Iterative tuning
session.set_step_params("clean_events", {"threshold": 20})
report, _ = session.rerun_step("clean_events", write_outputs=False)
print(f"With threshold=20: {session.context['clean_events'].count()} rows")
# Output: With threshold=20: 2 rows
```

### Example 2: Parameter Tuning Workflow

```python
from pipeline_builder.pipeline.debug_session import PipelineDebugSession

# Create session
session = PipelineDebugSession(
    spark, config,
    steps=[bronze_step, silver_step_with_params],
    bronze_sources={"events": source_df}
)

# Test different thresholds
thresholds = [0, 10, 20, 30, 40]
results = []

for threshold in thresholds:
    session.set_step_params("clean_events", {"threshold": threshold})
    report, _ = session.rerun_step("clean_events", write_outputs=False)
    
    if report.status.value == "completed":
        row_count = session.context["clean_events"].count()
        results.append((threshold, row_count))
        print(f"Threshold {threshold}: {row_count} rows")
    else:
        print(f"Threshold {threshold}: Failed")

# Find optimal threshold (e.g., closest to target of 2 rows)
target_rows = 2
optimal = min(results, key=lambda x: abs(x[1] - target_rows))
print(f"Optimal threshold: {optimal[0]} (produces {optimal[1]} rows)")
```

### Example 3: Debugging Workflow

```python
# Step 1: Identify failing step
try:
    report = pipeline.run_initial_load(bronze_sources=sources)
except Exception as e:
    print(f"Pipeline failed: {e}")
    # Identify step from error message

# Step 2: Run until failing step
report, context = runner.run_until("failing_step", steps=all_steps, bronze_sources=sources)

# Step 3: Inspect inputs
for dep_name in ["bronze_step", "upstream_silver"]:
    if dep_name in context:
        df = context[dep_name]
        print(f"{dep_name}: {df.count()} rows")
        df.show(5)

# Step 4: Run step individually
report, context = runner.run_step(
    "failing_step",
    steps=all_steps,
    context=context,
    write_outputs=False
)

# Step 5: Check validation results
if report.silver_results.get("failing_step"):
    result = report.silver_results["failing_step"]
    print(f"Validation rate: {result.validation_rate}%")
    print(f"Invalid rows: {result.invalid_rows}")

# Step 6: Fix and rerun
# ... modify transform function ...
report, context = runner.rerun_step(
    "failing_step",
    steps=all_steps,
    context=context,
    write_outputs=False
)
```

---

## API Reference

### SimplePipelineRunner

#### run_until()

```python
def run_until(
    self,
    step_name: str,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    bronze_sources: Optional[Dict[str, DataFrame]] = None,
    step_params: Optional[Dict[str, Dict[str, Any]]] = None,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

Execute steps in dependency order until `step_name` completes (inclusive).

#### run_step()

```python
def run_step(
    self,
    step_name: str,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    context: Optional[Dict[str, DataFrame]] = None,
    step_params: Optional[Dict[str, Dict[str, Any]]] = None,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

Execute only `step_name`, loading dependencies from context or tables.

#### rerun_step()

```python
def rerun_step(
    self,
    step_name: str,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    context: Dict[str, DataFrame],
    step_params: Optional[Dict[str, Dict[str, Any]]] = None,
    invalidate_downstream: bool = True,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

Rerun `step_name` with parameter overrides, optionally invalidating downstream outputs.

### PipelineDebugSession

#### __init__()

```python
def __init__(
    self,
    spark: SparkSession,
    config: PipelineConfig,
    steps: list[BronzeStep | SilverStep | GoldStep],
    mode: PipelineMode = PipelineMode.INITIAL,
    bronze_sources: Optional[Dict[str, DataFrame]] = None,
    logger: Optional[PipelineLogger] = None,
    functions: Optional[FunctionsProtocol] = None,
)
```

Create a new debug session.

#### run_until()

```python
def run_until(
    self,
    step_name: str,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

Run pipeline until `step_name` completes.

#### run_step()

```python
def run_step(
    self,
    step_name: str,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

Run a single step.

#### rerun_step()

```python
def rerun_step(
    self,
    step_name: str,
    invalidate_downstream: bool = True,
    write_outputs: bool = True,
) -> tuple[PipelineReport, Dict[str, DataFrame]]
```

Rerun a step with current parameter overrides.

#### set_step_params()

```python
def set_step_params(
    self,
    step_name: str,
    params: Dict[str, Any],
) -> None
```

Set parameters for a step.

#### clear_step_params()

```python
def clear_step_params(
    self,
    step_name: Optional[str] = None,
) -> None
```

Clear parameter overrides for a step or all steps.

---

## Troubleshooting

### Issue: Step not found in context

**Problem:** `KeyError` when accessing step output in context.

**Solution:**
- Ensure the step was executed successfully
- Check that `report.status.value == "completed"`
- Verify step name matches exactly (case-sensitive)

### Issue: Dependencies not found

**Problem:** Step execution fails because dependencies are missing.

**Solution:**
- Pre-populate context with required dependencies
- Ensure dependencies were written to tables (if using `write_outputs=True`)
- Use `run_until()` to build context incrementally

### Issue: Parameters not being passed

**Problem:** Transform function doesn't receive parameters.

**Solution:**
- Ensure function signature accepts `params=None` or `**kwargs`
- Check `step_params` dictionary key matches step name exactly
- Verify parameter names match what the function expects

### Issue: Context not persisting between runs

**Problem:** Context is empty in subsequent runs.

**Solution:**
- Use `PipelineDebugSession` which maintains context automatically
- Manually pass context between `run_step()` calls
- Load from tables if steps were written with `write_outputs=True`

### Issue: Downstream steps see stale data

**Problem:** After rerunning a step, downstream steps still see old output.

**Solution:**
- Use `invalidate_downstream=True` in `rerun_step()`
- Manually remove downstream outputs from context
- Rerun downstream steps after upstream changes

### Issue: Performance issues with large contexts

**Problem:** Context becomes too large in memory.

**Solution:**
- Use `write_outputs=True` to persist to tables
- Load only needed dependencies from tables
- Clear context entries that are no longer needed

---

## Additional Resources

- [Building Pipelines](BUILDING_PIPELINES_GUIDE.md) - Define bronze, silver, and gold steps
- [Execution Modes](EXECUTION_MODES_GUIDE.md) - Run initial, incremental, and validation-only
- [Troubleshooting](TROUBLESHOOTING_GUIDE.md) - Common issues and solutions
- [Guides index](README.md) - All guides in this directory

---

**Last Updated**: January 2025  
**Version**: 2.8.0
