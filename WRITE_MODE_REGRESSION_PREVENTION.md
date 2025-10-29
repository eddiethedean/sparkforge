# Write Mode Regression Prevention

## Problem Fixed

The original issue was that when running `run_incremental()`, the system was incorrectly showing `write_mode: overwrite` in the log table and **actually overwriting the Silver tables**, causing data loss. This was a critical bug because:

1. **Data Loss**: Incremental pipelines were performing full refreshes instead of true incremental updates
2. **Misleading Logs**: The log table showed incorrect write_mode information
3. **Silent Failure**: The bug was not obvious and could cause significant data loss over time

## Root Cause

The bug was in `/Users/odosmatthews/Documents/coding/sparkforge/sparkforge/execution.py` line 311:

```python
# BEFORE (BUGGY):
result.write_mode = "overwrite" if mode != ExecutionMode.VALIDATION_ONLY and not isinstance(step, BronzeStep) else None
```

This hardcoded `"overwrite"` for all execution modes, ignoring the fact that incremental mode should use `"append"`.

## Solution Implemented

Fixed the logic to properly determine write_mode based on execution mode:

```python
# AFTER (FIXED):
# Determine write mode based on execution mode
if mode == ExecutionMode.INCREMENTAL:
    write_mode = "append"
else:  # INITIAL, FULL_REFRESH, or other modes
    write_mode = "overwrite"

# Set write_mode based on execution mode and step type
if mode != ExecutionMode.VALIDATION_ONLY and not isinstance(step, BronzeStep):
    if mode == ExecutionMode.INCREMENTAL:
        result.write_mode = "append"
    else:  # INITIAL, FULL_REFRESH, or other modes
        result.write_mode = "overwrite"
else:
    result.write_mode = None
```

## Behavior After Fix

| Execution Mode | Write Mode | Behavior |
|----------------|------------|----------|
| `INITIAL` | `overwrite` | Replaces all existing data (correct) |
| `INCREMENTAL` | `append` | Adds new data without losing existing data (fixed!) |
| `FULL_REFRESH` | `overwrite` | Replaces all existing data (correct) |
| `VALIDATION_ONLY` | `None` | No writing occurs (correct) |

## Regression Prevention Tests

Created comprehensive tests to prevent this bug from being reintroduced:

### Test Files Created

1. **`tests/unit/test_execution_write_mode.py`** - Core execution engine tests
2. **`tests/unit/test_pipeline_runner_write_mode.py`** - Pipeline runner tests  
3. **`tests/integration/test_write_mode_integration.py`** - End-to-end integration tests

### Key Test Categories

1. **Mode-Specific Tests**: Verify each execution mode uses the correct write_mode
2. **Step Type Tests**: Verify Silver and Gold steps behave consistently
3. **Regression Prevention**: Specific tests that will fail if the bug is reintroduced
4. **Integration Tests**: End-to-end tests that verify the complete pipeline behavior

### Critical Regression Test

The most important test is `test_incremental_mode_never_uses_overwrite` which specifically prevents the original bug:

```python
def test_incremental_mode_never_uses_overwrite(self, spark_session):
    """Test that incremental mode never uses overwrite to prevent data loss."""
    # This test specifically prevents the regression where incremental mode
    # was incorrectly using overwrite, causing data loss
    result = execution_engine.execute_step(
        silver_step, context, ExecutionMode.INCREMENTAL
    )
    
    # This assertion will fail if the bug is reintroduced
    assert result.write_mode != "overwrite", (
        "CRITICAL BUG: Incremental mode is using overwrite! "
        "This will cause data loss. Incremental mode must use append."
    )
    
    assert result.write_mode == "append", (
        "Incremental mode must use append write_mode to preserve existing data"
    )
```

## Test Runner Script

Created `scripts/test_write_mode_regression.py` for easy testing:

```bash
# Run all write mode regression tests
python scripts/test_write_mode_regression.py

# Run only the critical regression test
python scripts/test_write_mode_regression.py --critical
```

## How to Prevent Future Regressions

1. **Run Tests Regularly**: Always run the regression tests before deploying changes
2. **CI/CD Integration**: Add these tests to your CI/CD pipeline
3. **Code Reviews**: When modifying execution logic, ensure these tests still pass
4. **Documentation**: This document serves as a reference for the fix and prevention

## Verification

To verify the fix is working:

```bash
# Run the critical test
python scripts/test_write_mode_regression.py --critical

# Or run specific tests
python -m pytest tests/unit/test_execution_write_mode.py::TestWriteModeRegression::test_incremental_mode_never_uses_overwrite -v
```

## Impact

- ✅ **Data Preservation**: Incremental pipelines now preserve existing data
- ✅ **Correct Logging**: Log tables now show accurate write_mode information  
- ✅ **Regression Prevention**: Comprehensive tests prevent the bug from returning
- ✅ **Confidence**: Developers can confidently use incremental mode without data loss concerns

This fix ensures that incremental pipelines work as expected, preserving historical data while adding new incremental data, and provides robust testing to prevent this critical bug from ever being reintroduced.
