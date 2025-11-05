# Mock-Spark Threading/Segmentation Fault Fix Plan

## Problem Summary

When running tests with pytest-xdist in parallel (`-n 8`), the `test_fraud_detection_scenarios` test in `test_financial_pipeline.py` fails with a segmentation fault. The test passes when run sequentially (`-n 1`).

### Root Causes Identified

1. **DuckDB Connection Isolation**: DuckDB connections created in ThreadPoolExecutor worker threads don't see schemas created in the main thread or other worker threads. This is a known DuckDB limitation.

2. **Schema Visibility Race Condition**: When multiple steps execute in parallel via ThreadPoolExecutor, each worker thread has its own DuckDB connection, and schemas created via `CREATE SCHEMA` in one thread may not be visible to other threads immediately.

3. **Process-Level Isolation**: When pytest-xdist runs tests in parallel across multiple processes, each process has its own DuckDB instance, but there may be shared resource contention or memory corruption issues.

4. **Thread Safety in DuckDB**: DuckDB operations from multiple threads within the same process may not be fully thread-safe, especially around schema creation and catalog operations.

## Current Mitigations (Already in Code)

The codebase already has some mitigations in place:

1. **Schema Pre-creation** (line 546-567 in `execution.py`):
   - Creates all required schemas in the main thread before parallel execution
   - Uses `CREATE SCHEMA IF NOT EXISTS` for idempotency
   - Has fallback to `_ensure_schema_exists` method

2. **Per-Thread Schema Creation** (line 411-424 in `execution.py`):
   - Creates schemas in each worker thread before `saveAsTable`
   - Tries both SQL and storage API methods

3. **Context Locking** (line 660 in `execution.py`):
   - Uses `context_lock` to protect shared context updates

## Issues with Current Implementation

1. **Insufficient Schema Creation**: The per-thread schema creation may not be executed early enough or may fail silently.

2. **No Connection Pooling**: Each worker thread gets a new DuckDB connection, but there's no guarantee of schema visibility.

3. **Race Conditions**: Between schema creation and table creation, another thread might interfere.

4. **No Process-Level Isolation**: pytest-xdist creates separate processes, but DuckDB operations may still have issues.

## Fix Strategy

### Option 1: Disable Parallel Execution for Mock-Spark (Recommended for Quick Fix)

**Pros**: 
- Simple, immediate fix
- No risk of threading issues
- Tests will pass consistently

**Cons**:
- Slower test execution
- Doesn't test parallel execution path for mock-spark

**Implementation**:
1. Detect mock-spark mode
2. Automatically set `parallel.enabled=False` when using mock-spark
3. Add configuration option to override if needed

### Option 2: Improve Thread-Safe Schema Creation (Recommended for Long-term)

**Pros**:
- Maintains parallel execution benefits
- Better tests real-world scenarios
- Fixes root cause

**Cons**:
- More complex implementation
- May require changes to mock-spark usage patterns

**Implementation Steps**:

1. **Enhanced Schema Creation with Retries**:
   ```python
   def _ensure_schema_in_thread(self, schema: str, connection):
       """Ensure schema exists in current thread's DuckDB connection."""
       max_retries = 3
       for attempt in range(max_retries):
           try:
               connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
               # Verify schema exists
               result = connection.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'")
               if result.fetchone():
                   return True
           except Exception as e:
               if attempt == max_retries - 1:
                   raise
               time.sleep(0.01 * (attempt + 1))  # Exponential backoff
       return False
   ```

2. **Connection-Level Schema Caching**:
   - Create a thread-local schema cache
   - Track which schemas have been created in each thread
   - Always create schemas in each thread before use

3. **Thread-Safe Storage Access**:
   - Use locks around storage operations
   - Ensure schema creation and table creation are atomic per thread

4. **Better Error Handling**:
   - Catch DuckDB-specific errors
   - Provide clear error messages
   - Retry on transient failures

### Option 3: Use Sequential Execution for Mock-Spark Tests Only

**Pros**:
- Tests still run in parallel (pytest-xdist)
- Only pipeline execution is sequential
- No threading issues

**Cons**:
- Slower pipeline execution within tests
- Doesn't test parallel execution

**Implementation**:
- Detect mock-spark mode
- Set `max_workers=1` for ThreadPoolExecutor when using mock-spark
- Keep pytest-xdist parallelism

### Option 4: Patch Mock-Spark Storage Backend

**Pros**:
- Fixes the issue at the source
- Benefits all users of mock-spark

**Cons**:
- Requires understanding mock-spark internals
- May need to fork or submit PR to mock-spark
- More effort

**Implementation**:
- Investigate mock-spark's DuckDB backend
- Add proper connection pooling or schema propagation
- Ensure thread-safe operations

## Recommended Approach

**Immediate Fix (Option 1 + 3 Hybrid)**:
1. Detect mock-spark mode automatically
2. Disable parallel execution (`parallel.enabled=False`) when using mock-spark
3. Keep pytest-xdist parallelism for test execution speed
4. Add a configuration flag to override this behavior if needed

**Long-term Fix (Option 2)**:
1. Implement enhanced schema creation with retries
2. Add thread-local schema caching
3. Improve error handling and logging
4. Add comprehensive tests for parallel execution with mock-spark

## Implementation Plan

### Phase 1: Quick Fix (1-2 hours)

1. **Add mock-spark detection in ExecutionEngine**:
   ```python
   def _is_mock_spark(self) -> bool:
       """Detect if using mock-spark."""
       return hasattr(self.spark, 'storage') and hasattr(self.spark.storage, 'schemas')
   ```

2. **Auto-disable parallel execution for mock-spark**:
   ```python
   if self._is_mock_spark() and self.config.parallel.enabled:
       self.logger.warning(
           "Parallel execution disabled for mock-spark due to DuckDB threading limitations. "
           "Set SPARKFORGE_FORCE_PARALLEL=1 to override."
       )
       workers = 1
   ```

3. **Add environment variable override**:
   - `SPARKFORGE_FORCE_PARALLEL=1` to force parallel execution even with mock-spark

4. **Update documentation**:
   - Document the limitation in `MOCK_SPARK_LIMITATIONS.md`
   - Explain why parallel execution is disabled

### Phase 2: Enhanced Schema Handling (2-4 hours)

1. **Implement thread-local schema tracking**:
   ```python
   import threading
   _thread_schemas = threading.local()
   
   def _get_thread_schemas(self):
       if not hasattr(_thread_schemas, 'schemas'):
           _thread_schemas.schemas = set()
       return _thread_schemas.schemas
   ```

2. **Enhance `_ensure_schema_exists`**:
   - Add retry logic with exponential backoff
   - Use thread-local cache to avoid redundant creation
   - Verify schema exists before returning

3. **Add per-thread schema creation before table operations**:
   - Always create schema in current thread before `saveAsTable`
   - Use connection-specific methods if available

### Phase 3: Testing and Validation (1-2 hours)

1. **Add test for parallel execution with mock-spark**:
   - Test that parallel execution works (or is disabled)
   - Verify no segmentation faults
   - Check schema visibility

2. **Run full test suite with `-n 8`**:
   - Verify all tests pass
   - Check for any remaining threading issues

3. **Performance testing**:
   - Compare sequential vs parallel execution times
   - Measure impact of schema creation overhead

## Files to Modify

1. **pipeline_builder/execution.py**:
   - Add `_is_mock_spark()` method
   - Modify `execute_pipeline()` to disable parallel for mock-spark
   - Enhance `_ensure_schema_exists()` with retries
   - Add thread-local schema tracking

2. **pipeline_builder/models/execution.py**:
   - Add configuration option for forcing parallel execution
   - Document the mock-spark limitation

3. **docs/MOCK_SPARK_LIMITATIONS.md**:
   - Document parallel execution limitation
   - Explain workarounds
   - Add examples

4. **tests/conftest.py**:
   - Consider adding test isolation improvements
   - Ensure proper cleanup between tests

## Testing Strategy

1. **Unit Tests**:
   - Test `_is_mock_spark()` detection
   - Test parallel execution disabling logic
   - Test schema creation with retries

2. **Integration Tests**:
   - Run `test_fraud_detection_scenarios` with `-n 8`
   - Run full test suite with parallel pytest
   - Verify no segmentation faults

3. **Performance Tests**:
   - Measure execution time with sequential execution
   - Compare with parallel execution (when enabled)

## Risk Assessment

**Low Risk (Option 1)**:
- Simple change
- Minimal code modification
- Easy to revert

**Medium Risk (Option 2)**:
- More complex implementation
- Requires thorough testing
- May introduce new bugs

**High Risk (Option 4)**:
- Requires external dependency changes
- May break other functionality
- Hard to maintain

## Success Criteria

1. ✅ All tests pass with `-n 8` (pytest-xdist parallel)
2. ✅ No segmentation faults
3. ✅ No threading-related errors
4. ✅ Clear documentation of limitations
5. ✅ Performance acceptable (sequential execution is acceptable for mock-spark)

## Timeline Estimate

- **Phase 1 (Quick Fix)**: 1-2 hours
- **Phase 2 (Enhanced Handling)**: 2-4 hours  
- **Phase 3 (Testing)**: 1-2 hours
- **Total**: 4-8 hours

## Notes

- The segmentation fault only occurs with pytest-xdist parallelism (`-n 8`), not with sequential pytest execution
- The issue is specific to mock-spark 2.16.1+ with DuckDB backend
- Real PySpark doesn't have this issue (uses proper connection pooling)
- Consider filing an issue with mock-spark project if this is a known limitation

