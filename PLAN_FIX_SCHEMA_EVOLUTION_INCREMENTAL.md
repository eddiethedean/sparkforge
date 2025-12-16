# Plan to Fix Schema Evolution Incremental Test Failure

## Problem Analysis

### Current Issue
The test `test_silver_schema_evolution_incremental_should_error` is failing because:
1. **result1** (initial load) succeeds ✓
2. **result2** (incremental with same schema) fails ✗ - Expected to succeed
3. **result3** (incremental with different schema) fails ✓ - Expected to fail

### Root Cause
After result1 completes, Spark's catalog shows an empty schema (`struct<>`) for the table due to catalog sync issues. When result2 tries to append with the same schema:
- We detect empty schema and skip validation
- We set `schema_validation_skipped = True`
- We try to use `overwriteSchema="true"` with `append` mode
- **Problem**: `overwriteSchema` doesn't work with `append` mode - it only works with `overwrite` mode
- Spark still fails with "column number mismatch" error

### Key Insight
When the schema is empty in incremental mode:
- We can't validate schemas match (no existing schema to compare)
- `mergeSchema` doesn't help (nothing to merge with)
- `overwriteSchema` doesn't work with `append` mode
- We need a different approach

## Solution Strategy

### Option 1: Recover Schema from Actual Data (Recommended)
When schema is empty but table has data:
1. Read a sample of the actual table data
2. Infer the schema from the data
3. Compare with output schema
4. If schemas match, proceed with write (Spark should accept it)
5. If schemas don't match, raise error

### Option 2: Use DESCRIBE TABLE to Recover Schema
1. Use `DESCRIBE TABLE` SQL to get column information
2. Reconstruct schema from DESCRIBE output
3. Compare with output schema
4. Proceed accordingly

### Option 3: Drop and Recreate Table (Not Recommended for Incremental)
- This would lose data, so not suitable for incremental mode
- Only acceptable if we're certain schemas match

## Implementation Plan

### Step 1: Enhance Schema Recovery Logic
**File**: `src/pipeline_builder/execution.py`
**Location**: Around line 618-629 (incremental mode schema validation)

**Changes**:
1. When schema is empty, try to recover it using:
   - First: `DESCRIBE TABLE` to get column info
   - Second: Read sample data and infer schema
   - Third: If both fail, check if table has any rows
2. If schema recovery succeeds:
   - Compare recovered schema with output schema
   - If schemas match, proceed with normal write (no special options needed)
   - If schemas don't match, raise error
3. If schema recovery fails:
   - Check if table has data (count > 0)
   - If table has data but no schema, this is a catalog corruption issue
   - Log warning and try write anyway (let Spark handle it)
   - If write fails, raise appropriate error

### Step 2: Remove `overwriteSchema` for Append Mode
**File**: `src/pipeline_builder/execution.py`
**Location**: Around lines 1275-1289, 1301-1314

**Changes**:
1. Remove `overwriteSchema="true"` when `write_mode_str == "append"`
2. `overwriteSchema` only works with `overwrite` mode
3. For append mode with empty schema, rely on schema recovery + normal write

### Step 3: Update Error Handling
**File**: `src/pipeline_builder/execution.py`
**Location**: Around lines 1469-1520

**Changes**:
1. When schema validation was skipped and write fails:
   - Check if error is due to empty schema issue
   - If yes, try schema recovery one more time
   - If recovery succeeds and schemas match, this is unexpected - log and re-raise
   - If recovery fails or schemas don't match, raise appropriate error

## Detailed Implementation

### Function: `_recover_table_schema(spark, table_name)`
```python
def _recover_table_schema(spark, table_name: str) -> Optional[StructType]:
    """
    Attempt to recover table schema when catalog shows empty schema.
    
    Returns:
        Recovered schema or None if recovery fails
    """
    try:
        # Method 1: DESCRIBE TABLE
        describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")
        describe_rows = describe_df.collect()
        if describe_rows and len(describe_rows) > 0:
            # Reconstruct schema from DESCRIBE output
            fields = []
            for row in describe_rows:
                col_name = row['col_name']
                col_type = row['data_type']
                # Parse type and create StructField
                # ... implementation ...
            return StructType(fields)
    except Exception:
        pass
    
    try:
        # Method 2: Read sample data
        table_df = spark.table(table_name)
        if table_df.columns:  # Has columns
            # Read first row to infer types
            sample = table_df.limit(1).collect()
            if sample:
                # Infer schema from sample
                # ... implementation ...
                return table_df.schema
    except Exception:
        pass
    
    return None
```

### Modified Incremental Schema Validation
```python
# For incremental mode with append, validate schema matches existing table
schema_validation_skipped = False
if mode == ExecutionMode.INCREMENTAL and write_mode_str == "append":
    if table_exists(self.spark, output_table):
        try:
            existing_table_inc = self.spark.table(output_table)
            existing_schema = existing_table_inc.schema
            output_schema = output_df.schema
            
            # If schema is empty, try to recover it
            if not existing_schema.fields or len(existing_schema.fields) == 0:
                self.logger.warning(
                    f"Table '{output_table}' has empty schema in incremental mode. "
                    f"Attempting schema recovery..."
                )
                recovered_schema = _recover_table_schema(self.spark, output_table)
                
                if recovered_schema and len(recovered_schema.fields) > 0:
                    # Use recovered schema for validation
                    existing_schema = recovered_schema
                    self.logger.info(
                        f"Successfully recovered schema for '{output_table}': {recovered_schema}"
                    )
                else:
                    # Schema recovery failed - check if table has data
                    row_count = existing_table_inc.count()
                    if row_count > 0:
                        # Table has data but no schema - catalog corruption
                        self.logger.warning(
                            f"Table '{output_table}' has {row_count} rows but empty schema. "
                            f"Catalog corruption detected. Will attempt write and let Spark handle it."
                        )
                        schema_validation_skipped = True
                        # Skip validation - let Spark validate during write
                        existing_columns = {}
                        output_columns = {}
                        missing_in_output = set()
                        new_in_output = set()
                        type_mismatches = []
                    else:
                        # Table is empty - treat as new table
                        self.logger.info(
                            f"Table '{output_table}' is empty. Treating as new table."
                        )
                        schema_validation_skipped = True
                        existing_columns = {}
                        output_columns = {}
                        missing_in_output = set()
                        new_in_output = set()
                        type_mismatches = []
            
            # Normal schema validation (if not skipped)
            if not schema_validation_skipped:
                # ... existing validation logic ...
```

## Testing Strategy

1. **Test schema recovery**:
   - Create table with data
   - Corrupt catalog (simulate empty schema)
   - Verify recovery works

2. **Test incremental with recovered schema**:
   - Run initial load
   - Simulate catalog sync issue
   - Run incremental with same schema
   - Verify it succeeds

3. **Test incremental with schema mismatch**:
   - Run initial load
   - Run incremental with different schema
   - Verify it fails appropriately

## Expected Outcomes

After implementation:
- ✅ result1 (initial load) succeeds
- ✅ result2 (incremental with same schema) succeeds (even with catalog sync issues)
- ✅ result3 (incremental with different schema) fails appropriately

## Risks and Mitigations

1. **Risk**: Schema recovery might be slow
   - **Mitigation**: Cache recovered schemas, only recover when needed

2. **Risk**: DESCRIBE TABLE might not work for all table types
   - **Mitigation**: Fall back to reading sample data

3. **Risk**: Recovered schema might not match actual data schema
   - **Mitigation**: Let Spark validate during write, catch and handle errors appropriately

