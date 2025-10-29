# Builder Tests

This directory contains system-level tests for the PipelineBuilder, PipelineRunner, and LogWriter integration using mock-spark for fast, reliable testing.

## Overview

These tests verify that the core pipeline components work together correctly in realistic scenarios, including:

- Pipeline construction using the PipelineBuilder API
- Pipeline execution with different modes (initial, incremental, full refresh)
- LogWriter integration for comprehensive logging
- The new `table_total_rows` metric functionality
- Various bronze/silver/gold layer combinations

## Test Files

### `test_pipeline_builder_integration.py`
Tests the core PipelineBuilder API patterns:
- Simple bronze → silver → gold pipeline
- Incremental load functionality
- Multiple bronze sources
- Multiple silver transforms
- Pipeline validation

### `test_logwriter_integration.py`
Tests LogWriter integration with pipeline execution:
- Basic write functionality
- Table total rows metric tracking
- Multiple execution runs
- Error handling
- Performance metrics

### `test_pipeline_combinations.py`
Tests different layer combinations:
- Bronze-only pipelines (validation only)
- Bronze → Silver pipelines
- Bronze → Gold pipelines (skipping silver)
- Full Bronze → Silver → Gold pipelines
- Multiple steps at each layer

### `test_table_total_rows_metric.py`
Tests the new `table_total_rows` metric:
- Initial load tracking
- Incremental load accumulation
- Full refresh tracking
- Multiple table scenarios
- Error handling
- Validation-only mode
- Large dataset handling

## Key Patterns

### Pipeline Construction
```python
# Create builder
builder = PipelineBuilder(spark=spark_session, schema="test_schema")

# Add bronze step
builder.with_bronze_rules(
    name="events",
    rules={"id": [F.col("id").isNotNull()]},
    incremental_col="timestamp"  # Optional
)

# Add silver step
builder.add_silver_transform(
    name="clean_events",
    source_bronze="events",
    transform=lambda spark, df, silvers: df.withColumn("processed", F.lit(True)),
    rules={"id": [F.col("id").isNotNull()]},
    table_name="clean_events",
)

# Add gold step
builder.add_gold_transform(
    name="event_summary",
    transform=lambda spark, silvers: silvers["clean_events"].groupBy("id").count(),
    rules={"id": [F.col("id").isNotNull()]},
    table_name="event_summary",
    source_silvers=["clean_events"],
)

# Build and execute
pipeline = builder.to_pipeline()
report = pipeline.run_initial_load(bronze_sources={"events": data_df})
```

### Transform Function Signatures
- **Silver transforms**: `lambda spark, df, silvers: ...` (3 parameters)
- **Gold transforms**: `lambda spark, silvers: ...` (2 parameters)

### LogWriter Usage
```python
# Create LogWriter
log_writer = LogWriter(spark=spark_session, schema="test_schema", table_name="pipeline_logs")

# Write execution results
result = log_writer.write_execution_result(
    execution_result=execution_result,
    run_id="run_001",
    run_mode="initial"
)

# Or write log rows directly
log_writer.write_log_rows([log_row], run_id="run_001")
```

### Table Total Rows Metric
The new `table_total_rows` metric tracks the total number of rows in destination tables after write operations:

```python
log_row: LogRow = {
    # ... other fields ...
    "table_total_rows": 150,  # Total rows in destination table
    "rows_written": 10,       # Rows written in this operation
    "write_mode": "append",   # Write mode used
    # ... other fields ...
}
```

## Running Tests

### Run All Builder Tests
```bash
python builder_tests/run_tests.py
```

### Run Specific Test File
```bash
pytest builder_tests/test_pipeline_builder_integration.py -v
```

### Run Specific Test
```bash
pytest builder_tests/test_pipeline_builder_integration.py::TestPipelineBuilderIntegration::test_simple_bronze_silver_gold_pipeline -v
```

## Test Data

Tests use simple, focused datasets rather than complex real-world data:
- **Simple events data**: 5 rows with id, name, timestamp, value
- **Simple users data**: 3 rows with user_id, username, email, created_at
- **Incremental data**: Small datasets for testing incremental loads

## Fixtures

### Core Fixtures
- `spark_session`: MockSparkSession for testing
- `pipeline_builder`: PipelineBuilder instance
- `log_writer`: LogWriter instance
- `test_schema`: Test schema name

### Data Fixtures
- `simple_events_data`: Basic events dataset
- `simple_users_data`: Basic users dataset
- `simple_events_schema`: Schema for events data
- `simple_users_schema`: Schema for users data

### Helper Functions
- `create_incremental_events_data()`: Generate incremental test data
- `create_bronze_validation_rules()`: Standard bronze validation rules
- `create_silver_validation_rules()`: Standard silver validation rules
- `create_gold_validation_rules()`: Standard gold validation rules

## Mock Spark Integration

All tests use mock-spark instead of real PySpark for:
- **Speed**: Tests run much faster
- **Reliability**: No external dependencies
- **Isolation**: Tests don't affect each other
- **Consistency**: Predictable behavior across environments

## Key Features Tested

### Pipeline Execution Modes
- **Initial load**: `pipeline.run_initial_load(bronze_sources)`
- **Incremental load**: `pipeline.run_incremental(bronze_sources)`
- **Full refresh**: `pipeline.run_initial_load(bronze_sources)` (overwrites existing data)
- **Validation only**: `pipeline.run_pipeline(mode="validation_only", bronze_sources)`

### Write Modes
- **Overwrite**: Replaces entire table (initial load, full refresh)
- **Append**: Adds to existing table (incremental load)
- **None**: No write operation (validation only, bronze steps)

### Metrics Tracking
- **Input rows**: Rows processed from source
- **Output rows**: Rows produced by transformation
- **Rows written**: Rows written to destination table
- **Table total rows**: Total rows in destination table after write
- **Validation rate**: Percentage of rows that passed validation
- **Performance metrics**: Memory usage, CPU usage, duration

## Error Scenarios

Tests cover various error scenarios:
- **Validation failures**: Invalid data that fails validation rules
- **Transform errors**: Errors in transformation functions
- **Table operation failures**: Errors when writing to tables
- **Pipeline validation errors**: Invalid pipeline configurations

## Performance Considerations

Tests are designed to be fast and efficient:
- Small datasets (5-100 rows typically)
- Simple transformations
- Mock-spark for speed
- Focused test cases
- Minimal setup/teardown

## Contributing

When adding new tests:
1. Follow the established patterns
2. Use simple, focused test data
3. Test specific functionality
4. Include both success and error scenarios
5. Verify metrics are correctly tracked
6. Use descriptive test names
7. Add appropriate assertions