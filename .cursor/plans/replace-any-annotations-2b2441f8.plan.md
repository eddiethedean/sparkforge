<!-- 2b2441f8-401f-4a7e-b536-0e05b181e055 ffffae7b-80d5-4c6f-8283-f7ead408df8f -->
# Replace Any Type Annotations with Proper Typing

## Phase 1: Create TypedDict Definitions

### 1.1 Reporting TypedDicts (`pipeline_builder/reporting.py`)

Create TypedDict classes for report structures:

- `ValidationReport` - for `create_validation_dict` return type
- `TransformReport` - for `create_transform_dict` return type  
- `WriteReport` - for `create_write_dict` return type
- `SummaryReport` - for `create_summary_report` return type (nested structure with execution_summary, performance_metrics, data_metrics)

### 1.2 Monitoring TypedDicts (`pipeline_builder/writer/monitoring.py`)

Create TypedDict classes for monitoring structures:

- `OperationMetrics` - for operation tracking (lines 84, 128-136)
- `MemoryUsageInfo` - for `get_memory_usage` return type (line 169)
- `PerformanceTrends` - for `analyze_execution_trends` return type (line 271)
- `AnomalyReport` - for `detect_anomalies` return type (line 339)
- `PerformanceReport` - for `generate_performance_report` return type (line 416)

### 1.3 Analytics TypedDicts (`pipeline_builder/writer/analytics.py`)

Create TypedDict classes:

- `QualityTrends` - for `analyze_quality_trends` return type (line 39)
- `QualityAnomalies` - for `detect_quality_anomalies` return type (line 136)
- `ExecutionTrends` - for `analyze_execution_trends` return type (line 308)
- `VolumeTrends` - for `_analyze_volume_trends` return type (line 408)

### 1.4 Storage TypedDicts (`pipeline_builder/writer/storage.py`)

Create TypedDict classes:

- `WriteResult` - for write operation results (lines 106, 179)
- `OptimizeResult` - for `optimize_table` return type (line 203)
- `VacuumResult` - for `vacuum_table` return type (line 260)
- `TableInfo` - for `get_table_info` return type (line 315)

### 1.5 Operations TypedDicts (`pipeline_builder/writer/operations.py`)

Create TypedDict classes:

- `DataQualityReport` - for `validate_data_quality` return type (line 234)

### 1.6 Core Writer TypedDicts (`pipeline_builder/writer/core.py`)

Create TypedDict classes:

- `LogWriteResult` - for log write operation results (lines 331, 419, 490, 554)
- `TableInfoResult` - for `get_table_info` return type (line 658)
- `OptimizeResult` - for `optimize_table` return type (line 671)
- `VacuumResult` - for `vacuum_table` return type (line 685)
- Various analysis result types reusing from analytics module

### 1.7 Update types.py

Replace generic type aliases with specific TypedDict classes:

- Remove `StepResult = Dict[str, Any]` - replace with specific TypedDict per use case
- Remove `PipelineResult = Dict[str, Any]` - create `PipelineResultDict` TypedDict
- Remove `ExecutionResultDict = Dict[str, Any]` - already exists in models.execution
- Remove `ValidationResultDict = Dict[str, Any]` - create specific TypedDict
- Remove `StepContext = Dict[str, Any]` - create `StepContextDict` TypedDict
- Remove `ExecutionContext = Dict[str, Any]` - already exists in models
- Remove config type aliases, create proper TypedDict classes
- Remove `ErrorContext = Dict[str, Any]` - create `ErrorContextDict` TypedDict

## Phase 2: Replace Dict[str, Any] Return Types

### 2.1 Update reporting.py

Replace all 4 function return types with new TypedDict classes

### 2.2 Update writer/monitoring.py  

Replace 5 method return types with new TypedDict classes

### 2.3 Update writer/analytics.py

Replace 4 method return types with new TypedDict classes

### 2.4 Update writer/storage.py

Replace 4 method return types with new TypedDict classes

### 2.5 Update writer/operations.py

Replace 1 method return type with new TypedDict class

### 2.6 Update writer/core.py

Replace 13 method return types with new TypedDict classes

### 2.7 Update writer/query_builder.py

Replace 4 static method return types (lines 68, 120, 139, 159) - use specific dict types like `Dict[str, str]` or `Dict[str, Column]`

## Phase 3: Replace Any in Function Signatures

### 3.1 Update table_operations.py

Replace `**options: Any` with `**options: str | int | float | bool` (lines 45, 83)

### 3.2 Update performance.py

- Line 56: Use `TypeVar` and generics for decorator return types instead of `Any`
- Line 101: Replace `**options: Any` with specific type
- Line 165: Use generics for wrapper return type

### 3.3 Update writer/core.py

- Line 40: Replace function signature `operation_func: Any, *args: Any, **kwargs: Any` with proper Callable type
- Line 41: Replace return type `tuple[int, float, Any, Any]` with specific types using TypeVar if needed

### 3.4 Update logging.py

Replace `**kwargs: Any` with `**kwargs: str | int | float | bool | None` (lines 76, 80, 84, 88, 92) and `kwargs: Dict[str, Any]` with specific type (line 96)

### 3.5 Update pipeline/builder.py

- Line 440: Replace `validator: Any` with `Validatable` protocol or specific validator type
- Lines 823, 859, 895: Replace `**kwargs: Any` with specific type union
- Line 1047: Replace `df_schema: Any` with proper schema type from compat

## Phase 4: Replace Any in Compatibility Layer

### 4.1 Update compat.py

- Lines 22, 41, 66: Replace `tuple[type[Any], type[Any], type[Any], Any, Any, type[Exception]]` with proper types - use specific spark/mock types
- Lines 125, 134, 139, 144: Replace function return `Any` with `Column` type
- Lines 163, 169: Replace `*cols: Any` with `Column | str` and return type with proper Window spec type

### 4.2 Update functions.py

Replace `Any` in Protocol methods (lines 29, 33) with `Column` type

## Phase 5: Update Metadata and Context Dictionaries

### 5.1 Create MetadataDict TypedDict

Replace `metadata: Dict[str, Any] | None` throughout with proper `MetadataDict` TypedDict allowing common metadata fields

### 5.2 Update writer/models.py

Line 114: Update `metadata: Dict[str, Any]` with `MetadataDict`
Lines 359, 423: Update metadata parameter types

### 5.3 Update writer/exceptions.py

Lines 24, 65, 93, 122, 157, 193, 227: Update `context: Dict[str, Any]` with proper `ErrorContextDict`

### 5.4 Update models files

- `models/pipeline.py` line 198: Replace `step_results: list[Any]` with proper type
- `models/execution.py` line 57: Replace `config: Dict[str, Any]` with proper config TypedDict
- `models/dependencies.py` line 93: Replace `config: Dict[str, Any]` with proper config TypedDict

## Phase 6: Update Remaining Complex Cases

### 6.1 Update dependencies module

- `dependencies/graph.py` line 38: Replace `metadata: Dict[str, Any]` with proper TypedDict
- `dependencies/graph.py` line 204: Replace return type with proper stats TypedDict
- `dependencies/analyzer.py` line 45: Replace `stats: Dict[str, Any]` with proper TypedDict

### 6.2 Update validation module

- `validation/utils.py` line 34: Create `DataFrameInfo` TypedDict for return type
- `validation/data_validation.py` line 250: Create proper return TypedDict
- `validation/pipeline_validation.py` line 29: Replace `step: Any` with proper Union of step types

### 6.3 Update pipeline/monitor.py

Lines 39-41: Replace `Dict[str, Any]` with proper step dictionary types

### 6.4 Update errors.py

Line 103: Replace return type `Dict[str, Any]` with proper error dict TypedDict

## Phase 7: Testing and Validation

### 7.1 Run mypy

Verify all type annotations are correct and no new errors introduced

### 7.2 Run tests

Ensure all tests pass with new type annotations

### 7.3 Check for remaining Any

Search for any missed `Any` annotations and address them

### To-dos

- [ ] Create TypedDict definitions for reporting structures in reporting.py
- [ ] Create TypedDict definitions for monitoring structures in writer/monitoring.py
- [ ] Create TypedDict definitions for analytics structures in writer/analytics.py
- [ ] Create TypedDict definitions for storage structures in writer/storage.py
- [ ] Create TypedDict definitions for operations structures in writer/operations.py
- [ ] Create TypedDict definitions for core writer structures in writer/core.py
- [ ] Replace generic Any-based type aliases in types.py with specific TypedDict classes
- [ ] Update all function return types in reporting.py to use new TypedDict classes
- [ ] Update all method return types in writer/monitoring.py to use new TypedDict classes
- [ ] Update all method return types in writer/analytics.py to use new TypedDict classes
- [ ] Update all method return types in writer/storage.py to use new TypedDict classes
- [ ] Update method return types in writer/operations.py to use new TypedDict classes
- [ ] Update all method return types in writer/core.py to use new TypedDict classes
- [ ] Update static method return types in writer/query_builder.py with specific dict types
- [ ] Replace **options: Any with specific type union in table_operations.py
- [ ] Use TypeVar and generics for decorator return types in performance.py
- [ ] Replace Any in function signatures in writer/core.py with proper Callable types
- [ ] Replace **kwargs: Any with specific type union in logging.py
- [ ] Replace Any parameters in pipeline/builder.py with specific types
- [ ] Replace all Any types in compat.py with proper Column and type unions
- [ ] Replace Any in Protocol methods in functions.py with Column type
- [ ] Create MetadataDict TypedDict for common metadata patterns
- [ ] Update metadata type annotations in writer/models.py
- [ ] Update context dictionaries in writer/exceptions.py with ErrorContextDict
- [ ] Replace Any in models files with proper types
- [ ] Replace Any in dependencies module with proper TypedDict classes
- [ ] Replace Any in validation module with proper TypedDict classes
- [ ] Replace Dict[str, Any] in pipeline/monitor.py with proper step types
- [ ] Replace return type in errors.py with proper error dict TypedDict
- [ ] Run mypy to verify all type annotations are correct
- [ ] Run all tests to ensure nothing broke with new type annotations
- [ ] Search for any remaining Any annotations and address them