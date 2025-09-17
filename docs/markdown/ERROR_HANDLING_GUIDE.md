# Error Handling Standardization Guide

This guide explains the comprehensive error handling system implemented in SparkForge, providing consistent error handling patterns across all modules.

## 🎯 **What Changed**

### **1. Hierarchical Exception Structure**

**Before:** Scattered exception types with inconsistent patterns
```python
# Inconsistent error handling
raise ValueError("Invalid configuration")
raise RuntimeError("Execution failed")
raise Exception("Unknown error")
```

**After:** Standardized exception hierarchy
```python
# Consistent error handling
raise PipelineConfigurationError("Invalid configuration", suggestions=["Check config file"])
raise PipelineExecutionError("Execution failed", step_name="bronze_events")
raise StepError("Step failed", step_name="silver_clean", step_type="silver")
```

### **2. Rich Error Context**

**Before:** Basic error messages
```python
raise ValueError("Step validation failed")
```

**After:** Rich error context with suggestions
```python
raise StepValidationError(
    "Step validation failed",
    step_name="bronze_events",
    step_type="bronze",
    validation_errors=["user_id is null", "timestamp is invalid"],
    suggestions=["Check data quality", "Review validation rules"],
    context={"quality_rate": 85.5, "threshold": 95.0}
)
```

### **3. Early Validation System**

**New:** Robust validation with early error detection
```python
# Validation errors are caught during construction
try:
    BronzeStep(name="events", rules={})  # Empty rules
except ValidationError as e:
    print(f"Validation failed: {e}")
    # Output: "Rules must be a non-empty dictionary"
```

### **4. Error Categories and Severity**

All errors are now categorized and have severity levels:

| Category | Description | Severity | Examples |
|----------|-------------|----------|----------|
| `VALIDATION` | Early validation failures | HIGH | Empty rules, invalid transforms |
| `CONFIGURATION` | Configuration issues | HIGH | Invalid schema, missing parameters |
| `VALIDATION` | Data validation failures | MEDIUM | Quality below threshold, schema mismatch |
| `EXECUTION` | Execution failures | HIGH | Step execution failed, timeout |
| `DATA_QUALITY` | Data quality issues | MEDIUM | Quality rate below threshold |
| `RESOURCE` | Resource problems | HIGH | Memory issues, connection failures |
| `SYSTEM` | System-level errors | CRITICAL | Network failures, storage issues |

## 🏗️ **Exception Hierarchy**

```
SparkForgeError (base)
├── ConfigurationError
├── ValidationError
├── ExecutionError
├── DataQualityError
├── ResourceError
├── PipelineError
│   ├── PipelineConfigurationError
│   ├── PipelineExecutionError
│   └── PipelineValidationError
├── StepError
│   ├── StepExecutionError
│   └── StepValidationError
├── DependencyError
│   ├── CircularDependencyError
│   └── InvalidDependencyError
└── SystemError
    ├── ResourceError
    ├── ConfigurationError
    ├── NetworkError
    └── StorageError
```

## 📚 **Usage Examples**

### **1. Basic Error Handling**

```python
from sparkforge import PipelineBuilder, PipelineConfigurationError, StepError

try:
    builder = PipelineBuilder(spark=None, schema="")  # Invalid
except PipelineConfigurationError as e:
    print(f"Configuration error: {e}")
    print(f"Suggestions: {e.suggestions}")
    print(f"Context: {e.context}")
```

### **2. Step Error Handling**

```python
from sparkforge import StepError, DependencyError

try:
    # Add step with invalid dependency
    builder.add_silver_transform(
        name="clean_events",
        source_bronze="nonexistent_bronze",  # Invalid
        transform=lambda spark, df, silvers: df,
        rules={},
        table_name="clean_events"
    )
except DependencyError as e:
    print(f"Dependency error: {e}")
    print(f"Step: {e.step_name}")
    print(f"Dependency: {e.dependency_name}")
    print(f"Suggestions: {e.suggestions}")
```

### **3. Error Context and Recovery**

```python
from sparkforge.errors import (
    StepExecutionError,
    is_recoverable_error,
    should_retry_error,
    get_error_suggestions
)

try:
    # Execute step
    result = pipeline.execute_step("bronze_events")
except StepExecutionError as e:
    print(f"Step execution failed: {e}")

    # Check if error is recoverable
    if is_recoverable_error(e):
        print("Error is recoverable, considering retry")

        # Check if should retry
        if should_retry_error(e, retry_count=0, max_retries=3):
            print("Retrying step execution")
            # Retry logic here
        else:
            print("Max retries exceeded")
    else:
        print("Error is not recoverable, manual intervention required")

    # Get suggestions
    suggestions = get_error_suggestions(e)
    print(f"Suggestions: {suggestions}")
```

### **4. Error Decorators**

```python
from sparkforge.errors import handle_errors, PipelineExecutionError

@handle_errors(
    error_type=PipelineExecutionError,
    message="Pipeline execution failed",
    suggestions=["Check pipeline configuration", "Review step definitions"]
)
def execute_pipeline(pipeline, sources):
    # Pipeline execution logic
    pass
```

## 🔧 **Error Handling Utilities**

### **1. Error Context Creation**

```python
from sparkforge.errors import create_error_context

context = create_error_context(
    step_name="bronze_events",
    step_type="bronze",
    pipeline_id="pipeline_123",
    table_name="raw_events",
    quality_rate=85.5
)
```

### **2. Error Logging**

```python
from sparkforge.errors import log_error
import logging

logger = logging.getLogger(__name__)

try:
    # Some operation
    pass
except PipelineExecutionError as e:
    log_error(e, logger)  # Logs with appropriate level
```

### **3. Error Formatting**

```python
from sparkforge.errors import format_error_message

error = StepExecutionError("Step failed", step_name="bronze_events")
formatted = format_error_message(error)
print(formatted)  # "[bronze_events] Step failed | Context: step_type=bronze | Suggestions: Check step configuration"
```

## 📊 **Error Monitoring and Reporting**

### **1. Error Metrics**

```python
from sparkforge.errors import ErrorSeverity, ErrorCategory

# Track error metrics
error_counts = {
    ErrorSeverity.CRITICAL: 0,
    ErrorSeverity.HIGH: 0,
    ErrorSeverity.MEDIUM: 0,
    ErrorSeverity.LOW: 0
}

error_categories = {
    ErrorCategory.CONFIGURATION: 0,
    ErrorCategory.VALIDATION: 0,
    ErrorCategory.EXECUTION: 0,
    # ... etc
}
```

### **2. Error Serialization**

```python
# Convert error to dictionary for logging/monitoring
error_dict = error.to_dict()
print(error_dict)
# {
#     "error_type": "StepExecutionError",
#     "message": "Step failed",
#     "error_code": "STEP_EXECUTION_ERROR",
#     "category": "execution",
#     "severity": "high",
#     "context": {...},
#     "suggestions": [...],
#     "timestamp": "2024-01-01T12:00:00",
#     "cause": "Original error message"
# }
```

## 🚀 **Best Practices**

### **1. Use Specific Exception Types**

```python
# Good: Specific exception type
raise StepExecutionError("Step failed", step_name="bronze_events")

# Bad: Generic exception
raise Exception("Step failed")
```

### **2. Provide Rich Context**

```python
# Good: Rich context
raise DataQualityError(
    "Quality below threshold",
    quality_rate=85.5,
    threshold=95.0,
    table_name="bronze_events",
    suggestions=["Check data source", "Review validation rules"]
)

# Bad: Minimal context
raise ValueError("Quality too low")
```

### **3. Include Recovery Suggestions**

```python
# Good: Helpful suggestions
raise ConfigurationError(
    "Invalid schema name",
    suggestions=[
        "Check schema name spelling",
        "Ensure schema exists in database",
        "Verify database connection"
    ]
)

# Bad: No suggestions
raise ValueError("Invalid schema")
```

### **4. Handle Errors Gracefully**

```python
# Good: Graceful error handling
try:
    result = pipeline.execute_step(step_name)
except StepExecutionError as e:
    logger.error(f"Step {step_name} failed: {e}")
    if is_recoverable_error(e):
        # Attempt recovery
        pass
    else:
        # Report and stop
        raise

# Bad: Silent failure
try:
    result = pipeline.execute_step(step_name)
except:
    pass  # Silent failure
```

## 📈 **Benefits**

### **1. Better Debugging**
- **Rich error context** with step names, types, and metadata
- **Clear error messages** with specific failure reasons
- **Recovery suggestions** for common issues
- **Structured error data** for monitoring and analysis

### **2. Improved Reliability**
- **Consistent error handling** across all modules
- **Proper error categorization** for targeted responses
- **Recovery mechanisms** for transient failures
- **Error monitoring** and alerting capabilities

### **3. Enhanced Developer Experience**
- **Clear error messages** that explain what went wrong
- **Actionable suggestions** for fixing issues
- **Consistent API** for error handling across modules
- **Better IDE support** with specific exception types

### **4. Production Readiness**
- **Error monitoring** and metrics collection
- **Graceful degradation** for non-critical failures
- **Retry mechanisms** for transient errors
- **Comprehensive logging** for troubleshooting

The standardized error handling system makes SparkForge more robust, debuggable, and production-ready while providing a better developer experience.
