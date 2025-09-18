# Type Safety Improvements Guide

This guide explains the comprehensive type safety improvements made to SparkForge, providing better IDE support, compile-time error detection, and improved code reliability.

## 🎯 **What Changed**

### **1. Comprehensive Type System**

**Before:** Limited type annotations with inconsistent patterns
```python
def add_silver_transform(self, name, source_bronze, transform, rules, table_name, ...):
    # No type hints
```

**After:** Comprehensive type system with rich type aliases
```python
def add_silver_transform(
    self,
    *,
    name: StepName,
    source_bronze: StepName,
    transform: SilverTransformFunction,
    rules: ColumnRules,
    table_name: TableName,
    watermark_col: Optional[str] = None,
    description: Optional[str] = None,
    depends_on: Optional[List[StepName]] = None
) -> 'PipelineBuilder':
    # Full type safety
```

### **2. Type Aliases and Utilities**

**Created:** `sparkforge/types/` package with 4 focused modules
- `aliases.py` - Comprehensive type aliases for common patterns
- `protocols.py` - Protocol definitions for better type checking
- `generics.py` - Generic types and type variables
- `utils.py` - Type utilities and validation helpers

### **3. Early Validation System**

**New:** Robust validation with type safety
```python
# Validation errors are caught during construction with type safety
@dataclass
class BronzeStep(BaseModel):
    name: str
    rules: ColumnRules
    incremental_col: str | None = None
    schema: str | None = None

    def __post_init__(self):
        """Validate required fields after initialization."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("Step name must be a non-empty string")
        if not isinstance(self.rules, dict) or not self.rules:
            raise ValidationError("Rules must be a non-empty dictionary")
```

### **3. Rich Type Definitions**

#### **Core Types**
```python
# String types
StepName = str
PipelineId = str
ExecutionId = str
TableName = str
SchemaName = str

# Numeric types
QualityRate = float
Duration = float
RowCount = int
WorkerCount = int
RetryCount = int
```

#### **Function Types**
```python
# Transform functions
TransformFunction = Callable[[DataFrame], DataFrame]
BronzeTransformFunction = Callable[[DataFrame], DataFrame]
SilverTransformFunction = Callable[[DataFrame, Dict[str, DataFrame]], DataFrame]
GoldTransformFunction = Callable[[Dict[str, DataFrame]], DataFrame]

# Validation functions
ValidationFunction = Callable[[DataFrame], bool]
FilterFunction = Callable[[DataFrame], DataFrame]
```

#### **Data Types**
```python
# Column and validation rules
ColumnRules = Dict[str, List[Any]]
ValidationRules = Dict[str, List[Any]]
QualityThresholds = Dict[str, float]

# Context types
ExecutionContext = Dict[str, Any]
StepContext = Dict[str, Any]
PipelineContext = Dict[str, Any]
```

### **4. Protocol Definitions**

#### **Core Protocols**
```python
@runtime_checkable
class Validatable(Protocol):
    def validate(self) -> None: ...

@runtime_checkable
class Serializable(Protocol):
    def to_dict(self) -> Dict[str, Any]: ...
    def to_json(self) -> str: ...

@runtime_checkable
class Executable(Protocol):
    def execute(self) -> Any: ...

@runtime_checkable
class Monitorable(Protocol):
    def get_metrics(self) -> Dict[str, Any]: ...
    def get_status(self) -> str: ...
```

#### **Pipeline Protocols**
```python
@runtime_checkable
class PipelineStep(Protocol):
    name: StepName
    step_type: StepType

    def execute(self, context: Dict[str, Any]) -> StepResult: ...
    def validate(self) -> ValidationResult: ...

@runtime_checkable
class PipelineValidator(Protocol):
    def validate_pipeline(self, pipeline: Any) -> ValidationResult: ...
    def validate_step(self, step: PipelineStep) -> ValidationResult: ...
```

### **5. Generic Types**

#### **Result Types**
```python
class Result(Generic[T]):
    def __init__(self, value: T, success: bool = True, error: Optional[str] = None):
        self.value = value
        self.success = success
        self.error = error

class OptionalResult(Generic[T]):
    def __init__(self, value: Optional[T] = None, error: Optional[str] = None):
        self.value = value
        self.error = error

class Either(Generic[L, R]):
    def __init__(self, left: Optional[L] = None, right: Optional[R] = None):
        # Either left or right, but not both
```

#### **Type-Safe Collections**
```python
class TypedDict(Generic[K, V], Dict[K, V]):
    def get_typed(self, key: K, default: V = None) -> V: ...
    def set_typed(self, key: K, value: V) -> None: ...

class TypedList(Generic[T], List[T]):
    def append_typed(self, item: T) -> None: ...
    def get_typed(self, index: int) -> T: ...
```

### **6. Type Utilities**

#### **Type Checking**
```python
def is_valid_type(value: Any, expected_type: Type[T]) -> bool:
    """Check if value is valid for expected type."""

def validate_type(value: Any, expected_type: Type[T], name: str = "value") -> T:
    """Validate value against expected type."""

def get_type_hints(obj: Any) -> Dict[str, Any]:
    """Get type hints for object."""
```

#### **Type Conversion**
```python
def cast_safe(value: Any, target_type: Type[T]) -> Optional[T]:
    """Safely cast value to target type."""

def convert_type(value: Any, target_type: Type[T]) -> T:
    """Convert value to target type, raising error if fails."""

def normalize_type(value: Any, target_type: Type[T]) -> T:
    """Normalize value to target type with fallback."""
```

#### **Type Inference**
```python
def infer_type(value: Any) -> Type:
    """Infer type of value."""

def infer_return_type(func: callable) -> Type:
    """Infer return type of function."""

def infer_parameter_types(func: callable) -> Dict[str, Type]:
    """Infer parameter types of function."""
```

## 📚 **Usage Examples**

### **1. Basic Type Usage**

```python
from sparkforge import (
    PipelineBuilder, StepName, StepType, ColumnRules,
    TransformFunction, SilverTransformFunction, GoldTransformFunction
)

# Type-safe pipeline building
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Type-safe step names
step_name: StepName = "bronze_events"
source_bronze: StepName = "raw_events"

# Type-safe column rules
rules: ColumnRules = {
    "user_id": [F.col("user_id").isNotNull()],
    "timestamp": [F.col("timestamp").isNotNull()]
}

# Type-safe transform functions
def silver_transform(spark: SparkSession, df: DataFrame, silvers: Dict[str, DataFrame]) -> DataFrame:
    return df.filter(F.col("status") == "active")

silver_func: SilverTransformFunction = silver_transform
```

### **2. Protocol Usage**

```python
from sparkforge.types import Validatable, Serializable, Executable

class MyStep(Validatable, Serializable, Executable):
    def validate(self) -> None:
        # Validation logic
        pass

    def to_dict(self) -> Dict[str, Any]:
        # Serialization logic
        return {}

    def execute(self) -> Any:
        # Execution logic
        return None

# Type checking
def process_step(step: Validatable) -> None:
    step.validate()  # IDE knows this method exists
```

### **3. Generic Types**

```python
from sparkforge.types import Result, OptionalResult, Either, Try

# Result type
result: Result[str] = Result("success", success=True)
if result:
    print(f"Success: {result.value}")

# Optional result
opt_result: OptionalResult[int] = OptionalResult(value=42)
value = opt_result.unwrap_or(0)

# Either type
either: Either[str, int] = Either(left="error")
if either.is_left:
    print(f"Error: {either.left}")

# Try type
try_result: Try[int] = Try(value=42)
mapped = try_result.map(lambda x: x * 2)
```

### **4. Type-Safe Collections**

```python
from sparkforge.types import TypedDict, TypedList

# Type-safe dictionary
string_dict: TypedDict[str, str] = TypedDict(str)
string_dict.set_typed("key", "value")  # Type-safe

# Type-safe list
int_list: TypedList[int] = TypedList(int)
int_list.append_typed(42)  # Type-safe
```

### **5. Type Validation Decorators**

```python
from sparkforge.types import typed, typed_method

@typed
def process_data(data: DataFrame, threshold: float) -> bool:
    return data.count() > threshold

@typed_method
def validate_step(self, step_name: StepName, rules: ColumnRules) -> bool:
    return len(rules) > 0
```

### **6. Type Utilities**

```python
from sparkforge.types import (
    is_valid_type, validate_type, cast_safe,
    infer_type, get_type_hints
)

# Type checking
if is_valid_type("hello", str):
    print("Valid string")

# Type validation
value = validate_type(42, int, "count")

# Safe casting
result = cast_safe("42", int)  # Returns 42
result = cast_safe("hello", int)  # Returns None

# Type inference
value_type = infer_type(42)  # Returns int
func_hints = get_type_hints(process_data)
```

## 🔧 **Type Safety Features**

### **1. IDE Support**
- **Autocomplete**: Full autocomplete for all type aliases
- **Type hints**: Rich type information in IDE
- **Error detection**: Compile-time type error detection
- **Refactoring**: Safe refactoring with type checking

### **2. Runtime Validation**
- **Type checking**: Runtime type validation with decorators
- **Type conversion**: Safe type conversion utilities
- **Error handling**: Type-safe error handling patterns
- **Validation**: Comprehensive input validation

### **3. Generic Programming**
- **Type variables**: Flexible generic type definitions
- **Protocols**: Interface-based programming
- **Result types**: Functional programming patterns
- **Collections**: Type-safe collection classes

### **4. Error Prevention**
- **Compile-time checks**: Catch errors before runtime
- **Type safety**: Prevent type-related bugs
- **Interface compliance**: Ensure protocol implementation
- **Validation**: Input validation and sanitization

## 📊 **Benefits**

### **1. Developer Experience**
- **Better IDE support** with autocomplete and type hints
- **Faster development** with compile-time error detection
- **Easier debugging** with clear type information
- **Safer refactoring** with type checking

### **2. Code Quality**
- **Type safety** prevents runtime type errors
- **Better documentation** with self-documenting types
- **Consistent patterns** across all modules
- **Maintainable code** with clear interfaces

### **3. Performance**
- **No runtime overhead** for type annotations
- **Optimized type checking** with efficient utilities
- **Lazy evaluation** for type inference
- **Cached type information** for better performance

### **4. Reliability**
- **Compile-time validation** catches errors early
- **Type-safe operations** prevent common bugs
- **Interface compliance** ensures proper implementation
- **Comprehensive validation** with type utilities

## 🚀 **Best Practices**

### **1. Use Type Aliases**
```python
# Good: Use type aliases
def process_step(step_name: StepName, rules: ColumnRules) -> StepResult:
    pass

# Bad: Use raw types
def process_step(step_name: str, rules: Dict[str, List[Any]]) -> Dict[str, Any]:
    pass
```

### **2. Implement Protocols**
```python
# Good: Implement protocols
class MyStep(Validatable, Serializable):
    def validate(self) -> None: ...
    def to_dict(self) -> Dict[str, Any]: ...

# Bad: Don't implement protocols
class MyStep:
    def validate(self) -> None: ...
    def to_dict(self) -> Dict[str, Any]: ...
```

### **3. Use Generic Types**
```python
# Good: Use generic types
def process_result(result: Result[str]) -> str:
    return result.value if result else ""

# Bad: Use raw types
def process_result(result: Any) -> str:
    return result.value if result else ""
```

### **4. Validate Types**
```python
# Good: Validate types
def process_data(data: DataFrame) -> bool:
    validate_type(data, DataFrame, "data")
    return data.count() > 0

# Bad: Don't validate types
def process_data(data: DataFrame) -> bool:
    return data.count() > 0
```

The type safety improvements make SparkForge more robust, maintainable, and developer-friendly while providing excellent IDE support and compile-time error detection.
