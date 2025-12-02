# Directory Structure Analysis: abstracts and pipeline_builder

## Overview

This document provides a comprehensive analysis of the `abstracts/` and `pipeline_builder/` directories, their relationships, and how they work together to create a flexible, extensible pipeline framework.

## Architecture Pattern

The codebase uses an **abstract interface pattern** where:
- `abstracts/` defines **abstract base classes (ABCs) and protocols** that specify interfaces
- `pipeline_builder/` provides a **concrete Spark/PySpark implementation**
- `sql_pipeline_builder/` provides a **concrete SQL implementation**

This design allows multiple implementations to share the same interface, enabling:
- Code reuse
- Type safety via protocols
- Easy testing with mock implementations
- Future extensibility (e.g., Dask, Polars engines)

---

## `abstracts/` Directory

### Purpose
Contains abstract base classes, protocols, and interfaces that define the contract for pipeline components.

### Structure

```
src/abstracts/
├── builder.py          # Abstract PipelineBuilder base class
├── engine.py           # Engine ABC interface
├── runner.py           # Runner ABC interface  
├── rules.py            # Rules Protocol
├── source.py           # Source Protocol (compatible with DataFrame)
├── step.py             # Step Protocol (Bronze/Silver/Gold steps)
├── transformer.py      # Transformer Protocol
└── reports/
    ├── run.py          # Report Protocol for execution results
    ├── transform.py    # TransformReport dataclass
    ├── validation.py   # ValidationReport dataclass
    └── write.py        # WriteReport dataclass
```

### Key Components

#### 1. **Protocols** (Structural Typing)

These use Python's `Protocol` for structural typing (duck typing):

- **`Step` Protocol** (`step.py`)
  - Defines interface for Bronze/Silver/Gold steps
  - Properties: `name`, `type`, `rules`, `source`, `target`, `transform`, etc.
  - Satisfied by `BronzeStep`, `SilverStep`, `GoldStep` from `pipeline_builder`

- **`Source` Protocol** (`source.py`)
  - Compatible with DataFrame and any data source
  - Used for type hints throughout the system

- **`Transformer` Protocol** (`transformer.py`)
  - Compatible with transformation functions
  - Covers both Silver and Gold transform functions

- **`Rules` Protocol** (`rules.py`)
  - Compatible with `ColumnRules` (Dict[str, List[Union[str, Column]]])
  - Any dict mapping column names to rule lists satisfies this

#### 2. **Abstract Base Classes**

- **`Engine` ABC** (`engine.py`)
  - Defines interface for execution engines
  - Methods:
    - `validate_source(step, source) -> ValidationReport`
    - `transform_source(step, source) -> TransformReport`
    - `write_target(step, source) -> WriteReport`

- **`Runner` ABC** (`runner.py`)
  - Defines interface for pipeline runners
  - Methods:
    - `run_initial_load(bronze_sources) -> Report`
    - `run_incremental(bronze_sources) -> Report`

- **`PipelineBuilder` Abstract Class** (`builder.py`)
  - Abstract base class for pipeline builders
  - Uses dependency injection (takes `runner_cls` and `engine`)
  - Methods:
    - `validate_steps(steps) -> bool`
    - `to_pipeline(steps, engine) -> Runner`
    - `with_bronze_rules(...)` (raises NotImplementedError)
    - `add_silver_transform(...)` (raises NotImplementedError)
    - `add_gold_transform(...)` (raises NotImplementedError)

#### 3. **Report Types**

Located in `reports/` subdirectory:

- **`Report` Protocol** (`run.py`)
  - Interface for pipeline execution reports
  - Properties: `pipeline_id`, `execution_id`, `status`, `start_time`, `end_time`, `duration_seconds`, `errors`, `success`

- **`TransformReport`** (`transform.py`)
  - Dataclass: `source`, `error`

- **`ValidationReport`** (`validation.py`)
  - Dataclass: `source`, `valid_rows`, `invalid_rows`, `error`

- **`WriteReport`** (`write.py`)
  - Dataclass: `source`, `written_rows`, `failed_rows`, `error`

### Key Files

```12:24:src/abstracts/step.py
class Step(Protocol):
    """
    Protocol for pipeline steps that BronzeStep, SilverStep, and GoldStep naturally satisfy.

    This Protocol defines the interface that all step types must implement,
    allowing duck typing compatibility between abstracts and pipeline_builder.
    """

    name: str
    type: Literal["bronze", "silver", "gold"]
    rules: Rules
    source: Optional[str]
    target: Optional[str]
    transform: Optional[Transformer]
    write_mode: Optional[Literal["overwrite", "append"]]
    write_schema: Optional[str]
```

```12:20:src/abstracts/engine.py
class Engine(ABC):
    @abstractmethod
    def validate_source(self, step: Step, source: Source) -> ValidationReport: ...

    @abstractmethod
    def transform_source(self, step: Step, source: Source) -> TransformReport: ...

    @abstractmethod
    def write_target(self, step: Step, source: Source) -> WriteReport: ...
```

---

## `pipeline_builder/` Directory

### Purpose
Concrete implementation of the pipeline framework using Apache Spark/PySpark and Delta Lake.

### Structure

```
src/pipeline_builder/
├── __init__.py                 # Package exports
├── pipeline/
│   ├── builder.py             # Concrete PipelineBuilder (inherits from abstracts)
│   ├── runner.py              # SimplePipelineRunner (implements abstracts.Runner)
│   ├── models.py              # Pipeline models (PipelineReport, PipelineStatus, etc.)
│   └── monitor.py             # Pipeline monitoring
├── engine/
│   └── spark_engine.py        # SparkEngine (implements abstracts.Engine)
├── models/
│   ├── steps.py               # BronzeStep, SilverStep, GoldStep (satisfy Step Protocol)
│   ├── pipeline.py            # PipelineConfig, etc.
│   └── ...
├── execution.py               # ExecutionEngine (internal execution logic)
├── validation/                # Validation system
├── writer/                    # Delta Lake writer
└── ...                        # Other modules
```

### Key Components

#### 1. **Concrete Implementations**

**`SparkEngine`** (`engine/spark_engine.py`)
- Implements `abstracts.Engine`
- Wraps `ExecutionEngine` and adapts between abstract interface and concrete types
- Converts between abstract `Step`/`Source` protocols and concrete `BronzeStep`/`SilverStep`/`GoldStep`/`DataFrame`

```28:34:src/pipeline_builder/engine/spark_engine.py
class SparkEngine(Engine):
    """
    SparkEngine implements abstracts.Engine using ExecutionEngine.

    This engine adapts between the abstracts interface (Step, Source protocols)
    and the concrete pipeline_builder types (BronzeStep/SilverStep/GoldStep, DataFrame).
    """
```

**`SimplePipelineRunner`** (`pipeline/runner.py`)
- Implements `abstracts.Runner`
- Inherits from `BaseRunner` (from `pipeline_builder_base`)
- Delegates execution to `ExecutionEngine`
- Supports both abstract interface and legacy dictionary-based step storage

```41:50:src/pipeline_builder/pipeline/runner.py
class SimplePipelineRunner(BaseRunner, Runner):
    """
    Simplified pipeline runner that delegates to the execution engine.

    This runner focuses on orchestration and reporting, delegating
    actual execution to the simplified ExecutionEngine.

    Implements abstracts.Runner interface while maintaining backward compatibility
    with additional methods (run_full_refresh, run_validation_only).
    """
```

**`PipelineBuilder`** (`pipeline/builder.py`)
- Inherits from `BasePipelineBuilder` (from `pipeline_builder_base`)
- Uses `AbstractsPipelineBuilder` pattern via composition
- Provides fluent API for building pipelines
- Creates concrete `BronzeStep`, `SilverStep`, `GoldStep` instances

```64:70:src/pipeline_builder/pipeline/builder.py
class PipelineBuilder(BasePipelineBuilder):
    """
    Production-ready builder for creating data pipelines with Bronze → Silver → Gold architecture.

    The PipelineBuilder provides a fluent API for constructing robust data pipelines with
    comprehensive validation, automatic dependency management, and enterprise-grade features.
```

#### 2. **Step Models**

**`BronzeStep`, `SilverStep`, `GoldStep`** (`models/steps.py`)
- Concrete dataclasses that satisfy the `Step` Protocol
- Used throughout `pipeline_builder` for type hints
- Compatible with abstract `Step` protocol via structural typing

### Key Relationships

```27:27:src/pipeline_builder/pipeline/builder.py
from abstracts.builder import PipelineBuilder as AbstractsPipelineBuilder
```

```12:17:src/pipeline_builder/engine/spark_engine.py
from abstracts.engine import Engine
from abstracts.reports.transform import TransformReport
from abstracts.reports.validation import ValidationReport
from abstracts.reports.write import WriteReport
from abstracts.source import Source
from abstracts.step import Step
```

```22:24:src/pipeline_builder/pipeline/runner.py
from abstracts.reports.run import Report
from abstracts.runner import Runner
from abstracts.source import Source
```

---

## How They Work Together

### 1. **Type Compatibility via Protocols**

The `Step` Protocol allows concrete step classes (`BronzeStep`, `SilverStep`, `GoldStep`) to be used wherever `Step` is expected:

```python
# abstracts/step.py defines the protocol
class Step(Protocol):
    name: str
    type: Literal["bronze", "silver", "gold"]
    # ...

# pipeline_builder/models/steps.py defines concrete classes
@dataclass
class BronzeStep:
    name: str
    type: Literal["bronze"] = "bronze"
    rules: ColumnRules
    # ... naturally satisfies Step Protocol
```

### 2. **Engine Adapter Pattern**

`SparkEngine` acts as an adapter between the abstract interface and concrete implementation:

```python
# Abstract interface expects Step and Source protocols
def validate_source(self, step: Step, source: Source) -> ValidationReport:
    # SparkEngine converts protocols to concrete types
    if not isinstance(step, (BronzeStep, SilverStep, GoldStep)):
        raise TypeError(...)
    # Then delegates to ExecutionEngine
```

### 3. **Runner Implementation**

`SimplePipelineRunner` implements both:
- Abstract `Runner` interface (for compatibility)
- Legacy dictionary-based step storage (for backward compatibility)

### 4. **Builder Pattern**

`PipelineBuilder` uses the abstract interfaces for validation and type hints while providing concrete implementation:

- Inherits from `BasePipelineBuilder` (concrete base)
- Imports `AbstractsPipelineBuilder` for type compatibility
- Creates concrete step instances that satisfy abstract protocols

---

## Benefits of This Architecture

### 1. **Separation of Concerns**
- Abstracts define **what** (interfaces)
- Implementations define **how** (concrete behavior)

### 2. **Multiple Backends**
- `pipeline_builder` uses Spark/PySpark
- `sql_pipeline_builder` uses SQL
- Easy to add more (e.g., Dask, Polars)

### 3. **Type Safety**
- Protocols provide type hints without tight coupling
- Structural typing allows natural compatibility

### 4. **Testing**
- Can create mock implementations of abstract interfaces
- Easy to test components in isolation

### 5. **Extensibility**
- New implementations just need to satisfy protocols/interfaces
- No changes needed to abstract definitions

---

## Comparison with `sql_pipeline_builder/`

`sql_pipeline_builder/` follows the same pattern:

```
src/sql_pipeline_builder/
├── engine/
│   └── sql_engine.py        # SqlEngine implements abstracts.Engine
├── pipeline/
│   └── runner.py            # Implements abstracts.Runner
└── ...
```

Both implementations:
- Use the same abstract interfaces
- Can be used interchangeably where protocols match
- Share type definitions via protocols

---

## Usage Example

```python
from abstracts.engine import Engine
from abstracts.step import Step
from abstracts.source import Source
from pipeline_builder.engine import SparkEngine
from pipeline_builder.models import BronzeStep

# Abstract interface
def process_step(engine: Engine, step: Step, source: Source):
    report = engine.validate_source(step, source)
    return report

# Concrete implementation works seamlessly
spark_engine = SparkEngine(spark, config, logger)
bronze_step = BronzeStep(name="events", rules={...})
dataframe = spark.createDataFrame(...)

# All type checks pass due to protocols
result = process_step(spark_engine, bronze_step, dataframe)
```

---

## Summary

- **`abstracts/`**: Defines interfaces, protocols, and abstract base classes
- **`pipeline_builder/`**: Provides Spark/PySpark concrete implementation
- **Relationship**: Concrete classes satisfy abstract protocols via structural typing
- **Benefits**: Flexible, extensible, type-safe architecture supporting multiple backends

The architecture follows the **Strategy Pattern** and **Adapter Pattern**, allowing different execution engines (Spark, SQL, etc.) to be swapped while maintaining the same interface.

