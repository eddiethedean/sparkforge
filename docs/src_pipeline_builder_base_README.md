# Pipeline Builder Base

Shared base package for pipeline builders implementing the Medallion Architecture pattern.

This package contains code shared between:
- `pipeline_builder` - Spark/Delta Lake implementation
- `sql_pipeline_builder` - SQLAlchemy implementation

## Contents

### Core Components

- **Models**: BaseModel, PipelineConfig, ValidationThresholds, ParallelConfig, ExecutionResult, StepResult, etc.
- **Dependencies**: DependencyAnalyzer, DependencyGraph for analyzing pipeline dependencies
- **Logging**: PipelineLogger for consistent logging across implementations
- **Errors**: Error classes (SparkForgeError, ValidationError, etc.) with context builders
- **Reporting**: Report TypedDicts and utilities
- **Writer**: Base LogWriter class for unified logging interface

### New Utilities (v2.3+)

- **Validation Framework**: `PipelineValidator`, `StepValidator` for common validation logic
- **Builder Utilities**: `BasePipelineBuilder` with common builder patterns
- **Runner Utilities**: `BaseRunner` with common execution patterns
- **Configuration Factories**: Factory functions for development/production/test configs
- **Step Management**: `StepManager` and step utility functions
- **Error Context**: Structured error context and suggestion generators

## Installation

```bash
pip install pipeline-builder-base
```

## Usage

### For Existing Implementations

This package is typically used as a dependency by `pipeline_builder` and `sql_pipeline_builder`. 
You generally don't import from it directly unless you're building a custom pipeline builder implementation.

### For New Implementations

If you're creating a new engine type (e.g., a third or fourth package), see [IMPLEMENTATION_GUIDE.md](IMPLEMENTATION_GUIDE.md) for detailed instructions on using the base classes and utilities.

### Quick Example

```python
from pipeline_builder_base import (
    BasePipelineBuilder,
    create_development_config,
    PipelineValidator,
)

class MyPipelineBuilder(BasePipelineBuilder):
    def __init__(self, engine, schema: str):
        config = create_development_config(schema)
        super().__init__(config)
        self.engine = engine
    
    def with_bronze_rules(self, name: str, rules: Dict):
        self._check_duplicate_step_name(name, "bronze")
        # Add your step logic here
        return self
```

## Dependencies

This package has no external dependencies - it's pure Python.

## Benefits

- **Reduced Duplication**: Eliminate ~40-50% of duplicated code between implementations
- **Consistent Behavior**: All implementations use same validation and error handling
- **Faster Development**: Focus on engine-specific logic, not boilerplate
- **Better Maintainability**: Fix bugs once, benefit everywhere

