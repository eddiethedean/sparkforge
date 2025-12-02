# Implementation Guide for New Engine Types

This guide explains how to use `pipeline_builder_base` to implement a new pipeline builder engine type (e.g., a third or fourth package).

## Overview

`pipeline_builder_base` provides a comprehensive set of utilities, validators, and base classes that make implementing new engine types much easier. By using these shared components, you can focus on engine-specific logic rather than duplicating common functionality.

## Key Components

### 1. Validation Framework

Use `PipelineValidator` and `StepValidator` for common validation logic:

```python
from pipeline_builder_base import PipelineValidator, StepValidator

validator = PipelineValidator(logger)
errors = validator.validate_pipeline(config, bronze_steps, silver_steps, gold_steps)
```

### 2. Base Builder

Inherit from `BasePipelineBuilder` to get common builder functionality:

```python
from pipeline_builder_base import BasePipelineBuilder
from pipeline_builder_base.models import PipelineConfig

class MyPipelineBuilder(BasePipelineBuilder):
    def __init__(self, engine, schema: str, **kwargs):
        config = create_development_config(schema, **kwargs)
        super().__init__(config)
        self.engine = engine
    
    def with_bronze_rules(self, name: str, rules: Dict, **kwargs):
        # Use base class validation
        self._check_duplicate_step_name(name, "bronze")
        
        # Create your step
        step = MyBronzeStep(name=name, rules=rules, **kwargs)
        self.bronze_steps[name] = step
        return self
```

### 3. Base Runner

Inherit from `BaseRunner` for common runner functionality:

```python
from pipeline_builder_base import BaseRunner
from pipeline_builder_base.models import PipelineConfig

class MyPipelineRunner(BaseRunner):
    def __init__(self, engine, config: PipelineConfig, **kwargs):
        super().__init__(config)
        self.engine = engine
    
    def run_initial_load(self, bronze_sources: Dict):
        start_time = datetime.now()
        results = []
        
        # Execute steps using engine
        for step in self.bronze_steps.values():
            result = self.engine.validate_source(step, bronze_sources[step.name])
            results.append(result)
        
        # Use base class helper
        execution_result = self._collect_step_results(results)
        return self._create_pipeline_report("success", start_time, datetime.now(), execution_result)
```

### 4. Configuration Factories

Use factory functions for preset configurations:

```python
from pipeline_builder_base import create_development_config, create_production_config

# Development config with relaxed validation
dev_config = create_development_config("dev_schema", verbose=True)

# Production config with strict validation
prod_config = create_production_config("prod_schema", verbose=False)
```

### 5. Step Utilities

Use step utilities for common operations:

```python
from pipeline_builder_base import (
    classify_step_type,
    extract_step_dependencies,
    StepManager,
)

# Classify step type
step_type = classify_step_type(step)  # Returns 'bronze', 'silver', or 'gold'

# Extract dependencies
deps = extract_step_dependencies(step)  # Returns list of dependency names

# Manage steps
manager = StepManager()
manager.add_step(bronze_step, "bronze")
all_steps = manager.get_all_steps()
```

### 6. Error Context

Use error context builders for better error messages:

```python
from pipeline_builder_base import build_validation_context, SuggestionGenerator

# Build validation context
context = build_validation_context(step, "silver")

# Generate suggestions
suggestions = SuggestionGenerator.suggest_fix_for_missing_dependency(
    "my_step", "missing_bronze", "silver"
)
```

## Implementation Steps

1. **Create Engine Class**: Implement `abstracts.Engine` interface
2. **Create Step Models**: Define your step classes (BronzeStep, SilverStep, GoldStep)
3. **Create Builder**: Inherit from `BasePipelineBuilder` and implement engine-specific logic
4. **Create Runner**: Inherit from `BaseRunner` and implement execution logic
5. **Use Shared Utilities**: Leverage validators, factories, and utilities from `pipeline_builder_base`

## Example: Minimal Implementation

```python
from pipeline_builder_base import (
    BasePipelineBuilder,
    BaseRunner,
    create_development_config,
    PipelineValidator,
)
from abstracts import Engine, Runner

class MyEngine(Engine):
    def validate_source(self, step, source):
        # Your validation logic
        pass
    
    def transform_source(self, step, source):
        # Your transformation logic
        pass
    
    def write_target(self, step, source):
        # Your write logic
        pass

class MyPipelineBuilder(BasePipelineBuilder):
    def __init__(self, engine: MyEngine, schema: str):
        config = create_development_config(schema)
        super().__init__(config)
        self.engine = engine
    
    def with_bronze_rules(self, name: str, rules: Dict):
        self._check_duplicate_step_name(name, "bronze")
        # Create and add step
        return self
    
    def to_pipeline(self):
        # Validate and create runner
        errors = self.validate_pipeline()
        if errors:
            raise ValueError(f"Pipeline validation failed: {errors}")
        return MyPipelineRunner(self.engine, self.config, ...)

class MyPipelineRunner(BaseRunner, Runner):
    def __init__(self, engine: MyEngine, config, **kwargs):
        super().__init__(config)
        self.engine = engine
    
    def run_initial_load(self, bronze_sources: Dict):
        # Use base class helpers
        start_time = datetime.now()
        # ... execution logic ...
        return self._create_pipeline_report("success", start_time, datetime.now())
```

## Benefits

- **Reduced Code**: Eliminate ~40-50% of duplicated code
- **Consistent Behavior**: All implementations use same validation and error handling
- **Faster Development**: Focus on engine-specific logic, not boilerplate
- **Better Maintainability**: Fix bugs once, benefit everywhere
- **Type Safety**: Full type annotations throughout

## Next Steps

1. Review existing implementations (`pipeline_builder`, `sql_pipeline_builder`) for patterns
2. Start with a minimal implementation using base classes
3. Gradually add engine-specific features
4. Use shared utilities wherever possible

