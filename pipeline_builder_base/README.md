# Pipeline Builder Base

Shared base package for pipeline builders implementing the Medallion Architecture pattern.

This package contains code shared between:
- `pipeline_builder` - Spark/Delta Lake implementation
- `sql_pipeline_builder` - SQLAlchemy implementation

## Contents

- **Models**: BaseModel, PipelineConfig, ValidationThresholds, ParallelConfig, ExecutionResult, StepResult, etc.
- **Dependencies**: DependencyAnalyzer, DependencyGraph for analyzing pipeline dependencies
- **Logging**: PipelineLogger for consistent logging across implementations
- **Errors**: Error classes (SparkForgeError, ValidationError, etc.)
- **Reporting**: Report TypedDicts and utilities
- **Writer**: Base LogWriter class for unified logging interface

## Installation

```bash
pip install pipeline-builder-base
```

## Usage

This package is typically used as a dependency by `pipeline_builder` and `sql_pipeline_builder`. 
You generally don't import from it directly unless you're building a custom pipeline builder implementation.

## Dependencies

This package has no external dependencies - it's pure Python.

