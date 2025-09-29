"""
Factory functions for creating and managing pipeline models.
"""

from __future__ import annotations

import json
from datetime import datetime

from .base import ParallelConfig, ValidationThresholds
from .enums import ExecutionMode
from .exceptions import PipelineConfigurationError, PipelineExecutionError
from .execution import ExecutionContext
from .pipeline import PipelineConfig
from .steps import BronzeStep, GoldStep, SilverStep


def create_pipeline_config(
    schema: str,
    bronze_threshold: float = 95.0,
    silver_threshold: float = 98.0,
    gold_threshold: float = 99.0,
    enable_parallel: bool = True,
    max_workers: int = 4,
    verbose: bool = True,
) -> PipelineConfig:
    """Factory function to create pipeline configuration."""
    thresholds = ValidationThresholds(
        bronze=bronze_threshold, silver=silver_threshold, gold=gold_threshold
    )
    parallel = ParallelConfig(enabled=enable_parallel, max_workers=max_workers)
    return PipelineConfig(
        schema=schema, thresholds=thresholds, parallel=parallel, verbose=verbose
    )


def create_execution_context(mode: ExecutionMode) -> ExecutionContext:
    """Factory function to create execution context."""
    return ExecutionContext(mode=mode, start_time=datetime.utcnow())


def validate_pipeline_config(config: PipelineConfig) -> None:
    """Validate a pipeline configuration."""
    try:
        config.validate()
    except PipelineExecutionError as e:
        raise PipelineConfigurationError(f"Invalid pipeline configuration: {e}") from e


def validate_step_config(step: BronzeStep | SilverStep | GoldStep) -> None:
    """Validate a step configuration."""
    try:
        step.validate()
    except PipelineExecutionError as e:
        raise PipelineConfigurationError(f"Invalid step configuration: {e}") from e


def serialize_pipeline_config(config: PipelineConfig) -> str:
    """Serialize pipeline configuration to JSON."""
    return config.to_json()


def deserialize_pipeline_config(json_str: str) -> PipelineConfig:
    """Deserialize pipeline configuration from JSON."""
    data = json.loads(json_str)
    return PipelineConfig(
        schema=data["schema"],
        thresholds=ValidationThresholds(
            bronze=data["thresholds"]["bronze"],
            silver=data["thresholds"]["silver"],
            gold=data["thresholds"]["gold"],
        ),
        parallel=ParallelConfig(
            enabled=data["parallel"]["enabled"],
            max_workers=data["parallel"]["max_workers"],
            timeout_secs=data["parallel"].get("timeout_secs", 300),
        ),
        verbose=data.get("verbose", True),
    )
