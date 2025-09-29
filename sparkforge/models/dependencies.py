"""
Dependency models for the Pipeline Builder.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from ..errors import PipelineValidationError
from .base import BaseModel


@dataclass
class SilverDependencyInfo(BaseModel):
    """
    Dependency information for Silver steps.

    Attributes:
        step_name: Name of the silver step
        source_bronze: Source bronze step name
        depends_on_silvers: Set of silver step names this step depends on
        can_run_parallel: Whether this step can run in parallel
        execution_group: Execution group for parallel processing
    """

    step_name: str
    source_bronze: str
    depends_on_silvers: set[str]
    can_run_parallel: bool
    execution_group: int

    def validate(self) -> None:
        """Validate dependency information."""
        if not self.step_name or not isinstance(self.step_name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise PipelineValidationError(
                "Source bronze step name must be a non-empty string"
            )
        if not isinstance(self.depends_on_silvers, set):
            raise PipelineValidationError("Depends on silvers must be a set")
        if self.execution_group < 0:
            raise PipelineValidationError("Execution group must be non-negative")


@dataclass
class CrossLayerDependency(BaseModel):
    """
    Represents a dependency between steps across different layers.

    Attributes:
        source_step: Name of the source step
        target_step: Name of the target step
        dependency_type: Type of dependency (data, validation, etc.)
        is_required: Whether this dependency is required for execution
    """

    source_step: str
    target_step: str
    dependency_type: str = "data"
    is_required: bool = True

    def validate(self) -> None:
        """Validate dependency information."""
        if not self.source_step or not isinstance(self.source_step, str):
            raise PipelineValidationError("Source step must be a non-empty string")
        if not self.target_step or not isinstance(self.target_step, str):
            raise PipelineValidationError("Target step must be a non-empty string")
        if self.source_step == self.target_step:
            raise PipelineValidationError("Source and target steps cannot be the same")


@dataclass
class UnifiedStepConfig(BaseModel):
    """
    Unified configuration for pipeline steps.

    Attributes:
        step_name: Name of the step
        step_type: Type of step (bronze/silver/gold)
        dependencies: List of step dependencies
        config: Step-specific configuration
    """

    step_name: str
    step_type: str
    dependencies: list[str]
    config: Dict[str, Any]

    def validate(self) -> None:
        """Validate unified step configuration."""
        if not self.step_name or not isinstance(self.step_name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if self.step_type not in ["bronze", "silver", "gold"]:
            raise PipelineValidationError("Step type must be bronze, silver, or gold")
        if not isinstance(self.dependencies, list):
            raise PipelineValidationError("Dependencies must be a list")
        if not isinstance(self.config, dict):
            raise PipelineValidationError("Config must be a dictionary")


@dataclass
class UnifiedExecutionPlan(BaseModel):
    """
    Unified execution plan for pipeline steps.

    Attributes:
        steps: List of unified step configurations
        execution_order: Ordered list of step names for execution
        parallel_groups: Groups of steps that can run in parallel
    """

    steps: list[UnifiedStepConfig]
    execution_order: list[str]
    parallel_groups: list[list[str]]

    def validate(self) -> None:
        """Validate unified execution plan."""
        if not isinstance(self.steps, list):
            raise PipelineValidationError("Steps must be a list")
        if not isinstance(self.execution_order, list):
            raise PipelineValidationError("Execution order must be a list")
        if not isinstance(self.parallel_groups, list):
            raise PipelineValidationError("Parallel groups must be a list")

        # Validate that all steps in execution order exist
        step_names = {step.step_name for step in self.steps}
        for step_name in self.execution_order:
            if step_name not in step_names:
                raise PipelineValidationError(f"Step {step_name} not found in steps")
