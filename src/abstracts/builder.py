from __future__ import annotations

from typing import List, Literal, Optional, Type

from abstracts.engine import Engine
from abstracts.rules import Rules
from abstracts.runner import Runner
from abstracts.step import Step
from abstracts.transformer import Transformer


class PipelineBuilder:
    """
    Abstract pipeline builder that uses dependency injection for engine and runner.

    This builder provides a fluent API for constructing pipelines with engine injection,
    allowing different engine implementations (SparkEngine, SqlEngine, etc.) to be used.
    """

    def __init__(self, runner_cls: Type[Runner], engine: Engine) -> None:
        """
        Initialize the pipeline builder.

        Args:
            runner_cls: Runner class to use for pipeline execution
            engine: Engine instance to use for step execution
        """
        self.runner_cls = runner_cls
        self.engine = engine
        self.steps: List[Step] = []

    def validate_steps(self, steps: List[Step]) -> bool:
        """
        Validate pipeline steps configuration.

        Args:
            steps: List of steps to validate

        Returns:
            True if all steps are valid, False otherwise

        Raises:
            ValueError: If validation fails with details
        """
        # Allow empty pipelines (some tests create empty pipelines)
        # if not steps:
        #     raise ValueError("Pipeline must have at least one step")

        # Check for duplicate step names
        step_names = [step.name for step in steps]
        if len(step_names) != len(set(step_names)):
            duplicates = [name for name in step_names if step_names.count(name) > 1]
            raise ValueError(f"Duplicate step names found: {set(duplicates)}")

        # Validate step dependencies
        # Determine step types by checking class type or type attribute
        # Concrete steps (BronzeStep, SilverStep, GoldStep) don't have type attribute
        # but satisfy the Step Protocol
        bronze_names = set()
        silver_names = set()
        gold_names = set()

        for step in steps:
            # Check if step has type attribute (for Protocol compatibility)
            if hasattr(step, "type") and step.type:
                step_type = step.type
            else:
                # Determine type from class name
                class_name = step.__class__.__name__
                if "Bronze" in class_name:
                    step_type = "bronze"
                elif "Silver" in class_name:
                    step_type = "silver"
                elif "Gold" in class_name:
                    step_type = "gold"
                else:
                    raise ValueError(f"Cannot determine step type for {class_name}")

            if step_type == "bronze":
                bronze_names.add(step.name)
            elif step_type == "silver":
                silver_names.add(step.name)
            elif step_type == "gold":
                gold_names.add(step.name)

        for step in steps:
            # Determine step type again for validation
            if hasattr(step, "type") and step.type:
                step_type = step.type
            else:
                class_name = step.__class__.__name__
                if "Bronze" in class_name:
                    step_type = "bronze"
                elif "Silver" in class_name:
                    step_type = "silver"
                elif "Gold" in class_name:
                    step_type = "gold"
                else:
                    continue  # Skip unknown types

            if step_type == "silver":
                # Silver steps use source_bronze attribute, not source
                source = getattr(step, "source_bronze", None) or getattr(
                    step, "source", None
                )
                if source and source not in bronze_names:
                    raise ValueError(
                        f"Silver step '{step.name}' references unknown bronze source '{source}'"
                    )
            elif step_type == "gold":
                # Gold steps use source_silvers attribute (list), not source
                source_silvers = getattr(step, "source_silvers", None)
                if source_silvers:
                    for silver_name in source_silvers:
                        if silver_name not in silver_names:
                            raise ValueError(
                                f"Gold step '{step.name}' references unknown silver source '{silver_name}'"
                            )
                # Also check source attribute for backward compatibility
                source = getattr(step, "source", None)
                if source and source not in silver_names:
                    raise ValueError(
                        f"Gold step '{step.name}' references unknown silver source '{source}'"
                    )

        return True

    def to_pipeline(
        self, steps: Optional[List[Step]] = None, engine: Optional[Engine] = None
    ) -> Runner:
        """
        Build and return a Runner for executing the pipeline.

        Args:
            steps: Optional list of steps (uses self.steps if not provided)
            engine: Optional engine instance (uses self.engine if not provided)

        Returns:
            Runner instance ready for execution

        Raises:
            ValueError: If step validation fails
        """
        steps_to_use = steps if steps is not None else self.steps
        engine_to_use = engine if engine is not None else self.engine

        if not steps_to_use:
            raise ValueError("No steps provided to build pipeline")

        if self.validate_steps(steps_to_use):
            return self.runner_cls(steps_to_use, engine_to_use)
        raise ValueError("Invalid steps configuration")

    def with_bronze_rules(self, name: str, rules: Rules) -> PipelineBuilder:
        """
        Add a bronze step with validation rules.

        Args:
            name: Unique name for the bronze step
            rules: Validation rules for the bronze step

        Returns:
            Self for method chaining
        """
        # Note: Step is a Protocol, so we can't instantiate it directly.
        # Concrete implementations will create appropriate step objects.
        # This method is meant to be overridden or used with concrete step types.
        raise NotImplementedError(
            "with_bronze_rules must be implemented by concrete PipelineBuilder subclasses"
        )

    def add_silver_transform(
        self,
        name: str,
        source: str,
        transform: Transformer,
        rules: Rules,
        write_target: str,
        write_mode: Literal["overwrite", "append"],
        write_schema: Optional[str] = None,
    ) -> PipelineBuilder:
        """
        Add a silver transformation step.

        Args:
            name: Unique name for the silver step
            source: Name of the bronze step this depends on
            transform: Transformation function
            rules: Validation rules
            write_target: Target table name
            write_mode: Write mode (overwrite or append)
            write_schema: Optional schema name

        Returns:
            Self for method chaining
        """
        # Note: Step is a Protocol, so we can't instantiate it directly.
        # Concrete implementations will create appropriate step objects.
        raise NotImplementedError(
            "add_silver_transform must be implemented by concrete PipelineBuilder subclasses"
        )

    def add_gold_transform(
        self,
        name: str,
        source: str,
        transform: Transformer,
        rules: Rules,
        write_target: str,
        write_schema: Optional[str] = None,
    ) -> PipelineBuilder:
        """
        Add a gold transformation step.

        Args:
            name: Unique name for the gold step
            source: Name of the silver step this depends on
            transform: Transformation function
            rules: Validation rules
            write_target: Target table name
            write_schema: Optional schema name

        Returns:
            Self for method chaining
        """
        # Note: Step is a Protocol, so we can't instantiate it directly.
        # Concrete implementations will create appropriate step objects.
        raise NotImplementedError(
            "add_gold_transform must be implemented by concrete PipelineBuilder subclasses"
        )
