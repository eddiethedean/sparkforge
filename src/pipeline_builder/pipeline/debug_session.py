"""Pipeline debug session for interactive stepwise execution.

This module provides a PipelineDebugSession class that simplifies interactive
debugging and iterative refinement of pipeline steps in notebook environments.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pipeline_builder_base.models import PipelineConfig

from ..compat import DataFrame
from ..models import BronzeStep, GoldStep, SilverStep
from .models import PipelineMode, PipelineReport
from .runner import SimplePipelineRunner


class PipelineDebugSession:
    """Interactive debug session for stepwise pipeline execution.

    Provides a convenient interface for running individual steps, overriding
    parameters, and iteratively refining pipeline logic without re-running
    the entire pipeline.

    Attributes:
        runner: SimplePipelineRunner instance for execution.
        steps: List of all pipeline steps.
        mode: Current execution mode.
        context: Execution context dictionary mapping step names to DataFrames.
        step_params: Dictionary mapping step names to parameter dictionaries.

    Example:
        >>> from pipeline_builder.pipeline.debug_session import PipelineDebugSession
        >>> from pipeline_builder_base.models import PipelineConfig
        >>>
        >>> # Create session
        >>> config = PipelineConfig.create_default(schema="my_schema")
        >>> session = PipelineDebugSession(spark, config, steps=[bronze, silver, gold])
        >>>
        >>> # Run until a specific step
        >>> report, context = session.run_until("clean_events")
        >>>
        >>> # Run a single step
        >>> report, context = session.run_step("clean_events")
        >>>
        >>> # Rerun with parameter override
        >>> session.step_params["clean_events"] = {"threshold": 0.9}
        >>> report, context = session.rerun_step("clean_events")
    """

    def __init__(
        self,
        spark: Any,  # SparkSession
        config: PipelineConfig,
        steps: list[BronzeStep | SilverStep | GoldStep],
        mode: PipelineMode = PipelineMode.INITIAL,
        bronze_sources: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
        logger: Optional[Any] = None,  # PipelineLogger
        functions: Optional[Any] = None,  # FunctionsProtocol
    ):
        """Initialize the debug session.

        Args:
            spark: Active SparkSession instance.
            config: Pipeline configuration.
            steps: List of pipeline steps (Bronze, Silver, Gold).
            mode: Initial execution mode. Defaults to INITIAL.
            bronze_sources: Optional bronze source data dictionary.
            logger: Optional logger instance.
            functions: Optional functions protocol instance.
        """
        # Group steps by type for runner
        bronze_steps: Dict[str, BronzeStep] = {}
        silver_steps: Dict[str, SilverStep] = {}
        gold_steps: Dict[str, GoldStep] = {}

        for step in steps:
            if step.step_type.value == "bronze":
                bronze_steps[step.name] = step  # type: ignore[assignment]
            elif step.step_type.value == "silver":
                silver_steps[step.name] = step  # type: ignore[assignment]
            elif step.step_type.value == "gold":
                gold_steps[step.name] = step  # type: ignore[assignment]

        self.runner = SimplePipelineRunner(
            spark=spark,
            config=config,
            bronze_steps=bronze_steps,
            silver_steps=silver_steps,
            gold_steps=gold_steps,
            logger=logger,
            functions=functions,
        )
        self.steps = steps
        self.mode = mode
        self.context: Dict[str, DataFrame] = {}  # type: ignore[valid-type]
        self.step_params: Dict[str, Dict[str, Any]] = {}

        # Initialize context with bronze sources if provided
        if bronze_sources:
            self.context.update(bronze_sources)

    def run_until(
        self,
        step_name: str,
        write_outputs: bool = True,
    ) -> tuple[PipelineReport, Dict[str, DataFrame]]:  # type: ignore[valid-type]
        """Run pipeline until a specific step completes (inclusive).

        Args:
            step_name: Name of the step to stop after (inclusive).
            write_outputs: If True, write outputs to tables. If False, skip writes.

        Returns:
            Tuple of (PipelineReport, context dictionary). Context is updated
            with all step outputs and stored in self.context.

        Example:
            >>> report, context = session.run_until("clean_events")
            >>> # Context now contains outputs up to clean_events
        """
        # Extract bronze sources from self.context for bronze steps
        bronze_sources = {}
        for step in self.steps:
            if step.step_type.value == "bronze" and step.name in self.context:
                bronze_sources[step.name] = self.context[step.name]
        
        report, context = self.runner.run_until(
            step_name=step_name,
            steps=self.steps,
            mode=self.mode,
            bronze_sources=bronze_sources if bronze_sources else None,
            step_params=self.step_params,
            write_outputs=write_outputs,
        )
        self.context = context
        return report, context

    def run_step(
        self,
        step_name: str,
        write_outputs: bool = True,
    ) -> tuple[PipelineReport, Dict[str, DataFrame]]:  # type: ignore[valid-type]
        """Run a single step, loading dependencies from context or tables.

        Args:
            step_name: Name of the step to execute.
            write_outputs: If True, write outputs to tables. If False, skip writes.

        Returns:
            Tuple of (PipelineReport, context dictionary). Context is updated
            with the step output and stored in self.context.

        Example:
            >>> report, context = session.run_step("clean_events")
            >>> # Step executed, context updated
        """
        report, context = self.runner.run_step(
            step_name=step_name,
            steps=self.steps,
            mode=self.mode,
            context=self.context,
            step_params=self.step_params,
            write_outputs=write_outputs,
        )
        self.context = context
        return report, context

    def rerun_step(
        self,
        step_name: str,
        invalidate_downstream: bool = True,
        write_outputs: bool = True,
    ) -> tuple[PipelineReport, Dict[str, DataFrame]]:  # type: ignore[valid-type]
        """Rerun a step with current parameter overrides.

        Uses self.step_params for parameter overrides. To change parameters,
        modify self.step_params before calling this method.

        Args:
            step_name: Name of the step to rerun.
            invalidate_downstream: If True, remove downstream outputs from context.
            write_outputs: If True, write outputs to tables. If False, skip writes.

        Returns:
            Tuple of (PipelineReport, context dictionary). Context is updated
            with the step output and stored in self.context.

        Example:
            >>> # Set parameter override
            >>> session.step_params["clean_events"] = {"threshold": 0.9}
            >>> # Rerun with override
            >>> report, context = session.rerun_step("clean_events")
        """
        report, context = self.runner.rerun_step(
            step_name=step_name,
            steps=self.steps,
            mode=self.mode,
            context=self.context,
            step_params=self.step_params,
            invalidate_downstream=invalidate_downstream,
            write_outputs=write_outputs,
        )
        self.context = context
        return report, context

    def set_step_params(
        self, step_name: str, params: Dict[str, Any]
    ) -> None:
        """Set parameters for a step.

        Convenience method to update step_params.

        Args:
            step_name: Name of the step.
            params: Parameter dictionary to pass to the step's transform function.

        Example:
            >>> session.set_step_params("clean_events", {"threshold": 0.9})
        """
        self.step_params[step_name] = params

    def clear_step_params(self, step_name: Optional[str] = None) -> None:
        """Clear parameter overrides for a step or all steps.

        Args:
            step_name: Name of the step. If None, clears all step params.

        Example:
            >>> session.clear_step_params("clean_events")  # Clear one step
            >>> session.clear_step_params()  # Clear all steps
        """
        if step_name is None:
            self.step_params.clear()
        else:
            self.step_params.pop(step_name, None)
