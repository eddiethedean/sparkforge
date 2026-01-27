"""
Simplified pipeline runner for the framework.

This module provides a clean, focused pipeline runner that delegates
execution to the simplified execution engine.

# Depends on:
#   compat
#   execution
#   functions
#   logging
#   models.pipeline
#   models.steps
#   pipeline.models
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Union, cast

from abstracts.reports.run import Report
from abstracts.runner import Runner
from abstracts.source import Source
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionMode,
    PipelineConfig,
    PipelineMetrics,
)
from pipeline_builder_base.runner import BaseRunner

from ..compat import DataFrame, SparkSession
from ..execution import ExecutionEngine
from ..execution import ExecutionResult as SparkExecutionResult
from ..functions import FunctionsProtocol
from ..models import BronzeStep, GoldStep, SilverStep
from .models import PipelineMode, PipelineReport, PipelineStatus


class SimplePipelineRunner(BaseRunner, Runner):
    """
    Simplified pipeline runner that delegates to the execution engine.

    This runner focuses on orchestration and reporting, delegating
    actual execution to the simplified ExecutionEngine.

    Implements abstracts.Runner interface while maintaining backward compatibility
    with additional methods (run_full_refresh, run_validation_only).
    """

    def __init__(
        self,
        spark: SparkSession,  # type: ignore[valid-type]
        config: PipelineConfig,
        bronze_steps: Optional[Dict[str, BronzeStep]] = None,
        silver_steps: Optional[Dict[str, SilverStep]] = None,
        gold_steps: Optional[Dict[str, GoldStep]] = None,
        logger: Optional[PipelineLogger] = None,
        functions: Optional[FunctionsProtocol] = None,
        # Abstracts.Runner compatibility - these will be set if using abstracts interface
        steps: Optional[list[Union[BronzeStep, SilverStep, GoldStep]]] = None,
        engine: Optional[
            Any
        ] = None,  # Engine from abstracts, but we use ExecutionEngine
    ):
        """
        Initialize the simplified pipeline runner.

        Args:
            spark: Active SparkSession instance
            config: Pipeline configuration
            bronze_steps: Bronze steps dictionary
            silver_steps: Silver steps dictionary
            gold_steps: Gold steps dictionary
            logger: Optional logger instance
            functions: Optional functions object for PySpark operations
            steps: Optional list of steps (for abstracts.Runner compatibility)
            engine: Optional engine (for abstracts.Runner compatibility, ignored)
        """
        # Initialize BaseRunner first
        super().__init__(config, logger=logger)

        # Initialize abstracts.Runner with empty lists (we'll use our own step storage)
        # This satisfies the abstract base class requirement
        # Use Any for engine type to avoid type checking issues with _DummyEngine

        dummy_engine: Any = _DummyEngine()
        Runner.__init__(self, steps=[], engine=engine or dummy_engine)

        self.spark = spark
        self.bronze_steps = bronze_steps or {}
        self.silver_steps = silver_steps or {}
        self.gold_steps = gold_steps or {}
        self.functions = functions
        self.execution_engine = ExecutionEngine(spark, config, self.logger, functions)

        # If steps provided (from abstracts interface), convert to step dictionaries
        if steps:
            for step in steps:
                if step.step_type.value == "bronze":
                    self.bronze_steps[step.name] = step  # type: ignore[assignment]
                elif step.step_type.value == "silver":
                    self.silver_steps[step.name] = step  # type: ignore[assignment]
                elif step.step_type.value == "gold":
                    self.gold_steps[step.name] = step  # type: ignore[assignment]

    def run_pipeline(
        self,
        steps: list[Union[BronzeStep, SilverStep, GoldStep]],
        mode: PipelineMode = PipelineMode.INITIAL,
        bronze_sources: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
    ) -> PipelineReport:
        """
        Run a complete pipeline.

        Args:
            steps: List of pipeline steps to execute
            mode: Pipeline execution mode
            bronze_sources: Optional bronze source data

        Returns:
            PipelineReport with execution results
        """
        start_time = datetime.now()
        pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Convert PipelineMode to ExecutionMode
        execution_mode = self._convert_mode(mode)

        try:
            self.logger.info(f"Starting pipeline execution: {pipeline_id}")

            # Prepare bronze sources if provided
            if bronze_sources:
                # Add bronze sources to context for execution
                context = {}
                for step in steps:
                    if step.step_type.value == "bronze" and step.name in bronze_sources:
                        context[step.name] = bronze_sources[step.name]
            else:
                context = {}

            # Execute pipeline using the execution engine
            result = self.execution_engine.execute_pipeline(
                steps, execution_mode, context=context
            )

            # Convert execution result to pipeline report
            report = self._create_spark_pipeline_report(
                pipeline_id=pipeline_id,
                mode=mode,
                start_time=start_time,
                execution_result=result,
            )

            self.logger.info(f"Completed pipeline execution: {pipeline_id}")
            return report

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            return self._create_error_report(
                pipeline_id=pipeline_id, mode=mode, start_time=start_time, error=str(e)
            )

    def run_initial_load(
        self,
        bronze_sources: Union[Optional[Dict[str, Source]], list] = None,
        steps: Optional[
            list
        ] = None,  # Backward compatibility: old signature accepted steps as first arg
    ) -> Report:  # PipelineReport satisfies Report Protocol
        """
        Run initial load pipeline.

        Implements abstracts.Runner.run_initial_load interface.
        Also supports backward-compatible signature with steps parameter.

        Args:
            bronze_sources: Dictionary mapping bronze step names to Source (DataFrame), or None
            steps: Optional list of steps (for backward compatibility with old signature)
        """
        # Handle backward compatibility: if first arg is a list, treat it as steps
        if isinstance(bronze_sources, list):
            # Old signature: run_initial_load([steps])
            steps = bronze_sources
            bronze_sources = None

        # Convert Source (Protocol) to DataFrame if needed
        # Source Protocol is satisfied by DataFrame, so we accept any DataFrame-like object
        from ..compat_helpers import is_dataframe_like

        bronze_sources_df: Optional[Dict[str, DataFrame]] = None  # type: ignore[valid-type]
        if bronze_sources:
            bronze_sources_df = {}
            for name, source in bronze_sources.items():
                # Check if it's a DataFrame-like object using compat helper
                if not is_dataframe_like(source):
                    raise TypeError(
                        f"bronze_sources must contain DataFrame-like objects, got {type(source)}"
                    )
                bronze_sources_df[name] = cast(DataFrame, source)

        # Use provided steps or stored steps
        if steps is None:
            steps = (
                list(self.bronze_steps.values())
                + list(self.silver_steps.values())
                + list(self.gold_steps.values())
            )

        # PipelineReport satisfies Report Protocol structurally
        return self.run_pipeline(steps, PipelineMode.INITIAL, bronze_sources_df)  # type: ignore[return-value]

    def run_incremental(
        self,
        bronze_sources: Union[Optional[Dict[str, Source]], list] = None,
        steps: Optional[
            list
        ] = None,  # Backward compatibility: old signature accepted steps as first arg
    ) -> Report:  # PipelineReport satisfies Report Protocol
        """
        Run incremental pipeline with all stored steps.

        Implements abstracts.Runner.run_incremental interface.
        Also supports backward-compatible signature with steps parameter.

        Args:
            bronze_sources: Optional dictionary mapping bronze step names to Source (DataFrame), or None
            steps: Optional list of steps (for backward compatibility with old signature)

        Returns:
            Report (PipelineReport) with execution results
        """
        # Handle backward compatibility: if first arg is a list, treat it as steps
        if isinstance(bronze_sources, list):
            # Old signature: run_incremental([steps])
            steps = bronze_sources
            bronze_sources = None

        # Convert Source (Protocol) to DataFrame if needed
        # Source Protocol is satisfied by DataFrame, so we accept any DataFrame-like object
        from ..compat_helpers import is_dataframe_like

        bronze_sources_df: Optional[Dict[str, DataFrame]] = None  # type: ignore[valid-type]
        if bronze_sources:
            bronze_sources_df = {}
            for name, source in bronze_sources.items():
                # Check if it's a DataFrame-like object using compat helper
                if not is_dataframe_like(source):
                    raise TypeError(
                        f"bronze_sources must contain DataFrame-like objects, got {type(source)}"
                    )
                bronze_sources_df[name] = cast(DataFrame, source)

        # Use provided steps or stored steps
        if steps is None:
            steps = (
                list(self.bronze_steps.values())
                + list(self.silver_steps.values())
                + list(self.gold_steps.values())
            )

        # PipelineReport satisfies Report Protocol structurally
        return self.run_pipeline(steps, PipelineMode.INCREMENTAL, bronze_sources_df)  # type: ignore[return-value]

    def run_full_refresh(
        self,
        bronze_sources: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
    ) -> PipelineReport:
        """
        Run full refresh pipeline with all stored steps.

        Args:
            bronze_sources: Optional dictionary mapping bronze step names to DataFrames

        Returns:
            PipelineReport with execution results
        """
        steps = (
            list(self.bronze_steps.values())
            + list(self.silver_steps.values())
            + list(self.gold_steps.values())
        )
        return self.run_pipeline(steps, PipelineMode.FULL_REFRESH, bronze_sources)

    def run_validation_only(
        self,
        bronze_sources: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
    ) -> PipelineReport:
        """
        Run validation-only pipeline with all stored steps.

        Args:
            bronze_sources: Optional dictionary mapping bronze step names to DataFrames

        Returns:
            PipelineReport with execution results
        """
        steps = (
            list(self.bronze_steps.values())
            + list(self.silver_steps.values())
            + list(self.gold_steps.values())
        )
        return self.run_pipeline(steps, PipelineMode.VALIDATION_ONLY, bronze_sources)

    def _get_all_steps(
        self, steps: Optional[list[Union[BronzeStep, SilverStep, GoldStep]]] = None
    ) -> list[Union[BronzeStep, SilverStep, GoldStep]]:
        """Get all steps from stored dictionaries or provided list.

        Args:
            steps: Optional list of steps. If None, returns all stored steps.

        Returns:
            List of all steps (bronze, silver, gold).
        """
        if steps is not None:
            return steps
        return (
            list(self.bronze_steps.values())
            + list(self.silver_steps.values())
            + list(self.gold_steps.values())
        )

    def run_until(
        self,
        step_name: str,
        steps: Optional[list[Union[BronzeStep, SilverStep, GoldStep]]] = None,
        mode: PipelineMode = PipelineMode.INITIAL,
        bronze_sources: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
        step_params: Optional[Dict[str, Dict[str, Any]]] = None,
        write_outputs: bool = True,
    ) -> tuple[PipelineReport, Dict[str, DataFrame]]:  # type: ignore[valid-type]
        """Run pipeline until a specific step completes (inclusive).

        Executes steps in dependency order until the specified step completes,
        then stops. Useful for debugging or partial pipeline execution.

        Args:
            step_name: Name of the step to stop after (inclusive).
            steps: Optional list of steps. If None, uses all stored steps.
            mode: Pipeline execution mode.
            bronze_sources: Optional bronze source data.
            step_params: Optional dictionary mapping step names to parameter
                dictionaries for transform functions.
            write_outputs: If True, write outputs to tables. If False, skip writes.

        Returns:
            Tuple of (PipelineReport, context dictionary) where context contains
            all step outputs for further execution.

        Example:
            >>> report, context = runner.run_until("clean_events")
            >>> # Now you can inspect context or continue execution
        """
        all_steps = self._get_all_steps(steps)
        start_time = datetime.now()
        pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        execution_mode = self._convert_mode(mode)

        try:
            self.logger.info(
                f"Starting pipeline execution until step '{step_name}': {pipeline_id}"
            )

            # Prepare context
            context: Dict[str, DataFrame] = {}  # type: ignore[valid-type]
            if bronze_sources:
                for step in all_steps:
                    if step.step_type.value == "bronze" and step.name in bronze_sources:
                        context[step.name] = bronze_sources[step.name]

            # Execute pipeline with stop_after_step
            result = self.execution_engine.execute_pipeline(
                all_steps,
                execution_mode,
                context=context,
                step_params=step_params,
                stop_after_step=step_name,
                write_outputs=write_outputs,
            )

            # Create report
            report = self._create_spark_pipeline_report(
                pipeline_id=pipeline_id,
                mode=mode,
                start_time=start_time,
                execution_result=result,
            )

            self.logger.info(
                f"Completed pipeline execution until step '{step_name}': {pipeline_id}"
            )
            return report, context

        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            error_report = self._create_error_report(
                pipeline_id=pipeline_id, mode=mode, start_time=start_time, error=str(e)
            )
            return error_report, context

    def run_step(
        self,
        step_name: str,
        steps: Optional[list[Union[BronzeStep, SilverStep, GoldStep]]] = None,
        mode: PipelineMode = PipelineMode.INITIAL,
        context: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
        step_params: Optional[Dict[str, Dict[str, Any]]] = None,
        write_outputs: bool = True,
    ) -> tuple[PipelineReport, Dict[str, DataFrame]]:  # type: ignore[valid-type]
        """Run a single step, loading dependencies from context or tables.

        Executes only the specified step, using existing outputs from context
        or reading from tables for dependencies. Useful for debugging individual steps.

        Args:
            step_name: Name of the step to execute.
            steps: Optional list of steps. If None, uses all stored steps.
            mode: Pipeline execution mode.
            context: Optional execution context. If None, empty dict is used.
                Dependencies will be loaded from tables if not in context.
            step_params: Optional dictionary mapping step names to parameter
                dictionaries for transform functions.
            write_outputs: If True, write outputs to tables. If False, skip writes.

        Returns:
            Tuple of (PipelineReport, context dictionary) with updated context.

        Example:
            >>> report, context = runner.run_step("clean_events", context=context)
            >>> # Step executed, context updated with output
        """
        all_steps = self._get_all_steps(steps)
        start_time = datetime.now()
        pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        execution_mode = self._convert_mode(mode)

        if context is None:
            context = {}

        try:
            self.logger.info(
                f"Starting single step execution '{step_name}': {pipeline_id}"
            )

            # Execute pipeline starting at this step
            result = self.execution_engine.execute_pipeline(
                all_steps,
                execution_mode,
                context=context,
                step_params=step_params,
                start_at_step=step_name,
                stop_after_step=step_name,
                write_outputs=write_outputs,
            )

            # Create report
            report = self._create_spark_pipeline_report(
                pipeline_id=pipeline_id,
                mode=mode,
                start_time=start_time,
                execution_result=result,
            )

            self.logger.info(f"Completed step execution '{step_name}': {pipeline_id}")
            return report, context

        except Exception as e:
            self.logger.error(f"Step execution failed: {e}")
            error_report = self._create_error_report(
                pipeline_id=pipeline_id, mode=mode, start_time=start_time, error=str(e)
            )
            return error_report, context

    def rerun_step(
        self,
        step_name: str,
        steps: Optional[list[Union[BronzeStep, SilverStep, GoldStep]]] = None,
        mode: PipelineMode = PipelineMode.INITIAL,
        context: Optional[Dict[str, DataFrame]] = None,  # type: ignore[valid-type]
        step_params: Optional[Dict[str, Dict[str, Any]]] = None,
        invalidate_downstream: bool = True,
        write_outputs: bool = True,
    ) -> tuple[PipelineReport, Dict[str, DataFrame]]:  # type: ignore[valid-type]
        """Rerun a step with optional parameter overrides.

        Reruns the specified step, optionally removing downstream outputs from
        context to ensure clean execution. Useful for debugging and iterative refinement.

        Args:
            step_name: Name of the step to rerun.
            steps: Optional list of steps. If None, uses all stored steps.
            mode: Pipeline execution mode.
            context: Optional execution context. If None, empty dict is used.
            step_params: Optional dictionary mapping step names to parameter
                dictionaries for transform functions. Overrides are applied to
                the specified step.
            invalidate_downstream: If True, remove downstream step outputs from
                context to ensure clean rerun. Defaults to True.
            write_outputs: If True, write outputs to tables. If False, skip writes.

        Returns:
            Tuple of (PipelineReport, context dictionary) with updated context.

        Example:
            >>> # First run
            >>> report, context = runner.run_step("clean_events")
            >>> # Rerun with different parameters
            >>> report2, context = runner.rerun_step(
            ...     "clean_events",
            ...     context=context,
            ...     step_params={"clean_events": {"filter_threshold": 0.9}}
            ... )
        """
        all_steps = self._get_all_steps(steps)
        start_time = datetime.now()
        pipeline_id = f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        execution_mode = self._convert_mode(mode)

        if context is None:
            context = {}

        # Invalidate downstream steps if requested
        if invalidate_downstream:
            from pipeline_builder_base.dependencies import DependencyAnalyzer

            # Build dependency graph to find downstream steps
            bronze_steps = [s for s in all_steps if s.step_type.value == "bronze"]
            silver_steps = [s for s in all_steps if s.step_type.value == "silver"]
            gold_steps = [s for s in all_steps if s.step_type.value == "gold"]

            analyzer = DependencyAnalyzer()
            analysis = analyzer.analyze_dependencies(
                bronze_steps={s.name: s for s in bronze_steps},
                silver_steps={s.name: s for s in silver_steps},
                gold_steps={s.name: s for s in gold_steps},
            )

            # Find downstream steps (steps that depend on step_name)
            execution_order = analysis.execution_order
            if step_name in execution_order:
                step_index = execution_order.index(step_name)
                downstream_steps = execution_order[step_index + 1 :]

                # Remove downstream outputs from context
                for downstream_name in downstream_steps:
                    if downstream_name in context:
                        del context[downstream_name]
                        self.logger.debug(
                            f"Removed downstream step '{downstream_name}' from context"
                        )

        try:
            self.logger.info(f"Rerunning step '{step_name}': {pipeline_id}")

            # Execute pipeline starting at this step
            result = self.execution_engine.execute_pipeline(
                all_steps,
                execution_mode,
                context=context,
                step_params=step_params,
                start_at_step=step_name,
                stop_after_step=step_name,
                write_outputs=write_outputs,
            )

            # Create report
            report = self._create_spark_pipeline_report(
                pipeline_id=pipeline_id,
                mode=mode,
                start_time=start_time,
                execution_result=result,
            )

            self.logger.info(f"Completed step rerun '{step_name}': {pipeline_id}")
            return report, context

        except Exception as e:
            self.logger.error(f"Step rerun failed: {e}")
            error_report = self._create_error_report(
                pipeline_id=pipeline_id, mode=mode, start_time=start_time, error=str(e)
            )
            return error_report, context

    def _convert_mode(self, mode: PipelineMode) -> ExecutionMode:
        """Convert PipelineMode to ExecutionMode."""
        mode_map = {
            PipelineMode.INITIAL: ExecutionMode.INITIAL,
            PipelineMode.INCREMENTAL: ExecutionMode.INCREMENTAL,
            PipelineMode.FULL_REFRESH: ExecutionMode.FULL_REFRESH,
            PipelineMode.VALIDATION_ONLY: ExecutionMode.VALIDATION_ONLY,
        }
        return mode_map.get(mode, ExecutionMode.INITIAL)

    def _create_spark_pipeline_report(
        self,
        pipeline_id: str,
        mode: PipelineMode,
        start_time: datetime,
        execution_result: SparkExecutionResult,
    ) -> PipelineReport:
        """Create a pipeline report from execution result."""
        end_time = execution_result.end_time or datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Count successful and failed steps
        steps = execution_result.steps or []
        successful_steps = [s for s in steps if s.status.value == "completed"]
        failed_steps = [s for s in steps if s.status.value == "failed"]

        # Import StepType for layer filtering
        from ..execution import StepType

        # Organize step results by layer (bronze/silver/gold)
        bronze_results = {}
        silver_results = {}
        gold_results = {}

        for step_result in steps:
            step_info = {
                "status": step_result.status.value,
                "duration": step_result.duration,
                "rows_processed": step_result.rows_processed,
                "output_table": step_result.output_table,
                "start_time": step_result.start_time.isoformat(),
                "end_time": step_result.end_time.isoformat()
                if step_result.end_time
                else None,
                "write_mode": step_result.write_mode,  # type: ignore[attr-defined]
                "validation_rate": step_result.validation_rate,
                "rows_written": step_result.rows_written,
                "input_rows": step_result.input_rows,
            }

            # Add error if present
            if step_result.error:
                step_info["error"] = step_result.error

            # Add dataframe if available in context (for users who want to access output)
            if hasattr(execution_result, "context"):
                context = getattr(execution_result, "context", None)
                if (
                    context
                    and isinstance(context, dict)
                    and step_result.step_name in context
                ):
                    step_info["dataframe"] = context[step_result.step_name]

            # Categorize by step type
            if step_result.step_type.value == "bronze":
                bronze_results[step_result.step_name] = step_info
            elif step_result.step_type.value == "silver":
                silver_results[step_result.step_name] = step_info
            elif step_result.step_type.value == "gold":
                gold_results[step_result.step_name] = step_info

        # Aggregate row counts from step results
        total_rows_processed = sum(s.rows_processed or 0 for s in steps)
        # For rows_written, only count Silver/Gold steps (those with output_table)
        total_rows_written = sum(
            s.rows_processed or 0 for s in steps if s.output_table is not None
        )

        # Calculate durations by layer
        bronze_duration = sum(
            s.duration or 0 for s in steps if s.step_type == StepType.BRONZE
        )
        silver_duration = sum(
            s.duration or 0 for s in steps if s.step_type == StepType.SILVER
        )
        gold_duration = sum(
            s.duration or 0 for s in steps if s.step_type == StepType.GOLD
        )

        return PipelineReport(
            pipeline_id=pipeline_id,
            execution_id=execution_result.execution_id,
            status=(
                PipelineStatus.COMPLETED
                if execution_result.status == "completed"
                else PipelineStatus.FAILED
            ),
            mode=mode,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            metrics=PipelineMetrics(
                total_steps=len(steps),
                successful_steps=len(successful_steps),
                failed_steps=len(failed_steps),
                total_duration=duration,
                bronze_duration=bronze_duration,
                silver_duration=silver_duration,
                gold_duration=gold_duration,
                total_rows_processed=total_rows_processed,
                total_rows_written=total_rows_written,
            ),
            bronze_results=bronze_results,
            silver_results=silver_results,
            gold_results=gold_results,
            errors=[s.error for s in failed_steps if s.error],
            warnings=[],
        )

    def _create_error_report(
        self, pipeline_id: str, mode: PipelineMode, start_time: datetime, error: str
    ) -> PipelineReport:
        """Create an error pipeline report."""
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return PipelineReport(
            pipeline_id=pipeline_id,
            execution_id=f"error_{pipeline_id}",
            status=PipelineStatus.FAILED,
            mode=mode,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            metrics=PipelineMetrics(
                total_steps=0,
                successful_steps=0,
                failed_steps=0,
                total_duration=duration,
            ),
            errors=[error],
            warnings=[],
        )


class _DummyEngine:
    """Dummy engine for Runner.__init__ compatibility."""

    pass


# Alias for backward compatibility
PipelineRunner = SimplePipelineRunner

# Explicitly clear abstract methods since they are implemented
# Python's ABC mechanism sometimes doesn't recognize implementations with positional-only args
if hasattr(SimplePipelineRunner, "__abstractmethods__"):
    SimplePipelineRunner.__abstractmethods__ = frozenset()
