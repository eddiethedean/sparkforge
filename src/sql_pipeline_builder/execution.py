"""
SQL execution engine for the framework pipelines.

This module provides a robust execution engine that handles SQL pipeline execution
with comprehensive error handling, step-by-step processing, and async support.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union, cast

from pipeline_builder_base.dependencies import DependencyAnalyzer
from pipeline_builder_base.dependencies.analyzer import (
    BronzeStepProtocol,
    GoldStepProtocol,
    SilverStepProtocol,
)
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    PipelineMetrics,
    PipelinePhase,
    StepResult,
)

from sql_pipeline_builder.compat import is_async_engine
from sql_pipeline_builder.engine import SqlEngine
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep
from sql_pipeline_builder.table_operations import read_table
from sql_pipeline_builder.validation import apply_sql_validation_rules


class SqlExecutionEngine:
    """
    SQL execution engine for the framework pipelines.

    This engine handles both individual step execution and full pipeline execution
    with a clean, unified interface. It supports both sync and async SQLAlchemy engines.
    """

    def __init__(
        self,
        session: Any,  # SQLAlchemy Session or AsyncSession
        config: Any,  # PipelineConfig
        logger: Optional[PipelineLogger] = None,
    ):
        """
        Initialize the SQL execution engine.

        Args:
            session: SQLAlchemy Session or AsyncSession instance
            config: Pipeline configuration
            logger: Optional logger instance
        """
        self.session = session
        self.config = config
        if logger is None:
            self.logger = PipelineLogger()
        else:
            self.logger = logger

        # Detect if engine supports async
        self.is_async = is_async_engine(
            session.bind if hasattr(session, "bind") else None
        )
        self.engine = SqlEngine(session, config, self.logger)

    def execute_step(
        self,
        step: Union[SqlBronzeStep, SqlSilverStep] | SqlGoldStep,
        context: Dict[str, Any],
        mode: ExecutionMode = ExecutionMode.INITIAL,
    ) -> StepResult:
        """
        Execute a single pipeline step.

        Args:
            step: Step to execute
            context: Execution context with available data sources
            mode: Execution mode

        Returns:
            StepResult with execution results
        """

        start_time = datetime.now(timezone.utc)
        step_name = step.name

        try:
            self.logger.info(f"Executing step: {step_name}")

            # Determine phase
            if isinstance(step, SqlBronzeStep):
                phase = PipelinePhase.BRONZE
            elif isinstance(step, SqlSilverStep):
                phase = PipelinePhase.SILVER
            else:
                phase = PipelinePhase.GOLD

            # Execute step based on type
            if isinstance(step, SqlBronzeStep):
                result = self._execute_bronze_step(step, context, mode)
            elif isinstance(step, SqlSilverStep):
                result = self._execute_silver_step(step, context, mode)
            else:
                result = self._execute_gold_step(step, context, mode)

            end_time = datetime.now(timezone.utc)

            return StepResult.create_success(
                step_name=step_name,
                phase=phase,
                start_time=start_time,
                end_time=end_time,
                rows_processed=result.get("rows_processed", 0),
                rows_written=result.get("rows_written", 0),
                validation_rate=result.get("validation_rate", 100.0),
                step_type=phase.value,
                table_fqn=result.get("table_fqn"),
                write_mode=result.get("write_mode"),
                input_rows=result.get("input_rows"),
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            self.logger.error(f"Step {step_name} failed: {e}")

            phase = PipelinePhase.BRONZE
            if isinstance(step, SqlSilverStep):
                phase = PipelinePhase.SILVER
            elif isinstance(step, SqlGoldStep):
                phase = PipelinePhase.GOLD

            return StepResult.create_failure(
                step_name=step_name,
                phase=phase,
                start_time=start_time,
                end_time=end_time,
                error_message=str(e),
                step_type=phase.value,
            )

    def _execute_bronze_step(
        self,
        step: SqlBronzeStep,
        context: Dict[str, Any],
        mode: ExecutionMode,
    ) -> Dict[str, Any]:
        """Execute a bronze step."""
        # Bronze steps read from source data
        # In SQL, this would typically be reading from an existing table or query
        source_key = step.name
        if source_key not in context:
            raise ValueError(f"Bronze source '{source_key}' not found in context")

        source_query = context[source_key]

        # Apply validation
        valid_query, invalid_query, stats = apply_sql_validation_rules(
            source_query,
            step.rules,
            step.name,
            self.session,
        )

        # Store validated query in context for downstream steps
        context[f"{step.name}_validated"] = valid_query

        return {
            "rows_processed": stats.total_rows,
            "rows_written": 0,  # Bronze doesn't write
            "validation_rate": stats.validation_rate,
            "table_fqn": None,
            "write_mode": None,
            "input_rows": stats.total_rows,
        }

    def _execute_silver_step(
        self,
        step: SqlSilverStep,
        context: Dict[str, Any],
        mode: ExecutionMode,
    ) -> Dict[str, Any]:
        """Execute a silver step."""
        if step.model_class is None:
            raise ValueError(
                f"Silver step '{step.name}' requires a model_class to create its table"
            )
        # Get bronze source
        bronze_key = f"{step.source_bronze}_validated"
        if bronze_key not in context:
            raise ValueError(
                f"Bronze source '{step.source_bronze}' not found in context"
            )

        bronze_query = context[bronze_key]

        # Transform
        silvers_dict = {}  # No prior silvers for silver steps
        transformed_query = step.transform(self.session, bronze_query, silvers_dict)

        # Apply validation
        valid_query, invalid_query, stats = apply_sql_validation_rules(
            transformed_query,
            step.rules,
            step.name,
            self.session,
        )

        # Write to table
        schema = step.schema or self.config.schema
        write_mode = "overwrite" if mode == ExecutionMode.INITIAL else "append"

        from sql_pipeline_builder.table_operations import write_table

        drop_existing = mode == ExecutionMode.INITIAL
        rows_written = write_table(
            self.session,
            valid_query,
            schema,
            step.table_name,
            write_mode,
            step.model_class,
            drop_existing_table=drop_existing,
        )

        # Store in context for gold steps - read from the table that was just written
        # This ensures gold steps query from the actual table, not the original query

        context[step.name] = read_table(
            self.session,
            schema,
            step.table_name,
            step.model_class,
        )

        return {
            "rows_processed": stats.total_rows,
            "rows_written": rows_written,
            "validation_rate": stats.validation_rate,
            "table_fqn": f"{schema}.{step.table_name}",
            "write_mode": write_mode,
            "input_rows": stats.total_rows,
        }

    def _execute_gold_step(
        self,
        step: SqlGoldStep,
        context: Dict[str, Any],
        mode: ExecutionMode,
    ) -> Dict[str, Any]:
        """Execute a gold step."""
        if step.model_class is None:
            raise ValueError(
                f"Gold step '{step.name}' requires a model_class to create its table"
            )
        # Get silver sources
        if step.source_silvers:
            silvers_dict = {
                name: context[name] for name in step.source_silvers if name in context
            }
        else:
            # Use all available silvers
            silvers_dict = {
                key: value
                for key, value in context.items()
                if isinstance(key, str) and not key.endswith("_validated")
            }

        if not silvers_dict:
            raise ValueError(f"No silver sources available for gold step '{step.name}'")

        # Transform
        transformed_query = step.transform(self.session, silvers_dict)

        # Apply validation
        valid_query, invalid_query, stats = apply_sql_validation_rules(
            transformed_query,
            step.rules,
            step.name,
            self.session,
        )

        # Write to table (always overwrite for gold)
        schema = step.schema or self.config.schema

        from sql_pipeline_builder.table_operations import write_table

        rows_written = write_table(
            self.session,
            valid_query,
            schema,
            step.table_name,
            "overwrite",  # Gold always overwrites
            step.model_class,
            drop_existing_table=True,
        )

        return {
            "rows_processed": stats.total_rows,
            "rows_written": rows_written,
            "validation_rate": stats.validation_rate,
            "table_fqn": f"{schema}.{step.table_name}",
            "write_mode": "overwrite",
            "input_rows": sum(
                q.count() if hasattr(q, "count") else 0 for q in silvers_dict.values()
            ),
        }

    def execute_pipeline(
        self,
        bronze_steps: Dict[str, SqlBronzeStep],
        silver_steps: Dict[str, SqlSilverStep],
        gold_steps: Dict[str, SqlGoldStep],
        bronze_sources: Dict[str, Any],  # Dict[str, Query]
        mode: ExecutionMode = ExecutionMode.INITIAL,
    ) -> ExecutionResult:
        """
        Execute entire pipeline with dependency-aware execution.

        Args:
            bronze_steps: Dictionary of bronze steps
            silver_steps: Dictionary of silver steps
            gold_steps: Dictionary of gold steps
            bronze_sources: Dictionary of bronze source queries
            mode: Execution mode

        Returns:
            ExecutionResult with execution results
        """

        start_time = datetime.now(timezone.utc)
        context = ExecutionContext(
            mode=mode,
            start_time=start_time,
            schema=self.config.schema,
        )

        # Initialize context with bronze sources
        execution_context: Dict[str, Any] = bronze_sources.copy()

        # Analyze dependencies
        analyzer = DependencyAnalyzer(logger=self.logger)
        # Cast to protocol types for type checking - SQL steps implement these protocols
        analysis = analyzer.analyze_dependencies(
            bronze_steps=cast(Optional[Dict[str, BronzeStepProtocol]], bronze_steps) if bronze_steps else None,
            silver_steps=cast(Optional[Dict[str, SilverStepProtocol]], silver_steps) if silver_steps else None,
            gold_steps=cast(Optional[Dict[str, GoldStepProtocol]], gold_steps) if gold_steps else None,
        )

        execution_groups = analysis.execution_groups
        step_results: list[StepResult] = []

        # Execute steps in dependency order
        for group in execution_groups:
            if self.is_async and self.config.parallel.enabled:
                # Parallel async execution
                step_results.extend(
                    self._execute_group_async(
                        group,
                        bronze_steps,
                        silver_steps,
                        gold_steps,
                        execution_context,
                        mode,
                    )
                )
            else:
                # Sequential execution
                for step_name in group:
                    step = (
                        bronze_steps.get(step_name)
                        or silver_steps.get(step_name)
                        or gold_steps.get(step_name)
                    )
                    if step:
                        result = self.execute_step(step, execution_context, mode)
                        step_results.append(result)

        context.finish()

        # Create metrics
        metrics = PipelineMetrics.from_step_results(step_results)
        success = all(result.success for result in step_results)

        return ExecutionResult(
            context=context,
            step_results=step_results,
            metrics=metrics,
            success=success,
        )

    def _execute_group_async(
        self,
        group: list[str],
        bronze_steps: Dict[str, SqlBronzeStep],
        silver_steps: Dict[str, SqlSilverStep],
        gold_steps: Dict[str, SqlGoldStep],
        execution_context: Dict[str, Any],
        mode: ExecutionMode,
    ) -> list[StepResult]:
        """Execute a group of steps in parallel using async."""
        # For now, fall back to sequential
        # Full async implementation would require async/await throughout
        self.logger.warning(
            "Async parallel execution not fully implemented, using sequential"
        )
        results = []
        for step_name in group:
            step = (
                bronze_steps.get(step_name)
                or silver_steps.get(step_name)
                or gold_steps.get(step_name)
            )
            if step:
                result = self.execute_step(step, execution_context, mode)
                results.append(result)
        return results
