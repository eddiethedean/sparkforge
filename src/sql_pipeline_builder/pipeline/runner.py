"""
SQL pipeline runner for the framework.

This module provides a clean, focused SQL pipeline runner that delegates
execution to the SQL execution engine.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Union

from abstracts.runner import Runner
from abstracts.source import Source
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ExecutionMode,
    ExecutionResult,
    PipelineConfig,
)
from pipeline_builder_base.runner import BaseRunner

from sql_pipeline_builder.execution import SqlExecutionEngine
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep


class SqlPipelineRunner(BaseRunner, Runner):
    """
    SQL pipeline runner that delegates to the SQL execution engine.

    This runner focuses on orchestration and reporting, delegating
    actual execution to the SqlExecutionEngine.
    """

    def __init__(
        self,
        session: Any,  # SQLAlchemy Session or AsyncSession
        config: PipelineConfig,
        bronze_steps: Optional[Dict[str, SqlBronzeStep]] = None,
        silver_steps: Optional[Dict[str, SqlSilverStep]] = None,
        gold_steps: Optional[Dict[str, SqlGoldStep]] = None,
        logger: Optional[PipelineLogger] = None,
        steps: Optional[list[SqlBronzeStep | SqlSilverStep | SqlGoldStep]] = None,
        engine: Optional[Any] = None,  # For abstracts.Runner compatibility
    ):
        """
        Initialize the SQL pipeline runner.

        Args:
            session: SQLAlchemy Session or AsyncSession instance
            config: Pipeline configuration
            bronze_steps: Bronze steps dictionary
            silver_steps: Silver steps dictionary
            gold_steps: Gold steps dictionary
            logger: Optional logger instance
            steps: Optional list of steps (for abstracts.Runner compatibility)
            engine: Optional engine (for abstracts.Runner compatibility, ignored)
        """
        # Initialize BaseRunner first
        super().__init__(config, logger=logger)

        # Initialize abstracts.Runner with empty lists

        class _DummyEngine:
            pass

        dummy_engine: Any = _DummyEngine()
        Runner.__init__(self, steps=[], engine=engine or dummy_engine)

        self.session = session
        self.bronze_steps = bronze_steps or {}
        self.silver_steps = silver_steps or {}
        self.gold_steps = gold_steps or {}
        self.execution_engine = SqlExecutionEngine(session, config, self.logger)

        # If steps provided (from abstracts interface), convert to step dictionaries
        if steps:
            for step in steps:
                if isinstance(step, SqlBronzeStep):
                    self.bronze_steps[step.name] = step
                elif isinstance(step, SqlSilverStep):
                    self.silver_steps[step.name] = step
                elif isinstance(step, SqlGoldStep):
                    self.gold_steps[step.name] = step

    def run_initial_load(
        self,
        bronze_sources: Optional[Dict[str, Any]] = None,  # Dict[str, Query]
    ) -> ExecutionResult:
        """
        Run initial load pipeline execution.

        Args:
            bronze_sources: Dictionary mapping bronze step names to SQLAlchemy Query objects

        Returns:
            ExecutionResult with execution results
        """
        if bronze_sources is None:
            bronze_sources = {}

        return self.execution_engine.execute_pipeline(
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
            bronze_sources=bronze_sources,
            mode=ExecutionMode.INITIAL,
        )

    def run_incremental(
        self,
        bronze_sources: Optional[Dict[str, Source]] = None,
    ) -> Any:  # ExecutionResult
        """
        Run incremental pipeline execution.

        Args:
            bronze_sources: Dictionary mapping bronze step names to Source (Query)

        Returns:
            ExecutionResult with execution results
        """
        if bronze_sources is None:
            bronze_sources = {}

        # Convert Source (Protocol) to Query if needed
        bronze_queries: Dict[str, Any] = {}
        for name, source in bronze_sources.items():
            # Source Protocol is satisfied by Query, so we accept any Query-like object
            if not (hasattr(source, "filter") or hasattr(source, "count")):
                raise TypeError(
                    f"bronze_sources must contain Query-like objects, got {type(source)}"
                )
            bronze_queries[name] = source

        return self.execution_engine.execute_pipeline(
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
            bronze_sources=bronze_queries,
            mode=ExecutionMode.INCREMENTAL,
        )

    def run_pipeline(
        self,
        steps: list[Union[SqlBronzeStep, SqlSilverStep] | SqlGoldStep],
        mode: ExecutionMode,
        bronze_sources: Optional[Dict[str, Any]] = None,
    ) -> ExecutionResult:
        """
        Run pipeline with specified steps and mode.

        Args:
            steps: List of steps to execute
            mode: Execution mode
            bronze_sources: Dictionary mapping bronze step names to Query objects

        Returns:
            ExecutionResult with execution results
        """
        if bronze_sources is None:
            bronze_sources = {}

        # Organize steps by type
        bronze_steps = {s.name: s for s in steps if isinstance(s, SqlBronzeStep)}
        silver_steps = {s.name: s for s in steps if isinstance(s, SqlSilverStep)}
        gold_steps = {s.name: s for s in steps if isinstance(s, SqlGoldStep)}

        return self.execution_engine.execute_pipeline(
            bronze_steps=bronze_steps,
            silver_steps=silver_steps,
            gold_steps=gold_steps,
            bronze_sources=bronze_sources,
            mode=mode,
        )
