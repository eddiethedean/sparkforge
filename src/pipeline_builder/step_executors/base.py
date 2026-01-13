"""Base step executor with common functionality.

This module provides the base class for all step executors with shared logic
and a common interface. Step executors handle the execution of specific step
types (Bronze, Silver, Gold) in the pipeline execution engine.

The base class provides:
    - Common initialization with SparkSession, logger, and functions
    - Abstract execute() method that must be implemented by subclasses
    - Shared infrastructure for all step executors
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pipeline_builder_base.logging import PipelineLogger

from ..compat import DataFrame, SparkSession
from ..functions import FunctionsProtocol


class BaseStepExecutor(ABC):
    """Base class for all step executors.

    Provides common functionality and interface for executing pipeline steps.
    Each step type (Bronze, Silver, Gold) has a specialized executor that
    inherits from this base class.

    Attributes:
        spark: SparkSession instance for DataFrame operations.
        logger: PipelineLogger instance for logging.
        functions: FunctionsProtocol instance for PySpark operations.

    Example:
        >>> from pipeline_builder.step_executors.base import BaseStepExecutor
        >>> from pipeline_builder.compat import SparkSession
        >>>
        >>> class MyStepExecutor(BaseStepExecutor):
        ...     def execute(self, step, context, mode=None):
        ...         # Implementation here
        ...         return output_df
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: Optional[PipelineLogger] = None,
        functions: Optional[FunctionsProtocol] = None,
    ):
        """Initialize the base step executor.

        Args:
            spark: Active SparkSession instance for DataFrame operations.
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
            functions: Optional FunctionsProtocol instance for PySpark
                operations. If None, functions must be provided by subclasses.
        """
        self.spark = spark
        self.logger = logger or PipelineLogger()
        self.functions = functions

    @abstractmethod
    def execute(
        self,
        step: Any,
        context: Dict[str, DataFrame],
        mode: Any = None,
    ) -> DataFrame:
        """Execute a pipeline step.

        Abstract method that must be implemented by subclasses. Each step
        executor implements step-specific execution logic.

        Args:
            step: The step to execute (BronzeStep, SilverStep, or GoldStep).
            context: Dictionary mapping step names to DataFrames. Contains
                source data required for step execution.
            mode: Optional execution mode (ExecutionMode enum). Some executors
                use this for incremental processing.

        Returns:
            Output DataFrame after step execution.

        Raises:
            ExecutionError: If step execution fails or required data is missing.

        Note:
            Subclasses must implement this method with step-specific logic.
            The method should handle data retrieval from context, apply
            transformations, and return the result DataFrame.
        """
        pass
