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

import inspect
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger

from ..compat import DataFrame, SparkSession
from ..functions import FunctionsProtocol
from ..table_operations import fqn, table_exists


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

    @staticmethod
    def _accepts_params(func: Any) -> bool:
        """Check if a function accepts a 'params' argument or **kwargs.

        Args:
            func: The function to inspect.

        Returns:
            True if the function accepts 'params' as a named argument or **kwargs,
            False otherwise.
        """
        try:
            sig = inspect.signature(func)
            for param_name, param in sig.parameters.items():
                if param_name == "params":
                    return True
                if param.kind == inspect.Parameter.VAR_KEYWORD:  # **kwargs
                    return True
            return False
        except (ValueError, TypeError):
            # If we can't inspect the signature, assume it doesn't accept params
            return False

    def _handle_validation_only_step(
        self, step: Any, step_type: str
    ) -> Optional[DataFrame]:
        """Handle validation-only steps by reading from existing table.

        This method checks if a step is validation-only (transform=None, existing=True)
        and if so, reads the data from the existing table. Returns None if the step
        is not validation-only.

        Args:
            step: Step instance (SilverStep or GoldStep).
            step_type: Step type string for error messages ("silver" or "gold").

        Returns:
            DataFrame if step is validation-only and table exists, None otherwise.

        Raises:
            ExecutionError: If schema doesn't exist, table doesn't exist, or
                reading fails.
        """
        # Check if this is a validation-only step
        if not (hasattr(step, "transform") and step.transform is None and
                hasattr(step, "existing") and step.existing):
            return None  # Not a validation-only step

        table_name = getattr(step, "table_name", step.name)
        schema = getattr(step, "schema", None)

        if schema is None:
            raise ExecutionError(
                f"Validation-only {step_type} step '{step.name}' requires schema to read from table"
            )

        # Validate schema exists before checking table
        try:
            databases = [db.name for db in self.spark.catalog.listDatabases()]  # type: ignore[attr-defined]
            if schema not in databases:
                raise ExecutionError(
                    f"Validation-only {step_type} step '{step.name}' requires schema '{schema}', but schema does not exist. "
                    f"Available schemas: {databases}"
                )
        except ExecutionError:
            raise  # Re-raise ExecutionError
        except Exception as e:
            raise ExecutionError(
                f"Failed to check if schema '{schema}' exists for validation-only {step_type} step '{step.name}': {e}"
            ) from e

        table_fqn = fqn(schema, table_name)
        try:
            if table_exists(self.spark, table_fqn):
                return self.spark.table(table_fqn)  # type: ignore[attr-defined]
            else:
                raise ExecutionError(
                    f"Validation-only {step_type} step '{step.name}' requires existing table '{table_fqn}', but table does not exist"
                )
        except ExecutionError:
            raise  # Re-raise ExecutionError
        except Exception as e:
            raise ExecutionError(
                f"Failed to read table '{table_fqn}' for validation-only {step_type} step '{step.name}': {e}"
            ) from e

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
