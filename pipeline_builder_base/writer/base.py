"""
Base LogWriter class for pipeline builders.

This module provides an abstract base class for LogWriter implementations
that can be used by both Spark and SQL pipeline builders.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from ..logging import PipelineLogger
from ..models import ExecutionResult
from .models import LogRow, WriteMode, WriterConfig, WriterMetrics, create_log_rows_from_execution_result


class BaseLogWriter(ABC):
    """
    Abstract base class for LogWriter implementations.

    This class defines the interface that all LogWriter implementations
    must follow, while allowing engine-specific implementations for
    storage operations.

    Subclasses must implement:
    - _write_log_rows() - Engine-specific write operation
    - _read_log_table() - Engine-specific read operation
    - _table_exists() - Engine-specific table existence check
    - _create_table() - Engine-specific table creation
    """

    def __init__(
        self,
        schema: str,
        table_name: str,
        config: WriterConfig | None = None,
        logger: PipelineLogger | None = None,
    ) -> None:
        """
        Initialize the base LogWriter.

        Args:
            schema: Database schema name
            table_name: Table name
            config: Writer configuration (optional)
            logger: Pipeline logger (optional)
        """
        self.schema = schema
        self.table_name = table_name
        self.logger = logger or PipelineLogger()
        
        # Create config from schema/table_name if not provided
        if config is None:
            from .models import WriteMode
            config = WriterConfig(
                table_schema=schema,
                table_name=table_name,
                write_mode=WriteMode.APPEND,
            )
        self.config = config
        self.config.validate()

    @property
    def table_fqn(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.table_name}"

    def create_table(self, execution_result: ExecutionResult) -> None:
        """
        Create the log table from the first execution result.

        Args:
            execution_result: The execution result to create table from
        """
        if self._table_exists():
            self.logger.warning(f"Table {self.table_fqn} already exists, skipping creation")
            return

        self.logger.info(f"Creating log table {self.table_fqn}")
        log_rows = create_log_rows_from_execution_result(
            execution_result,
            run_id=execution_result.context.run_id,
            run_mode=execution_result.context.run_mode,
        )
        
        if not log_rows:
            self.logger.warning("No log rows to create table from")
            return

        self._create_table(log_rows)
        self._write_log_rows(log_rows, WriteMode.APPEND)

    def append(self, execution_result: ExecutionResult) -> WriterMetrics:
        """
        Append execution result to the log table.

        Args:
            execution_result: The execution result to append

        Returns:
            Writer metrics
        """
        if not self._table_exists():
            self.logger.warning(f"Table {self.table_fqn} does not exist, creating it")
            self.create_table(execution_result)
            return self._get_metrics()

        log_rows = create_log_rows_from_execution_result(
            execution_result,
            run_id=execution_result.context.run_id,
            run_mode=execution_result.context.run_mode,
        )

        if not log_rows:
            self.logger.warning("No log rows to append")
            return self._get_metrics()

        self._write_log_rows(log_rows, WriteMode.APPEND)
        return self._get_metrics()

    def write(self, execution_result: ExecutionResult, mode: WriteMode = WriteMode.APPEND) -> WriterMetrics:
        """
        Write execution result to the log table.

        Args:
            execution_result: The execution result to write
            mode: Write mode (APPEND or OVERWRITE)

        Returns:
            Writer metrics
        """
        if mode == WriteMode.OVERWRITE or not self._table_exists():
            if not self._table_exists():
                self.logger.info(f"Table {self.table_fqn} does not exist, creating it")
                self.create_table(execution_result)
            else:
                self.logger.info(f"Overwriting table {self.table_fqn}")
                # For overwrite, we need to clear the table first
                # This is engine-specific, so subclasses should override if needed
                self._write_log_rows([], WriteMode.OVERWRITE)

        log_rows = create_log_rows_from_execution_result(
            execution_result,
            run_id=execution_result.context.run_id,
            run_mode=execution_result.context.run_mode,
        )

        if not log_rows:
            self.logger.warning("No log rows to write")
            return self._get_metrics()

        self._write_log_rows(log_rows, mode)
        return self._get_metrics()

    def read(self, limit: int | None = None) -> list[LogRow]:
        """
        Read log rows from the table.

        Args:
            limit: Maximum number of rows to read (None for all)

        Returns:
            List of log rows
        """
        if not self._table_exists():
            self.logger.warning(f"Table {self.table_fqn} does not exist")
            return []

        return self._read_log_table(limit)

    # Abstract methods that must be implemented by subclasses

    @abstractmethod
    def _write_log_rows(self, log_rows: list[LogRow], mode: WriteMode) -> None:
        """
        Write log rows to the storage system.

        This is an engine-specific operation that must be implemented
        by subclasses.

        Args:
            log_rows: List of log rows to write
            mode: Write mode
        """
        pass

    @abstractmethod
    def _read_log_table(self, limit: int | None = None) -> list[LogRow]:
        """
        Read log rows from the storage system.

        This is an engine-specific operation that must be implemented
        by subclasses.

        Args:
            limit: Maximum number of rows to read (None for all)

        Returns:
            List of log rows
        """
        pass

    @abstractmethod
    def _table_exists(self) -> bool:
        """
        Check if the log table exists.

        This is an engine-specific operation that must be implemented
        by subclasses.

        Returns:
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def _create_table(self, sample_rows: list[LogRow]) -> None:
        """
        Create the log table with appropriate schema.

        This is an engine-specific operation that must be implemented
        by subclasses.

        Args:
            sample_rows: Sample log rows to infer schema from
        """
        pass

    def _get_metrics(self) -> WriterMetrics:
        """
        Get writer metrics.

        This is a default implementation that can be overridden
        by subclasses for more detailed metrics.

        Returns:
            Writer metrics
        """
        return {
            "total_writes": 1,
            "successful_writes": 1,
            "failed_writes": 0,
            "total_duration_secs": 0.0,
            "avg_write_duration_secs": 0.0,
            "total_rows_written": 0,
            "memory_usage_peak_mb": 0.0,
        }

