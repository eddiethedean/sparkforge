"""Table service for table operations.

This module provides centralized table operations including existence checks,
schema management, and table lifecycle operations. The TableService acts as
a facade for table-related operations, delegating to SchemaManager for
schema-specific functionality.
"""

from __future__ import annotations

from typing import Any, Optional

from pipeline_builder_base.logging import PipelineLogger

from ..compat import SparkSession
from ..table_operations import fqn, table_exists
from .schema_manager import SchemaManager


class TableService:
    """Service for table operations.

    Centralizes all table-related operations including existence checks,
    schema management, and table lifecycle operations. Acts as a facade
    for table operations, delegating schema management to SchemaManager.

    Attributes:
        spark: SparkSession instance for table operations.
        logger: PipelineLogger instance for logging.
        schema_manager: SchemaManager instance for schema operations.

    Example:
        >>> from pipeline_builder.storage.table_service import TableService
        >>> from pipeline_builder.compat import SparkSession
        >>>
        >>> service = TableService(spark)
        >>> service.ensure_schema_exists("analytics")
        >>> exists = service.table_exists("analytics.events")
        >>> schema = service.get_table_schema("analytics.events")
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the table service.

        Args:
            spark: Active SparkSession instance for table operations.
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.spark = spark
        self.logger = logger or PipelineLogger()
        self.schema_manager = SchemaManager(spark, logger)

    def ensure_schema_exists(self, schema: str) -> None:
        """Ensure a schema exists, creating it if necessary.

        Delegates to SchemaManager to create the schema if it doesn't exist.
        Uses idempotent CREATE SCHEMA IF NOT EXISTS.

        Args:
            schema: Schema name to create or verify.

        Raises:
            ExecutionError: If schema creation fails after all attempts.
        """
        self.schema_manager.ensure_schema_exists(schema)

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Fully qualified table name (schema.table).

        Returns:
            True if table exists, False otherwise.
        """
        return table_exists(self.spark, table_name)

    def get_table_schema(
        self,
        table_name: str,
        refresh: bool = False,
    ) -> Optional[Any]:
        """Get the schema of an existing table.

        Retrieves the StructType schema of an existing table, optionally
        refreshing table metadata first to ensure accurate schema information.

        Args:
            table_name: Fully qualified table name (schema.table).
            refresh: Whether to refresh table metadata before reading schema.
                Defaults to False.

        Returns:
            StructType schema if table exists and schema is readable, None
            otherwise. May return empty struct<> if catalog sync issues occur.
        """
        return self.schema_manager.get_table_schema(table_name, refresh)

    def validate_schema_match(
        self,
        table_name: str,
        output_schema: Any,
        mode: Any,
        step_name: str,
    ) -> tuple[bool, list[str]]:
        """Validate that output schema matches existing table schema.

        Validates that the output DataFrame schema matches the existing table
        schema. Required for INCREMENTAL and FULL_REFRESH modes to prevent
        schema drift.

        Args:
            table_name: Fully qualified table name (schema.table).
            output_schema: StructType schema of the output DataFrame.
            mode: ExecutionMode enum value.
            step_name: Name of the step being validated (for error messages).

        Returns:
            Tuple of (matches: bool, differences: list[str]) where:
            - matches: True if schemas match, False otherwise
            - differences: List of human-readable mismatch descriptions

        Raises:
            ExecutionError: If schema cannot be read or doesn't match (for
                INCREMENTAL/FULL_REFRESH modes).
        """
        return self.schema_manager.validate_schema_match(
            table_name, output_schema, mode, step_name
        )

    def drop_table_if_exists(self, table_name: str) -> None:
        """Drop a table if it exists.

        Safely drops a table, handling cases where the table doesn't exist.
        Errors are logged but not raised.

        Args:
            table_name: Fully qualified table name (schema.table).

        Note:
            Uses DROP TABLE IF EXISTS for idempotent operation. Errors are
            logged at debug level but not raised.
        """
        try:
            if self.table_exists(table_name):
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            self.logger.debug(f"Could not drop table {table_name}: {e}")

    def refresh_table(self, table_name: str) -> None:
        """Refresh table metadata.

        Refreshes Spark catalog metadata for a table, ensuring subsequent
        operations see the latest schema and data.

        Args:
            table_name: Fully qualified table name (schema.table).

        Note:
            Uses REFRESH TABLE SQL command. Errors are logged at debug level
            but not raised, as some table types may not support refresh.
        """
        try:
            self.spark.sql(f"REFRESH TABLE {table_name}")
        except Exception as refresh_error:
            # Refresh might fail for some table types - log but continue
            self.logger.debug(f"Could not refresh table {table_name}: {refresh_error}")

    def fqn(self, schema: str, table: str) -> str:
        """Create a fully qualified table name.

        Combines schema and table names into a fully qualified table name
        (schema.table).

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            Fully qualified table name in format "schema.table".
        """
        return fqn(schema, table)
