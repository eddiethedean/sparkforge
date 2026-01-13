"""Schema manager for table schema validation and management.

This module provides schema validation and management functionality. The
SchemaManager handles schema creation, retrieval, and validation operations
for pipeline tables.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.logging import PipelineLogger

from ..compat import SparkSession
from .schema_utils import get_existing_schema_safe, schemas_match


class SchemaManager:
    """Manages schema validation and operations.

    Handles schema existence checks, validation, and schema matching. Provides
    centralized schema management for the pipeline execution engine.

    Attributes:
        spark: SparkSession instance for schema operations.
        logger: PipelineLogger instance for logging.

    Example:
        >>> from pipeline_builder.storage.schema_manager import SchemaManager
        >>> from pipeline_builder.compat import SparkSession
        >>>
        >>> manager = SchemaManager(spark)
        >>> manager.ensure_schema_exists("analytics")
        >>> schema = manager.get_table_schema("analytics.events")
        >>> matches, differences = manager.validate_schema_match(
        ...     "analytics.events", output_schema, ExecutionMode.INCREMENTAL, "clean_events"
        ... )
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the schema manager.

        Args:
            spark: Active SparkSession instance for schema operations.
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.spark = spark
        self.logger = logger or PipelineLogger()

    def ensure_schema_exists(self, schema: str) -> None:
        """Ensure a schema exists, creating it if necessary.

        Checks if schema exists in catalog, and creates it if it doesn't.
        Uses idempotent CREATE SCHEMA IF NOT EXISTS for safe creation.

        Args:
            schema: Schema name to create or verify.

        Raises:
            ExecutionError: If schema creation fails after all attempts.
        """
        # Check if schema already exists
        try:
            databases = [db.name for db in self.spark.catalog.listDatabases()]
            if schema in databases:
                return  # Schema already exists, nothing to do
        except Exception:
            pass  # If we can't check, try to create anyway

        try:
            # Use SQL CREATE SCHEMA (works for both PySpark and mock-spark)
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            # Verify it was created
            databases = [db.name for db in self.spark.catalog.listDatabases()]
            if schema not in databases:
                raise ExecutionError(
                    f"Schema '{schema}' creation via SQL failed - schema not in catalog. "
                    f"Available databases: {databases}"
                )
        except ExecutionError:
            raise  # Re-raise ExecutionError
        except Exception as e:
            # Wrap other exceptions
            raise ExecutionError(f"Failed to create schema '{schema}': {str(e)}") from e

    def get_table_schema(
        self,
        table_name: str,
        refresh: bool = False,
    ) -> Optional[Any]:
        """
        Get the schema of an existing table.

        Args:
            table_name: Fully qualified table name
            refresh: Whether to refresh table metadata before reading schema

        Returns:
            StructType schema if table exists and schema is readable, None otherwise
        """
        if refresh:
            try:
                self.spark.sql(f"REFRESH TABLE {table_name}")
            except Exception as refresh_error:
                # Refresh might fail for some table types - log but continue
                self.logger.debug(
                    f"Could not refresh table {table_name} before schema read: {refresh_error}"
                )

        return get_existing_schema_safe(self.spark, table_name)

    def validate_schema_match(
        self,
        table_name: str,
        output_schema: Any,
        mode: Any,
        step_name: str,
    ) -> Tuple[bool, list[str]]:
        """
        Validate that output schema matches existing table schema.

        Args:
            table_name: Fully qualified table name
            output_schema: Schema of the output DataFrame
            mode: Execution mode
            step_name: Name of the step being validated

        Returns:
            Tuple of (matches: bool, differences: list[str])

        Raises:
            ExecutionError: If schema cannot be read or doesn't match (depending on mode)
        """
        existing_schema = self.get_table_schema(table_name, refresh=True)

        if existing_schema is None:
            # Cannot read schema - raise error
            raise ExecutionError(
                f"Cannot read schema for table '{table_name}' in {mode.value} mode. "
                "Schema validation is required for INCREMENTAL and FULL_REFRESH modes.",
                context={
                    "step_name": step_name,
                    "table": table_name,
                    "mode": mode.value,
                },
                suggestions=[
                    "Ensure the table exists and is accessible",
                    "Check that the table schema is readable",
                    "Use INITIAL mode if you need to recreate the table",
                ],
            )

        # If catalog reports empty schema, treat as mismatch with explicit guidance
        schema_is_empty = not existing_schema.fields or len(existing_schema.fields) == 0
        if schema_is_empty:
            raise ExecutionError(
                f"Schema mismatch for table '{table_name}' in {mode.value} mode. "
                f"Catalog reports empty schema (struct<>), but output schema has {len(output_schema.fields)} fields: {[f.name for f in output_schema.fields]}. "
                f"Use INITIAL mode to recreate the table or provide schema_override explicitly.",
                context={
                    "step_name": step_name,
                    "table": table_name,
                    "mode": mode.value,
                    "existing_schema": "struct<> (empty - catalog sync issue)",
                    "output_schema": str(output_schema),
                },
                suggestions=[
                    "Run initial_load/full_refresh to recreate the table with the desired schema",
                    "Provide schema_override to force the schema in allowed modes",
                ],
            )

        matches, differences = schemas_match(existing_schema, output_schema)

        if not matches:
            raise ExecutionError(
                f"Schema mismatch for table '{table_name}' in {mode.value} mode. "
                f"Schema changes are only allowed in INITIAL mode.\n"
                f"{chr(10).join(differences)}\n\n"
                f"Existing table schema: {existing_schema}\n"
                f"Output DataFrame schema: {output_schema}",
                context={
                    "step_name": step_name,
                    "table": table_name,
                    "mode": mode.value,
                    "existing_schema": str(existing_schema),
                    "output_schema": str(output_schema),
                },
                suggestions=[
                    "Ensure the output schema matches the existing table schema exactly",
                    "Run with INITIAL mode to recreate the table with the new schema",
                    "Manually update the existing table schema to match the new schema",
                ],
            )

        return matches, differences
