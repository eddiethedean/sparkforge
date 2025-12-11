"""
SQL LogWriter implementation for SQL databases.

This module provides a SQLAlchemy-based implementation of the LogWriter
for tracking pipeline executions in SQL databases.
"""

from __future__ import annotations

from typing import Any, Optional

from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.writer import BaseLogWriter
from pipeline_builder_base.writer.models import (
    LogRow,
    WriteMode,
    WriterConfig,
)

from sql_pipeline_builder.table_operations import table_exists


class SqlLogWriter(BaseLogWriter):
    """
    SQL LogWriter for tracking pipeline executions in SQL databases.

    This implementation uses SQLAlchemy to store log rows in SQL tables,
    supporting both sync and async SQLAlchemy engines.
    """

    def __init__(
        self,
        session: Any,  # SQLAlchemy Session or AsyncSession
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        config: Optional[WriterConfig] = None,
        logger: Optional[PipelineLogger] = None,
    ) -> None:
        """
        Initialize the SQL LogWriter.

        Args:
            session: SQLAlchemy Session or AsyncSession instance
            schema: Database schema name (simplified API)
            table_name: Table name (simplified API)
            config: Writer configuration (optional)
            logger: Pipeline logger (optional)
        """
        if schema is None or table_name is None:
            if config is None:
                raise ValueError(
                    "Either (schema, table_name) or config must be provided"
                )
            schema = config.table_schema
            table_name = config.table_name

        super().__init__(schema, table_name, config, logger)
        self.session = session

    def _write_log_rows(self, log_rows: list[LogRow], mode: WriteMode) -> None:
        """
        Write log rows to SQL table.

        Args:
            log_rows: List of log rows to write
            mode: Write mode
        """
        try:
            from sqlalchemy import (
                Boolean,
                Column,
                DateTime,
                Float,
                Integer,
                MetaData,
                String,
                Table,
                Text,
            )

            # Create table if it doesn't exist
            if not self._table_exists():
                self._create_table(log_rows)

            # Get or create table object
            metadata = MetaData()
            log_table = Table(
                self.table_name,
                metadata,
                Column("run_id", String, primary_key=True),
                Column("run_mode", String),
                Column("run_started_at", DateTime),
                Column("run_ended_at", DateTime),
                Column("execution_id", String),
                Column("pipeline_id", String),
                Column("schema", String),
                Column("phase", String),
                Column("step_name", String),
                Column("step_type", String),
                Column("start_time", DateTime),
                Column("end_time", DateTime),
                Column("duration_secs", Float),
                Column("table_fqn", String),
                Column("write_mode", String),
                Column("input_rows", Integer),
                Column("output_rows", Integer),
                Column("rows_written", Integer),
                Column("rows_processed", Integer),
                Column("table_total_rows", Integer),
                Column("valid_rows", Integer),
                Column("invalid_rows", Integer),
                Column("validation_rate", Float),
                Column("success", Boolean),
                Column("error_message", Text),
                Column("memory_usage_mb", Float),
                Column("cpu_usage_percent", Float),
                Column("metadata", Text),  # JSON string
                schema=self.schema,
                autoload_with=self.session.bind
                if hasattr(self.session, "bind")
                else None,
            )

            # Prepare data for insertion
            insert_data = []
            for row in log_rows:
                # Convert metadata dict to JSON string
                import json

                metadata_json = json.dumps(row.get("metadata", {}))

                insert_data.append(
                    {
                        "run_id": row.get("run_id"),
                        "run_mode": row.get("run_mode"),
                        "run_started_at": row.get("run_started_at"),
                        "run_ended_at": row.get("run_ended_at"),
                        "execution_id": row.get("execution_id"),
                        "pipeline_id": row.get("pipeline_id"),
                        "schema": row.get("schema"),
                        "phase": row.get("phase"),
                        "step_name": row.get("step_name"),
                        "step_type": row.get("step_type"),
                        "start_time": row.get("start_time"),
                        "end_time": row.get("end_time"),
                        "duration_secs": row.get("duration_secs", 0.0),
                        "table_fqn": row.get("table_fqn"),
                        "write_mode": row.get("write_mode"),
                        "input_rows": row.get("input_rows"),
                        "output_rows": row.get("output_rows"),
                        "rows_written": row.get("rows_written"),
                        "rows_processed": row.get("rows_processed", 0),
                        "table_total_rows": row.get("table_total_rows"),
                        "valid_rows": row.get("valid_rows", 0),
                        "invalid_rows": row.get("invalid_rows", 0),
                        "validation_rate": row.get("validation_rate", 0.0),
                        "success": row.get("success", False),
                        "error_message": row.get("error_message"),
                        "memory_usage_mb": row.get("memory_usage_mb"),
                        "cpu_usage_percent": row.get("cpu_usage_percent"),
                        "metadata": metadata_json,
                    }
                )

            if mode == WriteMode.OVERWRITE:
                # Delete existing data
                self.session.execute(log_table.delete())
                self.session.commit()

            # Insert new data
            self.session.execute(log_table.insert(), insert_data)
            self.session.commit()

            self.logger.info(
                f"Successfully wrote {len(log_rows)} log rows to {self.table_fqn}"
            )

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Failed to write log rows: {e}")
            raise

    def _read_log_table(self, limit: Optional[int] = None) -> list[LogRow]:
        """
        Read log rows from SQL table.

        Args:
            limit: Maximum number of rows to read (None for all)

        Returns:
            List of log rows
        """
        try:
            import json

            from sqlalchemy import MetaData, Table, select

            metadata = MetaData()
            log_table = Table(
                self.table_name,
                metadata,
                schema=self.schema,
                autoload_with=self.session.bind
                if hasattr(self.session, "bind")
                else None,
            )

            query = select(log_table)
            if limit:
                query = query.limit(limit)

            result = self.session.execute(query)
            rows = result.fetchall()

            # Convert to LogRow format
            log_rows = []
            for row in rows:
                row_dict = dict(row._mapping)
                # Parse metadata JSON string
                metadata_str = row_dict.get("metadata", "{}")
                try:
                    metadata_dict = json.loads(metadata_str) if metadata_str else {}
                except Exception:
                    metadata_dict = {}

                log_row: LogRow = {
                    "run_id": row_dict.get("run_id", ""),
                    "run_mode": row_dict.get("run_mode", "initial"),
                    "run_started_at": row_dict.get("run_started_at"),
                    "run_ended_at": row_dict.get("run_ended_at"),
                    "execution_id": row_dict.get("execution_id", ""),
                    "pipeline_id": row_dict.get("pipeline_id", ""),
                    "schema": row_dict.get("schema", ""),
                    "phase": row_dict.get("phase", "pipeline"),  # type: ignore
                    "step_name": row_dict.get("step_name", ""),
                    "step_type": row_dict.get("step_type", ""),
                    "start_time": row_dict.get("start_time"),
                    "end_time": row_dict.get("end_time"),
                    "duration_secs": row_dict.get("duration_secs", 0.0),
                    "table_fqn": row_dict.get("table_fqn"),
                    "write_mode": row_dict.get("write_mode"),  # type: ignore
                    "input_rows": row_dict.get("input_rows"),
                    "output_rows": row_dict.get("output_rows"),
                    "rows_written": row_dict.get("rows_written"),
                    "rows_processed": row_dict.get("rows_processed", 0),
                    "table_total_rows": row_dict.get("table_total_rows"),
                    "valid_rows": row_dict.get("valid_rows", 0),
                    "invalid_rows": row_dict.get("invalid_rows", 0),
                    "validation_rate": row_dict.get("validation_rate", 0.0),
                    "success": row_dict.get("success", False),
                    "error_message": row_dict.get("error_message"),
                    "memory_usage_mb": row_dict.get("memory_usage_mb"),
                    "cpu_usage_percent": row_dict.get("cpu_usage_percent"),
                    "metadata": metadata_dict,
                }
                log_rows.append(log_row)

            return log_rows

        except Exception as e:
            self.logger.error(f"Failed to read log table: {e}")
            return []

    def _table_exists(self) -> bool:
        """
        Check if the log table exists.

        Returns:
            True if table exists, False otherwise
        """
        return table_exists(self.session, self.schema, self.table_name)

    def _create_table(self, sample_rows: list[LogRow]) -> None:
        """
        Create the log table with appropriate schema.

        Args:
            sample_rows: Sample log rows to infer schema from
        """
        try:
            from sqlalchemy import (
                Boolean,
                Column,
                DateTime,
                Float,
                Integer,
                MetaData,
                String,
                Table,
                Text,
            )

            metadata = MetaData()

            # Create table with all required columns
            Table(
                self.table_name,
                metadata,
                Column("run_id", String(255), primary_key=True),
                Column("run_mode", String(50)),
                Column("run_started_at", DateTime),
                Column("run_ended_at", DateTime),
                Column("execution_id", String(255)),
                Column("pipeline_id", String(255)),
                Column("schema", String(255)),
                Column("phase", String(50)),
                Column("step_name", String(255)),
                Column("step_type", String(100)),
                Column("start_time", DateTime),
                Column("end_time", DateTime),
                Column("duration_secs", Float),
                Column("table_fqn", String(500)),
                Column("write_mode", String(50)),
                Column("input_rows", Integer),
                Column("output_rows", Integer),
                Column("rows_written", Integer),
                Column("rows_processed", Integer),
                Column("table_total_rows", Integer),
                Column("valid_rows", Integer),
                Column("invalid_rows", Integer),
                Column("validation_rate", Float),
                Column("success", Boolean),
                Column("error_message", Text),
                Column("memory_usage_mb", Float),
                Column("cpu_usage_percent", Float),
                Column("metadata", Text),  # JSON string
                schema=self.schema,
            )

            # Create schema if needed
            from sql_pipeline_builder.table_operations import (
                create_schema_if_not_exists,
            )

            create_schema_if_not_exists(self.session, self.schema)

            # Create table
            metadata.create_all(
                self.session.bind if hasattr(self.session, "bind") else None
            )
            self.session.commit()

            self.logger.info(f"Created log table {self.table_fqn}")

        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Failed to create log table: {e}")
            raise
