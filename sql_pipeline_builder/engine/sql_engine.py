"""
SqlEngine implementation of abstracts.Engine.

This engine wraps SQL execution and adapts between the abstracts interface
and the concrete sql_pipeline_builder implementation.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from abstracts.engine import Engine
from abstracts.reports.transform import TransformReport
from abstracts.reports.validation import ValidationReport
from abstracts.reports.write import WriteReport
from abstracts.source import Source
from abstracts.step import Step

from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import PipelineConfig
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep
from sql_pipeline_builder.table_operations import write_table
from sql_pipeline_builder.validation import apply_sql_validation_rules


class SqlEngine(Engine):
    """
    SqlEngine implements abstracts.Engine using SQLAlchemy.

    This engine adapts between the abstracts interface (Step, Source protocols)
    and the concrete sql_pipeline_builder types (SqlBronzeStep/SqlSilverStep/SqlGoldStep, Query).
    """

    def __init__(
        self,
        session: Any,  # SQLAlchemy Session or AsyncSession
        config: PipelineConfig,
        logger: Optional[PipelineLogger] = None,
    ):
        """
        Initialize SqlEngine.

        Args:
            session: SQLAlchemy Session or AsyncSession instance
            config: PipelineConfig instance
            logger: Optional logger instance
        """
        self.session = session
        self.config = config
        self.logger = logger or PipelineLogger()

    def validate_source(self, step: Step, source: Source) -> ValidationReport:
        """
        Validate a data source according to step rules.

        Args:
            step: Step with validation rules
            source: Source data to validate (SQLAlchemy Query)

        Returns:
            ValidationReport with validation results
        """
        # Type check: source should be a Query
        if not hasattr(source, "filter") and not hasattr(source, "count"):
            raise TypeError(f"Source must be a SQLAlchemy Query, got {type(source)}")

        query: Any = source

        # Type check: step should be a concrete step type
        if not isinstance(step, (SqlBronzeStep, SqlSilverStep, SqlGoldStep)):
            raise TypeError(
                f"Step must be SqlBronzeStep, SqlSilverStep, or SqlGoldStep, got {type(step)}"
            )

        # Apply validation rules
        try:
            rules: Any = step.rules
            valid_query, invalid_query, validation_stats = apply_sql_validation_rules(
                query,
                rules,
                step.name,
                self.session,
            )

            valid_rows = valid_query.count() if hasattr(valid_query, "count") else 0
            invalid_rows = invalid_query.count() if hasattr(invalid_query, "count") else 0

            return ValidationReport(
                source=valid_query,  # Return validated source
                valid_rows=valid_rows,
                invalid_rows=invalid_rows,
                error=None,
            )
        except Exception as e:
            return ValidationReport(
                source=query,
                valid_rows=0,
                invalid_rows=query.count() if hasattr(query, "count") else 0,
                error=e,
            )

    def transform_source(self, step: Step, source: Source) -> TransformReport:
        """
        Transform a data source according to step transformation logic.

        Args:
            step: Step with transformation function
            source: Source data to transform (Query or Dict[str, Query])

        Returns:
            TransformReport with transformed source
        """
        # Type check: step should be a concrete step type
        if not isinstance(step, (SqlBronzeStep, SqlSilverStep, SqlGoldStep)):
            raise TypeError(
                f"Step must be SqlBronzeStep, SqlSilverStep, or SqlGoldStep, got {type(step)}"
            )

        try:
            # Bronze steps: no transformation, just return source
            if isinstance(step, SqlBronzeStep):
                return TransformReport(source=source, error=None)

            # Silver steps: transform with bronze query and empty silvers dict
            elif isinstance(step, SqlSilverStep):
                if step.transform is None:
                    raise ValueError(
                        f"Silver step '{step.name}' requires a transform function"
                    )
                # Source should be a Query for silver steps
                if not hasattr(source, "filter"):
                    raise TypeError(
                        f"Silver step '{step.name}' requires a Query source, got {type(source)}"
                    )
                transformed_query = step.transform(self.session, source, {})
                return TransformReport(source=transformed_query, error=None)

            # Gold steps: transform with silvers dict
            elif isinstance(step, SqlGoldStep):
                if step.transform is None:
                    raise ValueError(
                        f"Gold step '{step.name}' requires a transform function"
                    )
                # For gold steps, source should be a dict of silvers (Dict[str, Query])
                if isinstance(source, dict):
                    silvers = source
                else:
                    # If single Query, this is an error for gold steps
                    raise TypeError(
                        f"Gold step '{step.name}' requires a dict of silvers, got {type(source)}"
                    )
                transformed_query = step.transform(self.session, silvers)
                return TransformReport(source=transformed_query, error=None)

            else:
                raise ValueError(f"Unknown step type: {type(step)}")

        except Exception as e:
            return TransformReport(source=source, error=e)

    def write_target(self, step: Step, source: Source) -> WriteReport:
        """
        Write a data source to target table.

        Args:
            step: Step with target configuration
            source: Source data to write (Query)

        Returns:
            WriteReport with write results
        """
        # Type check: source should be a Query
        if not hasattr(source, "filter") and not hasattr(source, "count"):
            raise TypeError(f"Source must be a SQLAlchemy Query, got {type(source)}")

        query: Any = source

        # Type check: step should be a concrete step type
        if not isinstance(step, (SqlBronzeStep, SqlSilverStep, SqlGoldStep)):
            raise TypeError(
                f"Step must be SqlBronzeStep, SqlSilverStep, or SqlGoldStep, got {type(step)}"
            )

        # Bronze steps don't write to tables
        if isinstance(step, SqlBronzeStep):
            rows_written = query.count() if hasattr(query, "count") else 0
            return WriteReport(
                source=query,
                written_rows=rows_written,
                failed_rows=0,
                error=None,
            )

        # Get table name and schema
        table_name = getattr(step, "table_name", None) or step.name
        schema = getattr(step, "schema", None) or self.config.schema

        if schema is None:
            raise ValueError(
                f"Step '{step.name}' requires a schema to be specified for writing"
            )

        # Determine write mode
        write_mode = getattr(step, "write_mode", "overwrite") or "overwrite"

        drop_existing = False
        if isinstance(step, SqlGoldStep):
            drop_existing = True
        elif isinstance(step, SqlSilverStep) and write_mode == "overwrite":
            drop_existing = True

        # Create schema if needed
        try:
            from sql_pipeline_builder.table_operations import create_schema_if_not_exists
            create_schema_if_not_exists(self.session, schema)
        except Exception as e:
            raise RuntimeError(f"Failed to create schema '{schema}': {e}") from e

        # Write to table
        try:
            rows_before = query.count() if hasattr(query, "count") else 0
            model_class = getattr(step, "model_class", None)
            rows_written = write_table(
                self.session,
                query,
                schema,
                table_name,
                write_mode,
                model_class,
                drop_existing_table=drop_existing,
            )
            return WriteReport(
                source=query,
                written_rows=rows_written,
                failed_rows=0,
                error=None,
            )
        except Exception as e:
            return WriteReport(
                source=query,
                written_rows=0,
                failed_rows=query.count() if hasattr(query, "count") else 0,
                error=e,
            )

