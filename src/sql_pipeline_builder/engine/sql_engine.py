"""
SqlEngine implementation of abstracts.Engine.

This engine wraps SQL execution and adapts between the abstracts interface
and the concrete sql_pipeline_builder implementation.
"""

from __future__ import annotations

from typing import Any, Optional

from abstracts.engine import Engine
from abstracts.reports.transform import TransformReport
from abstracts.reports.validation import ValidationReport
from abstracts.reports.write import WriteReport
from abstracts.source import Source
from abstracts.step import Step
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import PipelineConfig

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

        # Type check: step should have step_type property (avoids isinstance issues in Python 3.8)
        if not hasattr(step, "step_type"):
            raise TypeError(
                f"Step must have step_type property (SqlBronzeStep, SqlSilverStep, or SqlGoldStep), got {type(step)}"
            )

        # Apply validation rules
        try:  # type: ignore[unreachable]
            rules: Any = step.rules
            valid_query, invalid_query, validation_stats = apply_sql_validation_rules(
                query,
                rules,
                step.name,
                self.session,
            )

            valid_rows = valid_query.count() if hasattr(valid_query, "count") else 0
            invalid_rows = (
                invalid_query.count() if hasattr(invalid_query, "count") else 0
            )

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
        # Type check: step should have step_type property (avoids isinstance issues in Python 3.8)
        if not hasattr(step, "step_type"):
            raise TypeError(
                f"Step must have step_type property (SqlBronzeStep, SqlSilverStep, or SqlGoldStep), got {type(step)}"
            )

        try:  # type: ignore[unreachable]
            step_phase = step.step_type
            # Bronze steps: no transformation, just return source
            if step_phase.value == "bronze":
                return TransformReport(source=source, error=None)

            # Silver steps: transform with bronze query and empty silvers dict
            elif step_phase.value == "silver":
                if step.transform is None:  # type: ignore[attr-defined]
                    raise ValueError(
                        f"Silver step '{step.name}' requires a transform function"
                    )
                # Source should be a Query for silver steps
                if not hasattr(source, "filter"):
                    raise TypeError(
                        f"Silver step '{step.name}' requires a Query source, got {type(source)}"
                    )
                transformed_query = step.transform(self.session, source, {})  # type: ignore[attr-defined, operator]
                return TransformReport(source=transformed_query, error=None)

            # Gold steps: transform with silvers dict
            elif step_phase.value == "gold":
                if step.transform is None:  # type: ignore[attr-defined]
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
                transformed_query = step.transform(self.session, silvers)  # type: ignore[operator]
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

        # Type check: step should have step_type property (avoids isinstance issues in Python 3.8)
        if not hasattr(step, "step_type"):
            raise TypeError(
                f"Step must have step_type property (SqlBronzeStep, SqlSilverStep, or SqlGoldStep), got {type(step)}"
            )

        # Bronze steps don't write to tables
        step_phase = step.step_type
        if step_phase.value == "bronze":
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
        if step_phase.value == "gold":
            drop_existing = True
        elif step_phase.value == "silver" and write_mode == "overwrite":
            drop_existing = True

        # Create schema if needed
        try:
            from sql_pipeline_builder.table_operations import (
                create_schema_if_not_exists,
            )

            create_schema_if_not_exists(self.session, schema)
        except Exception as e:
            raise RuntimeError(f"Failed to create schema '{schema}': {e}") from e

        # Write to table
        try:
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
