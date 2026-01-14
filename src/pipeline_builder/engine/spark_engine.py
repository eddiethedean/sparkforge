# mypy: ignore-errors
"""
SparkEngine implementation of abstracts.Engine.

This engine wraps ExecutionEngine and adapts between the abstracts interface
and the concrete pipeline_builder implementation.
"""

from __future__ import annotations

from typing import Any, Optional, Union

from abstracts.engine import Engine
from abstracts.reports.transform import TransformReport
from abstracts.reports.validation import ValidationReport
from abstracts.reports.write import WriteReport
from abstracts.source import Source
from abstracts.step import Step
from pipeline_builder_base.logging import PipelineLogger

from ..execution import ExecutionEngine, _create_dataframe_writer
from ..functions import FunctionsProtocol
from ..models import BronzeStep, GoldStep, SilverStep
from ..protocols import (
    DataFrameProtocol as DataFrame,
)
from ..protocols import (
    SparkSessionProtocol as SparkSession,
)
from ..table_operations import fqn
from ..validation import apply_column_rules


class SparkEngine(Engine):
    """
    SparkEngine implements abstracts.Engine using ExecutionEngine.

    This engine adapts between the abstracts interface (Step, Source protocols)
    and the concrete pipeline_builder types (BronzeStep/SilverStep/GoldStep, DataFrame).
    """

    def __init__(
        self,
        spark: SparkSession,  # type: ignore[valid-type]
        config: Any,  # PipelineConfig
        logger: Optional[PipelineLogger] = None,
        functions: Optional[FunctionsProtocol] = None,
    ):
        """
        Initialize SparkEngine.

        Args:
            spark: SparkSession instance
            config: PipelineConfig instance
            logger: Optional logger instance
            functions: Optional functions protocol for PySpark operations
        """
        self.spark = spark
        self.config = config
        self.logger = logger or PipelineLogger()
        self.functions = functions
        self._execution_engine = ExecutionEngine(spark, config, self.logger, functions)

    def validate_source(self, step: Step, source: Source) -> ValidationReport:
        """
        Validate a data source according to step rules.

        Args:
            step: Step with validation rules
            source: Source data to validate (DataFrame)

        Returns:
            ValidationReport with validation results
        """
        # Duck-type: must expose DataFrameProtocol surface
        if not hasattr(source, "schema") or not hasattr(source, "count"):
            raise TypeError(f"Source must be DataFrame-like, got {type(source)}")

        df: DataFrame = source  # type: ignore[valid-type]

        concrete_step: Union[BronzeStep, SilverStep, GoldStep] = step  # type: ignore[assignment]

        # Apply validation rules
        try:
            # Rules type compatibility - Step Protocol uses Rules, concrete steps use ColumnRules
            # mypy doesn't understand Protocol structural typing here, so we use Any
            rules: Any = concrete_step.rules
            valid_df, invalid_df, validation_stats = apply_column_rules(
                df,
                rules,
                "pipeline",
                concrete_step.name,
                functions=self.functions,
            )

            valid_rows = valid_df.count()  # type: ignore[attr-defined]
            invalid_rows = invalid_df.count()  # type: ignore[attr-defined]

            return ValidationReport(
                source=valid_df,  # Return validated source
                valid_rows=valid_rows,
                invalid_rows=invalid_rows,
                error=None,
            )
        except Exception as e:
            return ValidationReport(
                source=df,
                valid_rows=0,
                invalid_rows=df.count() if df is not None else 0,  # type: ignore[attr-defined]
                error=e,
            )

    def transform_source(self, step: Step, source: Source) -> TransformReport:
        """
        Transform a data source according to step transformation logic.

        Args:
            step: Step with transformation function
            source: Source data to transform (DataFrame)

        Returns:
            TransformReport with transformed source
        """
        if not hasattr(source, "schema") or not hasattr(source, "count"):
            raise TypeError(f"Source must be DataFrame-like, got {type(source)}")

        df: DataFrame = source  # type: ignore[valid-type]

        concrete_step: Union[BronzeStep, SilverStep, GoldStep] = step  # type: ignore[assignment]

        try:
            # Bronze steps: no transformation, just return source
            step_phase = concrete_step.step_type
            if step_phase.value == "bronze":
                return TransformReport(source=df, error=None)

            elif step_phase.value == "silver":
                if concrete_step.transform is None:  # type: ignore[attr-defined]
                    raise ValueError(
                        f"Silver step '{concrete_step.name}' requires a transform function"
                    )
                transformed_df = concrete_step.transform(self.spark, df, {})  # type: ignore[attr-defined]
                return TransformReport(source=transformed_df, error=None)

            # Gold steps: transform with silvers dict
            # Note: For gold steps, the "source" parameter is actually a dict of silvers
            # This is a limitation of the abstracts.Engine interface for gold steps
            elif step_phase.value == "gold":
                if concrete_step.transform is None:  # type: ignore[attr-defined]
                    raise ValueError(
                        f"Gold step '{concrete_step.name}' requires a transform function"
                    )
                # For gold steps, source should be a dict of silvers (Dict[str, DataFrame]  # type: ignore[valid-type])
                # The abstracts interface expects Source, but we accept dict for gold steps
                if type(source) is dict:
                    silvers = source
                else:
                    # If single DataFrame, this is an error for gold steps
                    raise TypeError(
                        f"Gold step '{concrete_step.name}' requires a dict of silvers, got {type(source)}"
                    )
                transformed_df = concrete_step.transform(self.spark, silvers)
                return TransformReport(source=transformed_df, error=None)

            else:
                raise ValueError(f"Unknown step type: {type(step)}")

        except Exception as e:
            return TransformReport(source=df, error=e)

    def write_target(self, step: Step, source: Source) -> WriteReport:
        """
        Write a data source to target table.

        Args:
            step: Step with target configuration
            source: Source data to write (DataFrame)

        Returns:
            WriteReport with write results
        """
        # Duck-type: must expose DataFrameProtocol surface (avoids isinstance issues in Python 3.8)
        if not hasattr(source, "schema") or not hasattr(source, "count"):
            raise TypeError(f"Source must be DataFrame-like, got {type(source)}")

        df: DataFrame = source  # type: ignore[valid-type]

        # Type check: step should have step_type property (avoids isinstance issues in Python 3.8)
        if not hasattr(step, "step_type"):
            raise TypeError(
                f"Step must have step_type property (BronzeStep, SilverStep, or GoldStep), got {type(step)}"
            )
        # Cast to help mypy - we know it's one of the concrete types after checking step_type
        concrete_step: Union[BronzeStep, SilverStep, GoldStep] = step  # type: ignore[assignment]

        # Bronze steps don't write to tables
        step_phase = concrete_step.step_type
        if step_phase.value == "bronze":
            rows_written = df.count()  # type: ignore[attr-defined]
            return WriteReport(
                source=df,
                written_rows=rows_written,
                failed_rows=0,
                error=None,
            )

        # Get table name and schema
        table_name = getattr(concrete_step, "table_name", None) or getattr(
            concrete_step, "target", concrete_step.name
        )
        schema = getattr(concrete_step, "schema", None) or getattr(
            concrete_step, "write_schema", None
        )

        if schema is None:
            raise ValueError(
                f"Step '{concrete_step.name}' requires a schema to be specified for writing"
            )

        if table_name is None:
            raise ValueError(
                f"Step '{concrete_step.name}' requires a table_name or target to be specified"
            )

        output_table = fqn(schema, table_name)

        # Determine write mode
        write_mode = getattr(concrete_step, "write_mode", "overwrite")
        if write_mode is None:
            write_mode = "overwrite"

        # Create schema if needed
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")  # type: ignore[attr-defined]
        except Exception as e:
            raise RuntimeError(f"Failed to create schema '{schema}': {e}") from e

        # Write to table
        try:
            rows_before = df.count()  # type: ignore[attr-defined]
            # Use helper function to ensure correct format (delta or parquet) based on availability
            writer = _create_dataframe_writer(df, self.spark, write_mode)
            writer.saveAsTable(output_table)  # type: ignore[attr-defined]
            rows_written = rows_before  # Assuming all rows were written successfully
            return WriteReport(
                source=df,
                written_rows=rows_written,
                failed_rows=0,
                error=None,
            )
        except Exception as e:
            return WriteReport(
                source=df,
                written_rows=0,
                failed_rows=df.count() if df is not None else 0,  # type: ignore[attr-defined]
                error=e,
            )
