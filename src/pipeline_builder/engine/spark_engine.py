"""
SparkEngine implementation of abstracts.Engine.

This engine wraps ExecutionEngine and adapts between the abstracts interface
and the concrete pipeline_builder implementation.
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

from ..compat import DataFrame, SparkSession
from ..execution import ExecutionEngine
from ..functions import FunctionsProtocol
from ..models import BronzeStep, GoldStep, SilverStep
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
        # Type check: source should be a DataFrame
        if not isinstance(source, DataFrame):
            raise TypeError(f"Source must be a DataFrame, got {type(source)}")

        df: DataFrame = source  # type: ignore[valid-type]

        # Type check: step should be a concrete step type
        # Cast to Any first to avoid Protocol isinstance issues with mypy
        step_any: Any = step
        if not isinstance(step_any, (BronzeStep, SilverStep, GoldStep)):
            raise TypeError(
                f"Step must be BronzeStep, SilverStep, or GoldStep, got {type(step)}"
            )
        # Cast to help mypy - we know it's one of the concrete types after isinstance
        # Use step_any directly since isinstance already narrowed the type
        concrete_step: BronzeStep | SilverStep | GoldStep = step_any

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
        # Type check: source should be a DataFrame
        if not isinstance(source, DataFrame):
            raise TypeError(f"Source must be a DataFrame, got {type(source)}")

        df: DataFrame = source  # type: ignore[valid-type]

        # Type check: step should be a concrete step type
        # Cast to Any first to avoid Protocol isinstance issues with mypy
        step_any: Any = step
        if not isinstance(step_any, (BronzeStep, SilverStep, GoldStep)):
            raise TypeError(
                f"Step must be BronzeStep, SilverStep, or GoldStep, got {type(step)}"
            )
        # Cast to help mypy - we know it's one of the concrete types after isinstance
        # Use step_any directly since isinstance already narrowed the type
        concrete_step: BronzeStep | SilverStep | GoldStep = step_any

        try:
            # Bronze steps: no transformation, just return source
            if isinstance(concrete_step, BronzeStep):
                return TransformReport(source=df, error=None)

            # Silver steps: transform with bronze data and empty silvers dict
            elif isinstance(concrete_step, SilverStep):
                if concrete_step.transform is None:
                    raise ValueError(
                        f"Silver step '{concrete_step.name}' requires a transform function"
                    )
                transformed_df = concrete_step.transform(self.spark, df, {})
                return TransformReport(source=transformed_df, error=None)

            # Gold steps: transform with silvers dict
            # Note: For gold steps, the "source" parameter is actually a dict of silvers
            # This is a limitation of the abstracts.Engine interface for gold steps
            elif isinstance(concrete_step, GoldStep):
                if concrete_step.transform is None:
                    raise ValueError(
                        f"Gold step '{concrete_step.name}' requires a transform function"
                    )
                # For gold steps, source should be a dict of silvers (Dict[str, DataFrame]  # type: ignore[valid-type])
                # The abstracts interface expects Source, but we accept dict for gold steps
                if isinstance(source, dict):
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
        # Type check: source should be a DataFrame
        if not isinstance(source, DataFrame):
            raise TypeError(f"Source must be a DataFrame, got {type(source)}")

        df: DataFrame = source  # type: ignore[valid-type]

        # Type check: step should be a concrete step type
        # Cast to Any first to avoid Protocol isinstance issues with mypy
        step_any: Any = step
        if not isinstance(step_any, (BronzeStep, SilverStep, GoldStep)):
            raise TypeError(
                f"Step must be BronzeStep, SilverStep, or GoldStep, got {type(step)}"
            )
        # Cast to help mypy - we know it's one of the concrete types after isinstance
        # Use step_any directly since isinstance already narrowed the type
        concrete_step: BronzeStep | SilverStep | GoldStep = step_any

        # Bronze steps don't write to tables
        if isinstance(concrete_step, BronzeStep):
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
            df.write.mode(write_mode).saveAsTable(output_table)  # type: ignore[attr-defined]
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
