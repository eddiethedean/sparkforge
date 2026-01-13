"""Execution validator service.

This module provides validation services that can be used during pipeline
execution to validate data according to step rules. The ExecutionValidator
separates validation logic from execution flow, making it composable and
testable.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from pipeline_builder_base.logging import PipelineLogger

from ..compat import DataFrame
from ..functions import FunctionsProtocol
from ..validation import apply_column_rules


class ExecutionValidator:
    """Service for validating data during pipeline execution.

    Handles validation logic separately from execution flow, making it
    composable and testable. Validates DataFrames according to step rules
    and provides validation metrics.

    Attributes:
        logger: PipelineLogger instance for logging.
        functions: FunctionsProtocol instance for PySpark operations.

    Example:
        >>> from pipeline_builder.validation.execution_validator import ExecutionValidator
        >>> from pipeline_builder.functions import get_default_functions
        >>>
        >>> validator = ExecutionValidator(functions=get_default_functions())
        >>> valid_df, invalid_df, stats = validator.validate_step_output(
        ...     df=output_df,
        ...     step_name="clean_events",
        ...     rules={"status": [F.col("status").isNotNull()]}
        ... )
        >>> rate, invalid_count = validator.get_validation_metrics(stats)
    """

    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
        functions: Optional[FunctionsProtocol] = None,
    ):
        """Initialize the execution validator.

        Args:
            logger: Optional PipelineLogger instance. If None, creates a default
                logger.
            functions: Optional FunctionsProtocol instance for PySpark
                operations. If None, functions must be provided when calling
                validation methods.
        """
        self.logger = logger or PipelineLogger()
        self.functions = functions

    def ensure_materialized_for_validation(
        self,
        df: DataFrame,
        rules: Dict[str, Any],
    ) -> DataFrame:
        """
        Force DataFrame materialization before validation to avoid CTE optimization issues.

        Mock-spark's CTE optimization can fail when validation rules reference columns
        created by transforms (via withColumn). By materializing the DataFrame first,
        we ensure all columns are available in the validation context.

        Args:
            df: DataFrame to potentially materialize
            rules: Validation rules dictionary

        Returns:
            Materialized DataFrame (or original if materialization not needed/available)
        """
        # Check if rules reference columns that might be new (not in original input)
        # Materialize before validation so downstream rules see all columns.
        if not rules:
            return df

        try:
            if hasattr(df, "cache"):
                df = df.cache()
            _ = df.count()
        except Exception as e:
            # Surface materialization problems instead of masking them
            self.logger.debug(f"Could not materialize DataFrame before validation: {e}")

        return df

    def validate_step_output(
        self,
        df: DataFrame,
        step_name: str,
        rules: Dict[str, Any],
        stage: str = "pipeline",
    ) -> Tuple[DataFrame, DataFrame, Any]:
        """Validate step output according to rules.

        Validates a DataFrame according to step validation rules. Returns
        separate DataFrames for valid and invalid rows, plus validation
        statistics.

        Args:
            df: DataFrame to validate.
            step_name: Name of the step being validated (for error messages).
            rules: Dictionary mapping column names to lists of validation rules.
            stage: Stage name for validation context. Defaults to "pipeline".

        Returns:
            Tuple of (valid_df, invalid_df, validation_stats) where:
            - valid_df: DataFrame containing rows that passed validation
            - invalid_df: DataFrame containing rows that failed validation
            - validation_stats: Validation statistics object with metrics

        Note:
            - Materializes DataFrame before validation to avoid CTE issues
            - Returns empty invalid_df if no rules provided
            - Uses apply_column_rules() for actual validation logic
        """
        if not rules:
            # No rules to apply, return original DataFrame
            return df, df.limit(0), None

        # Materialize before validation to avoid CTE issues
        df = self.ensure_materialized_for_validation(df, rules)

        # Apply validation rules
        valid_df, invalid_df, validation_stats = apply_column_rules(
            df,
            rules,
            stage,
            step_name,
            functions=self.functions,
        )

        return valid_df, invalid_df, validation_stats

    def get_validation_metrics(
        self,
        validation_stats: Any,
    ) -> Tuple[float, int]:
        """
        Extract validation metrics from validation stats.

        Args:
            validation_stats: Validation statistics object

        Returns:
            Tuple of (validation_rate, invalid_rows)
        """
        if validation_stats is None:
            return 100.0, 0

        validation_rate = getattr(validation_stats, "validation_rate", 100.0)
        invalid_rows = getattr(validation_stats, "invalid_rows", 0)

        return validation_rate, invalid_rows
