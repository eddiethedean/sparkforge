"""Silver step executor.

This module provides the executor for silver steps, which transform bronze
data into cleaned and enriched data. Silver steps can handle incremental
processing to only process new data since the last run.
"""

from __future__ import annotations

from typing import Dict

from pipeline_builder_base.errors import ExecutionError
from pipeline_builder_base.models import ExecutionMode

from ..compat import DataFrame, F
from ..models import SilverStep
from ..table_operations import fqn
from .base import BaseStepExecutor


class SilverStepExecutor(BaseStepExecutor):
    """Executor for silver steps in the pipeline.

    Silver steps transform bronze data into cleaned and enriched data. They
    can handle incremental processing to only process new rows since the last
    run, improving efficiency for large datasets.

    Silver steps:
        - Transform bronze data using step.transform() function
        - Support incremental processing via watermark columns
        - Write results to Delta Lake tables
        - Apply validation rules after transformation

    Example:
        >>> from pipeline_builder.step_executors.silver import SilverStepExecutor
        >>> from pipeline_builder_base.models import ExecutionMode
        >>> from pipeline_builder.models import SilverStep
        >>>
        >>> executor = SilverStepExecutor(spark)
        >>> result = executor.execute(
        ...     step=SilverStep(
        ...         name="clean_events",
        ...         source_bronze="events",
        ...         transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
        ...         rules={"status": [F.col("status").isNotNull()]},
        ...         table_name="clean_events"
        ...     ),
        ...     context={"events": bronze_df},
        ...     mode=ExecutionMode.INITIAL
        ... )
    """

    def execute(  # type: ignore[override]
        self,
        step: SilverStep,
        context: Dict[str, DataFrame],
        mode: ExecutionMode,
    ) -> DataFrame:
        """Execute a silver step.

        Transforms bronze data using the step's transform function. For
        INCREMENTAL mode, filters bronze input to only process new rows.

        Args:
            step: SilverStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                the source bronze step name (step.source_bronze).
            mode: Execution mode. INCREMENTAL mode triggers incremental filtering
                of bronze input.

        Returns:
            Transformed DataFrame ready for validation and writing.

        Raises:
            ExecutionError: If source bronze step not found in context or
                incremental filtering fails.

        Note:
            - Applies incremental filtering if mode is INCREMENTAL
            - Calls step.transform() with bronze DataFrame and empty silvers dict
            - Transformation logic is defined in the step's transform function
        """
        # Get source bronze data
        if step.source_bronze not in context:
            raise ExecutionError(
                f"Source bronze step {step.source_bronze} not found in context"
            )

        bronze_df: DataFrame = context[step.source_bronze]

        if mode == ExecutionMode.INCREMENTAL:
            bronze_df = self._filter_incremental_bronze_input(step, bronze_df)

        # Apply transform with source bronze data and empty silvers dict
        return step.transform(self.spark, bronze_df, {})

    def _filter_incremental_bronze_input(
        self,
        step: SilverStep,
        bronze_df: DataFrame,
    ) -> DataFrame:
        """Filter bronze input rows already processed in previous incremental runs.

        Filters bronze DataFrame to only include rows that haven't been processed
        yet. Uses the source bronze step's incremental column and the silver step's
        watermark column to determine which rows to exclude.

        Args:
            step: SilverStep instance with incremental configuration.
            bronze_df: Bronze DataFrame to filter.

        Returns:
            Filtered DataFrame containing only new rows to process. Returns
            original DataFrame if filtering cannot be performed (missing columns,
            table doesn't exist, etc.).

        Raises:
            ExecutionError: If filtering fails due to column or type issues.

        Note:
            Filtering logic:
            1. Reads existing silver table to get maximum watermark value
            2. Filters bronze rows where incremental_col > max_watermark
            3. Returns original DataFrame if table doesn't exist (first run)

            Requires:
            - step.source_incremental_col: Column in bronze DataFrame
            - step.watermark_col: Column in existing silver table
            - step.schema and step.table_name: To locate existing table

            Skips filtering gracefully if requirements not met (returns original DataFrame).
        """
        incremental_col = getattr(step, "source_incremental_col", None)
        watermark_col = getattr(step, "watermark_col", None)
        schema = getattr(step, "schema", None)
        table_name = getattr(step, "table_name", step.name)

        if not incremental_col or not watermark_col or schema is None:
            return bronze_df

        if incremental_col not in getattr(bronze_df, "columns", []):
            self.logger.debug(
                f"Silver step {step.name}: incremental column '{incremental_col}' "
                f"not present in bronze DataFrame; skipping incremental filter"
            )
            return bronze_df

        # Validate that incremental column type is appropriate for filtering
        try:
            df_schema = bronze_df.schema
            col_field = df_schema[incremental_col]  # type: ignore[index]
            col_type = col_field.dataType
            col_type_name = str(col_type)

            # Check if type is comparable (numeric, date, timestamp, string)
            # Non-comparable types: boolean, array, map, struct
            non_comparable_types = ["boolean", "array", "map", "struct", "binary"]
            if any(
                non_comp in col_type_name.lower() for non_comp in non_comparable_types
            ):
                self.logger.warning(
                    f"Silver step {step.name}: incremental column '{incremental_col}' "
                    f"has type '{col_type_name}' which may not be suitable for comparison operations. "
                    f"Filtering may fail or produce unexpected results. "
                    f"Consider using a numeric, date, timestamp, or string column for incremental processing."
                )
        except (KeyError, AttributeError, Exception) as e:
            # If we can't inspect the schema, log a warning but continue
            self.logger.debug(
                f"Silver step {step.name}: could not validate incremental column type: {e}"
            )

        output_table = fqn(schema, table_name)

        try:
            existing_table = self.spark.table(output_table)
        except Exception as exc:
            self.logger.debug(
                f"Silver step {step.name}: unable to read existing table {output_table} "
                f"for incremental filter: {exc}"
            )
            return bronze_df

        if watermark_col not in getattr(existing_table, "columns", []):
            self.logger.debug(
                f"Silver step {step.name}: watermark column '{watermark_col}' "
                f"not present in existing table {output_table}; skipping incremental filter"
            )
            return bronze_df

        try:
            watermark_rows = existing_table.select(watermark_col).collect()
        except Exception as exc:
            self.logger.warning(
                f"Silver step {step.name}: failed to collect watermark values "
                f"from {output_table}: {exc}"
            )
            return bronze_df

        if not watermark_rows:
            return bronze_df

        cutoff_value = None
        for row in watermark_rows:
            value = None
            if hasattr(row, "__getitem__"):
                try:
                    value = row[watermark_col]
                except Exception:
                    try:
                        value = row[0]
                    except Exception:
                        value = None
            if value is None and hasattr(row, "asDict"):
                value = row.asDict().get(watermark_col)
            if value is None:
                continue
            cutoff_value = value if cutoff_value is None else max(cutoff_value, value)

        if cutoff_value is None:
            return bronze_df

        try:
            filtered_df = bronze_df.filter(F.col(incremental_col) > F.lit(cutoff_value))
        except Exception as exc:
            # Provide detailed error context for incremental filtering failures
            error_msg = str(exc).lower()
            if "cannot resolve" in error_msg or "column" in error_msg:
                # Column-related error - provide schema context
                available_cols = sorted(getattr(bronze_df, "columns", []))
                raise ExecutionError(
                    f"Silver step {step.name}: failed to filter bronze rows using incremental column '{incremental_col}'. "
                    f"Error: {exc!r}. "
                    f"Available columns in bronze DataFrame: {available_cols}. "
                    f"This may indicate that the incremental column was dropped or renamed in a previous transform. "
                    f"Please ensure the incremental column '{incremental_col}' exists in the bronze DataFrame."
                ) from exc
            elif "type" in error_msg or "cast" in error_msg:
                # Type-related error - provide type information
                try:
                    df_schema = bronze_df.schema
                    col_type = df_schema[incremental_col].dataType  # type: ignore[index]
                    raise ExecutionError(
                        f"Silver step {step.name}: failed to filter bronze rows using incremental column '{incremental_col}'. "
                        f"Error: {exc!r}. "
                        f"Column type: {col_type}. "
                        f"Cutoff value type: {type(cutoff_value).__name__}. "
                        f"Incremental columns must be comparable types (numeric, date, timestamp). "
                        f"Please ensure the incremental column type is compatible with the cutoff value."
                    ) from exc
                except (KeyError, AttributeError, Exception):
                    # If we can't get type info, provide generic error
                    raise ExecutionError(
                        f"Silver step {step.name}: failed to filter bronze rows using incremental column '{incremental_col}'. "
                        f"Error: {exc!r}. "
                        f"This may be a type mismatch between the incremental column and the cutoff value. "
                        f"Please ensure the incremental column type is compatible with the cutoff value type."
                    ) from exc
            else:
                # Generic error with context
                raise ExecutionError(
                    f"Silver step {step.name}: failed to filter bronze rows using "
                    f"{incremental_col} > {cutoff_value}: {exc!r}. "
                    f"Please check that the incremental column exists and is of a comparable type."
                ) from exc

        self.logger.info(
            f"Silver step {step.name}: filtering bronze rows where "
            f"{incremental_col} <= {cutoff_value}"
        )
        return filtered_df  # type: ignore[no-any-return]
