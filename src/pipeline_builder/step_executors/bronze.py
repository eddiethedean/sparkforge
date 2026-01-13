"""Bronze step executor.

This module provides the executor for bronze steps, which validate existing
data without transformation or writing. Bronze steps are the first layer in
the Medallion architecture and serve as data quality gates.
"""

from __future__ import annotations

from typing import Any, Dict

from pipeline_builder_base.errors import ExecutionError

from ..compat import DataFrame
from ..models import BronzeStep
from .base import BaseStepExecutor


class BronzeStepExecutor(BaseStepExecutor):
    """Executor for bronze steps in the pipeline.

    Bronze steps validate existing raw data without transformation or writing.
    They serve as data quality gates, ensuring that incoming data meets
    basic validation rules before being processed by silver steps.

    Bronze steps:
        - Validate data according to step rules
        - Do not transform data
        - Do not write to tables
        - Return the same DataFrame (validated but unchanged)

    Example:
        >>> from pipeline_builder.step_executors.bronze import BronzeStepExecutor
        >>> from pipeline_builder.models import BronzeStep
        >>>
        >>> executor = BronzeStepExecutor(spark)
        >>> result = executor.execute(
        ...     step=BronzeStep(name="events", rules={"id": [F.col("id").isNotNull()]}),
        ...     context={"events": source_df}
        ... )
        >>> # result is the same DataFrame, validated
    """

    def execute(
        self,
        step: BronzeStep,
        context: Dict[str, DataFrame],
        mode: Any = None,  # Mode not used for bronze steps
    ) -> DataFrame:
        """Execute a bronze step.

        Validates existing data from context without transformation. The step
        name must exist in the context dictionary with the source DataFrame.

        Args:
            step: BronzeStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                the step name as a key with the source DataFrame.
            mode: Execution mode (not used for bronze steps, can be None).

        Returns:
            Output DataFrame (same as input, validated but unchanged).

        Raises:
            ExecutionError: If step name not found in context or DataFrame is
                invalid.

        Note:
            - Bronze steps only validate data, they don't transform or write
            - Validation is applied separately by the execution engine
            - Empty DataFrames are allowed but logged as warnings
        """
        # Bronze steps require data to be provided in context
        # This is the expected behavior - bronze steps validate existing data
        if step.name not in context:
            raise ExecutionError(
                f"Bronze step '{step.name}' requires data to be provided in context. "
                f"Bronze steps are for validating existing data, not creating it. "
                f"Please provide data using bronze_sources parameter or context dictionary. "
                f"Available context keys: {list(context.keys())}"
            )

        df: DataFrame = context[step.name]

        # Validate that the DataFrame is not empty (optional check)
        if df.count() == 0:
            self.logger.warning(
                f"Bronze step '{step.name}' received empty DataFrame. "
                f"This may indicate missing or invalid data source."
            )

        return df
