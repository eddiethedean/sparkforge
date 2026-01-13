"""Gold step executor.

This module provides the executor for gold steps, which aggregate silver data
into final business metrics and analytics. Gold steps are the final layer
in the Medallion architecture.
"""

from __future__ import annotations

from typing import Any, Dict

from pipeline_builder_base.errors import ExecutionError

from ..compat import DataFrame
from ..models import GoldStep
from .base import BaseStepExecutor


class GoldStepExecutor(BaseStepExecutor):
    """Executor for gold steps in the pipeline.

    Gold steps aggregate silver data into final business metrics and analytics.
    They typically perform aggregations, joins, and business logic to produce
    final reporting tables.

    Gold steps:
        - Aggregate multiple silver tables
        - Perform business logic and calculations
        - Write results to Delta Lake tables
        - Apply validation rules after transformation

    Example:
        >>> from pipeline_builder.step_executors.gold import GoldStepExecutor
        >>> from pipeline_builder.models import GoldStep
        >>>
        >>> executor = GoldStepExecutor(spark)
        >>> result = executor.execute(
        ...     step=GoldStep(
        ...         name="daily_metrics",
        ...         transform=lambda spark, silvers: (
        ...             silvers["clean_events"]
        ...             .groupBy("date")
        ...             .agg(F.count("*").alias("count"))
        ...         ),
        ...         rules={"count": [F.col("count") > 0]},
        ...         table_name="daily_metrics",
        ...         source_silvers=["clean_events"]
        ...     ),
        ...     context={"clean_events": silver_df}
        ... )
    """

    def execute(
        self,
        step: GoldStep,
        context: Dict[str, DataFrame],
        mode: Any = None,  # Mode not used for gold steps
    ) -> DataFrame:
        """Execute a gold step.

        Transforms silver data using the step's transform function. Builds a
        dictionary of source silver DataFrames from step.source_silvers.

        Args:
            step: GoldStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                all source silver step names listed in step.source_silvers.
            mode: Execution mode (not used for gold steps, can be None).

        Returns:
            Transformed DataFrame ready for validation and writing.

        Raises:
            ExecutionError: If any source silver step not found in context.

        Note:
            - Builds silvers dictionary from step.source_silvers
            - Calls step.transform() with SparkSession and silvers dictionary
            - Transformation logic is defined in the step's transform function
            - Gold steps typically perform aggregations and business metrics
        """
        # Build silvers dict from source_silvers
        silvers = {}
        if step.source_silvers is not None:
            for silver_name in step.source_silvers:
                if silver_name not in context:
                    raise ExecutionError(
                        f"Source silver {silver_name} not found in context"
                    )
                silvers[silver_name] = context[silver_name]

        return step.transform(self.spark, silvers)
