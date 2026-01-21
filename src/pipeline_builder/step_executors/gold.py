"""Gold step executor.

This module provides the executor for gold steps, which aggregate silver data
into final business metrics and analytics. Gold steps are the final layer
in the Medallion architecture.
"""

from __future__ import annotations

import inspect
from typing import Any, Dict, Optional

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
        step_params: Optional[Dict[str, Any]] = None,
        step_types: Optional[Dict[str, str]] = None,
    ) -> DataFrame:
        """Execute a gold step.

        Transforms silver data using the step's transform function. Builds a
        dictionary of source silver DataFrames from step.source_silvers.

        Args:
            step: GoldStep instance to execute.
            context: Dictionary mapping step names to DataFrames. Must contain
                all source silver step names listed in step.source_silvers.
            mode: Execution mode (not used for gold steps, can be None).
            step_params: Optional dictionary of parameters to pass to the transform
                function. If the transform function accepts a 'params' argument or
                **kwargs, these will be passed. Otherwise, ignored for backward
                compatibility.

        Returns:
            Transformed DataFrame ready for validation and writing.

        Raises:
            ExecutionError: If any source silver step not found in context.

        Note:
            - Builds silvers dictionary from step.source_silvers
            - Calls step.transform() with SparkSession and silvers dictionary
            - If step_params is provided and transform accepts params/kwargs, passes them
            - Transformation logic is defined in the step's transform function
            - Gold steps typically perform aggregations and business metrics
        """
        # Handle validation-only steps (no transform function)
        if step.transform is None:
            if step.existing:
                # Use base class method for validation-only step handling
                result = self._handle_validation_only_step(step, "gold")
                if result is not None:
                    return result
            else:
                raise ExecutionError(
                    f"Gold step '{step.name}' has no transform function and is not marked as existing"
                )

        # Build silvers dict from source_silvers
        silvers = {}
        if step.source_silvers is not None:
            for silver_name in step.source_silvers:
                if silver_name not in context:
                    raise ExecutionError(
                        f"Source silver {silver_name} not found in context"
                    )
                silvers[silver_name] = context[silver_name]

        # Build prior_golds dict from context (all previously executed gold steps)
        prior_golds: Dict[str, DataFrame] = {}
        if step_types is not None:
            for key, value in context.items():
                if key != step.name and step_types.get(key) == "gold":
                    prior_golds[key] = value

        # Detect if transform function accepts prior_golds parameter
        has_prior_golds = False
        if step.transform is not None:
            try:
                sig = inspect.signature(step.transform)
                has_prior_golds = "prior_golds" in sig.parameters
            except (ValueError, TypeError):
                # If we can't inspect the signature, assume it doesn't accept prior_golds
                has_prior_golds = False

        # Apply transform with silvers dict and optionally prior_golds
        # Support backward-compatible params passing
        if step_params is not None and self._accepts_params(step.transform):
            # Try calling with params argument
            try:
                sig = inspect.signature(step.transform)
                if "params" in sig.parameters:
                    if has_prior_golds:
                        return step.transform(self.spark, silvers, prior_golds, params=step_params)
                    else:
                        return step.transform(self.spark, silvers, params=step_params)
                else:
                    # Has **kwargs, call with params as keyword
                    if has_prior_golds:
                        return step.transform(self.spark, silvers, prior_golds, **step_params)
                    else:
                        return step.transform(self.spark, silvers, **step_params)
            except Exception:
                # Fallback to standard call if params passing fails
                if has_prior_golds:
                    return step.transform(self.spark, silvers, prior_golds)
                else:
                    return step.transform(self.spark, silvers)
        else:
            if has_prior_golds:
                return step.transform(self.spark, silvers, prior_golds)
            else:
                return step.transform(self.spark, silvers)
