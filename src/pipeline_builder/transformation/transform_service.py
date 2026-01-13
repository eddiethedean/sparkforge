"""Transform service for applying transformations.

This module provides a service for applying transformation functions to
DataFrames. The TransformService handles transform function execution and
context preparation for Silver and Gold steps.
"""

from __future__ import annotations

from typing import Dict, Optional

from pipeline_builder_base.logging import PipelineLogger

from ..compat import DataFrame, SparkSession
from ..models import GoldStep, SilverStep


class TransformService:
    """Service for applying transformations to DataFrames.

    Handles transform function execution and context preparation for Silver
    and Gold steps. Separates transformation logic from execution flow.

    Attributes:
        spark: SparkSession instance for DataFrame operations.
        logger: PipelineLogger instance for logging.

    Example:
        >>> from pipeline_builder.transformation.transform_service import TransformService
        >>> from pipeline_builder.compat import SparkSession
        >>>
        >>> service = TransformService(spark)
        >>> result = service.apply_silver_transform(
        ...     step=silver_step,
        ...     bronze_df=bronze_data,
        ...     silvers={}
        ... )
        >>> gold_result = service.apply_gold_transform(
        ...     step=gold_step,
        ...     silvers={"clean_events": silver_data}
        ... )
    """

    def __init__(
        self,
        spark: SparkSession,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the transform service.

        Args:
            spark: Active SparkSession instance for DataFrame operations.
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.spark = spark
        self.logger = logger or PipelineLogger()

    def apply_silver_transform(
        self,
        step: SilverStep,
        bronze_df: DataFrame,
        silvers: Dict[str, DataFrame],
    ) -> DataFrame:
        """Apply a silver step transformation.

        Executes the transform function for a Silver step. Silver transforms
        receive bronze DataFrame and silvers dictionary (usually empty).

        Args:
            step: SilverStep instance with transform function.
            bronze_df: Bronze DataFrame to transform (source data).
            silvers: Dictionary of silver DataFrames. Usually empty for
                Silver steps, but available for cross-silver dependencies.

        Returns:
            Transformed DataFrame after applying the step's transform function.

        Raises:
            ValueError: If step.transform is None.

        Note:
            Silver transforms have signature:
            (spark: SparkSession, bronze_df: DataFrame, silvers: Dict[str, DataFrame]) -> DataFrame
        """
        if step.transform is None:
            raise ValueError(f"Silver step '{step.name}' requires a transform function")

        return step.transform(self.spark, bronze_df, silvers)

    def apply_gold_transform(
        self,
        step: GoldStep,
        silvers: Dict[str, DataFrame],
    ) -> DataFrame:
        """Apply a gold step transformation.

        Executes the transform function for a Gold step. Gold transforms
        receive a dictionary of silver DataFrames for aggregation and
        business logic.

        Args:
            step: GoldStep instance with transform function.
            silvers: Dictionary mapping silver step names to DataFrames.
                Must contain all step.source_silvers.

        Returns:
            Transformed DataFrame after applying the step's transform function.

        Raises:
            ValueError: If step.transform is None.

        Note:
            Gold transforms have signature:
            (spark: SparkSession, silvers: Dict[str, DataFrame]) -> DataFrame
        """
        if step.transform is None:
            raise ValueError(f"Gold step '{step.name}' requires a transform function")

        return step.transform(self.spark, silvers)
