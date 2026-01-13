"""Step factory for creating pipeline steps.

This module provides a factory for creating step instances, separating
step creation from pipeline building logic. The StepFactory centralizes
step creation, making it easier to modify step creation logic and test
pipeline building.
"""

from __future__ import annotations

from typing import Any, Optional

from pipeline_builder_base.logging import PipelineLogger

from ..models import BronzeStep, GoldStep, SilverStep
from ..types import (
    ColumnRules,
    GoldTransformFunction,
    SilverTransformFunction,
    StepName,
    TableName,
)


class StepFactory:
    """Factory for creating pipeline step instances.

    Handles step creation logic separately from pipeline building. Provides
    methods to create BronzeStep, SilverStep, and GoldStep instances with
    proper validation and configuration.

    Attributes:
        logger: PipelineLogger instance for logging.

    Example:
        >>> from pipeline_builder.pipeline.step_factory import StepFactory
        >>> from pipeline_builder.functions import get_default_functions
        >>> F = get_default_functions()
        >>>
        >>> factory = StepFactory()
        >>> bronze = factory.create_bronze_step(
        ...     name="events",
        ...     rules={"id": [F.col("id").isNotNull()]},
        ...     incremental_col="timestamp"
        ... )
        >>> silver = factory.create_silver_step(
        ...     name="clean_events",
        ...     source_bronze="events",
        ...     transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
        ...     rules={"status": [F.col("status").isNotNull()]},
        ...     table_name="clean_events"
        ... )
    """

    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
    ):
        """Initialize the step factory.

        Args:
            logger: Optional PipelineLogger instance. If None, creates a
                default logger.
        """
        self.logger = logger or PipelineLogger()

    def create_bronze_step(
        self,
        name: StepName,
        rules: ColumnRules,
        incremental_col: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> BronzeStep:
        """
        Create a bronze step.

        Args:
            name: Step name
            rules: Validation rules
            incremental_col: Optional incremental column name
            schema: Optional schema name

        Returns:
            BronzeStep instance
        """
        return BronzeStep(
            name=name,
            rules=rules,
            incremental_col=incremental_col,
            schema=schema,
        )

    def create_silver_step(
        self,
        name: StepName,
        source_bronze: StepName,
        transform: SilverTransformFunction,
        rules: ColumnRules,
        table_name: TableName,
        schema: Optional[str] = None,
        source_incremental_col: Optional[str] = None,
        watermark_col: Optional[str] = None,
        schema_override: Optional[Any] = None,
    ) -> SilverStep:
        """
        Create a silver step.

        Args:
            name: Step name
            source_bronze: Source bronze step name
            transform: Transform function
            rules: Validation rules
            table_name: Target table name
            schema: Optional schema name
            source_incremental_col: Optional source incremental column
            watermark_col: Optional watermark column
            schema_override: Optional schema override

        Returns:
            SilverStep instance
        """
        return SilverStep(
            name=name,
            source_bronze=source_bronze,
            transform=transform,
            rules=rules,
            table_name=table_name,
            schema=schema,
            source_incremental_col=source_incremental_col,
            watermark_col=watermark_col,
            schema_override=schema_override,
        )

    def create_gold_step(
        self,
        name: StepName,
        transform: GoldTransformFunction,
        rules: ColumnRules,
        table_name: TableName,
        source_silvers: Optional[list[StepName]] = None,
        schema: Optional[str] = None,
        schema_override: Optional[Any] = None,
    ) -> GoldStep:
        """
        Create a gold step.

        Args:
            name: Step name
            transform: Transform function
            rules: Validation rules
            table_name: Target table name
            source_silvers: Optional list of source silver step names
            schema: Optional schema name
            schema_override: Optional schema override

        Returns:
            GoldStep instance
        """
        return GoldStep(
            name=name,
            transform=transform,
            rules=rules,
            table_name=table_name,
            source_silvers=source_silvers,
            schema=schema,
            schema_override=schema_override,
        )
