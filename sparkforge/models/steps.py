"""
Step models for the Pipeline Builder.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import PipelineValidationError, ValidationError
from .base import BaseModel
from .types import ColumnRules, GoldTransformFunction, SilverTransformFunction


@dataclass
class BronzeStep(BaseModel):
    """
    Bronze layer step configuration for raw data validation and ingestion.

    Bronze steps represent the first layer of the Medallion Architecture,
    handling raw data validation and establishing the foundation for downstream
    processing. They define validation rules and incremental processing capabilities.

    **Validation Requirements:**
        - `name`: Must be a non-empty string
        - `rules`: Must be a non-empty dictionary with validation rules
        - `incremental_col`: Must be a string if provided

    Attributes:
        name: Unique identifier for this Bronze step
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        incremental_col: Column name for incremental processing (e.g., "timestamp").
                        If provided, enables watermarking for efficient updates.
                        If None, forces full refresh mode for downstream steps.
        schema: Optional schema name for reading bronze data

    Raises:
        ValidationError: If validation requirements are not met during construction

    Example:
        >>> from pyspark.sql import functions as F
        >>>
        >>> # Valid Bronze step with PySpark expressions
        >>> bronze_step = BronzeStep(
        ...     name="user_events",
        ...     rules={
        ...         "user_id": [F.col("user_id").isNotNull()],
        ...         "event_type": [F.col("event_type").isin(["click", "view", "purchase"])],
        ...         "timestamp": [F.col("timestamp").isNotNull(), F.col("timestamp") > "2020-01-01"]
        ...     },
        ...     incremental_col="timestamp"
        ... )
        >>>
        >>> # Validate configuration
        >>> bronze_step.validate()
        >>> print(f"Supports incremental: {bronze_step.has_incremental_capability}")

        >>> # Invalid Bronze step (will raise ValidationError)
        >>> try:
        ...     BronzeStep(name="", rules={})  # Empty name and rules
        ... except ValidationError as e:
        ...     print(f"Validation failed: {e}")
        ...     # Output: "Step name must be a non-empty string"
    """

    name: str
    rules: ColumnRules
    incremental_col: str | None = None
    schema: str | None = None

    def __post_init__(self) -> None:
        """Validate required fields after initialization."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("Step name must be a non-empty string")
        if not isinstance(self.rules, dict) or not self.rules:
            raise ValidationError("Rules must be a non-empty dictionary")
        if self.incremental_col is not None and not isinstance(
            self.incremental_col, str
        ):
            raise ValidationError("Incremental column must be a string")

    def validate(self) -> None:
        """Validate bronze step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if self.incremental_col is not None and not isinstance(
            self.incremental_col, str
        ):
            raise PipelineValidationError("Incremental column must be a string")

    @property
    def has_incremental_capability(self) -> bool:
        """Check if this Bronze step supports incremental processing."""
        return self.incremental_col is not None


@dataclass
class SilverStep(BaseModel):
    """
    Silver layer step configuration for data cleaning and enrichment.

    Silver steps represent the second layer of the Medallion Architecture,
    transforming raw Bronze data into clean, business-ready datasets.
    They apply data quality rules, business logic, and data transformations.

    **Validation Requirements:**
        - `name`: Must be a non-empty string
        - `source_bronze`: Must be a non-empty string (except for existing tables)
        - `transform`: Must be callable and cannot be None
        - `rules`: Must be a non-empty dictionary with validation rules
        - `table_name`: Must be a non-empty string

    Attributes:
        name: Unique identifier for this Silver step
        source_bronze: Name of the Bronze step providing input data
        transform: Transformation function with signature:
                 (spark: SparkSession, bronze_df: DataFrame, prior_silvers: Dict[str, DataFrame]) -> DataFrame
                 Must be callable and cannot be None.
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        table_name: Target Delta table name where results will be stored
        watermark_col: Column name for watermarking (e.g., "timestamp", "updated_at").
                      If provided, enables incremental processing with append mode.
                      If None, uses overwrite mode for full refresh.
        existing: Whether this represents an existing table (for validation-only steps)
        schema: Optional schema name for writing silver data

    Raises:
        ValidationError: If validation requirements are not met during construction

    Example:
        >>> def clean_user_events(spark, bronze_df, prior_silvers):
        ...     return (bronze_df
        ...         .filter(F.col("user_id").isNotNull())
        ...         .withColumn("event_date", F.date_trunc("day", "timestamp"))
        ...         .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
        ...     )
        >>>
        >>> # Valid Silver step
        >>> silver_step = SilverStep(
        ...     name="clean_events",
        ...     source_bronze="user_events",
        ...     transform=clean_user_events,
        ...     rules={
        ...         "user_id": [F.col("user_id").isNotNull()],
        ...         "event_date": [F.col("event_date").isNotNull()]
        ...     },
        ...     table_name="clean_user_events",
        ...     watermark_col="timestamp"
        ... )

        >>> # Invalid Silver step (will raise ValidationError)
        >>> try:
        ...     SilverStep(name="clean_events", source_bronze="", transform=None, rules={}, table_name="")
        ... except ValidationError as e:
        ...     print(f"Validation failed: {e}")
        ...     # Output: "Transform function is required and must be callable"
    """

    name: str
    source_bronze: str
    transform: SilverTransformFunction
    rules: ColumnRules
    table_name: str
    watermark_col: str | None = None
    existing: bool = False
    schema: str | None = None

    def __post_init__(self) -> None:
        """Validate required fields after initialization."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("Step name must be a non-empty string")
        if not self.existing and (
            not self.source_bronze or not isinstance(self.source_bronze, str)
        ):
            raise ValidationError("Source bronze step name must be a non-empty string")
        if self.transform is None or not callable(self.transform):
            raise ValidationError("Transform function is required and must be callable")
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValidationError("Table name must be a non-empty string")

    def validate(self) -> None:
        """Validate silver step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise PipelineValidationError(
                "Source bronze step name must be a non-empty string"
            )
        if not callable(self.transform):
            raise PipelineValidationError("Transform must be a callable function")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if not self.table_name or not isinstance(self.table_name, str):
            raise PipelineValidationError("Table name must be a non-empty string")


@dataclass
class GoldStep(BaseModel):
    """
    Gold layer step configuration for business analytics and reporting.

    Gold steps represent the third layer of the Medallion Architecture,
    creating business-ready datasets for analytics, reporting, and dashboards.
    They aggregate and transform Silver layer data into meaningful business insights.

    **Validation Requirements:**
        - `name`: Must be a non-empty string
        - `transform`: Must be callable and cannot be None
        - `rules`: Must be a non-empty dictionary with validation rules
        - `table_name`: Must be a non-empty string
        - `source_silvers`: Must be a non-empty list if provided

    Attributes:
        name: Unique identifier for this Gold step
        transform: Transformation function with signature:
                 (spark: SparkSession, silvers: Dict[str, DataFrame]) -> DataFrame
                 - spark: Active SparkSession for operations
                 - silvers: Dictionary of all Silver DataFrames by step name
                 Must be callable and cannot be None.
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        table_name: Target Delta table name where results will be stored
        source_silvers: List of Silver step names to use as input sources.
                       If None, uses all available Silver steps.
                       Allows selective consumption of Silver data.
        schema: Optional schema name for writing gold data

    Raises:
        ValidationError: If validation requirements are not met during construction

    Example:
        >>> def user_daily_metrics(spark, silvers):
        ...     events_df = silvers["clean_events"]
        ...     return (events_df
        ...         .groupBy("user_id", "event_date")
        ...         .agg(
        ...             F.count("*").alias("total_events"),
        ...             F.countDistinct("event_type").alias("unique_event_types"),
        ...             F.max("timestamp").alias("last_activity"),
        ...             F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases")
        ...         )
        ...         .withColumn("is_active_user", F.col("total_events") > 5)
        ...     )
        >>>
        >>> # Valid Gold step
        >>> gold_step = GoldStep(
        ...     name="user_metrics",
        ...     transform=user_daily_metrics,
        ...     rules={
        ...         "user_id": [F.col("user_id").isNotNull()],
        ...         "total_events": [F.col("total_events") > 0]
        ...     },
        ...     table_name="user_daily_metrics",
        ...     source_silvers=["clean_events"]
        ... )

        >>> # Invalid Gold step (will raise ValidationError)
        >>> try:
        ...     GoldStep(name="", transform=None, rules={}, table_name="", source_silvers=[])
        ... except ValidationError as e:
        ...     print(f"Validation failed: {e}")
        ...     # Output: "Step name must be a non-empty string"
    """

    name: str
    transform: GoldTransformFunction
    rules: ColumnRules
    table_name: str
    source_silvers: list[str] | None = None
    schema: str | None = None

    def __post_init__(self) -> None:
        """Validate required fields after initialization."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("Step name must be a non-empty string")
        if self.transform is None or not callable(self.transform):
            raise ValidationError("Transform function is required and must be callable")
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValidationError("Table name must be a non-empty string")
        if not isinstance(self.rules, dict) or not self.rules:
            raise ValidationError("Rules must be a non-empty dictionary")
        if self.source_silvers is not None and (
            not isinstance(self.source_silvers, list) or not self.source_silvers
        ):
            raise ValidationError("Source silvers must be a non-empty list")

    def validate(self) -> None:
        """Validate gold step configuration."""
        if not self.name or not isinstance(self.name, str):
            raise PipelineValidationError("Step name must be a non-empty string")
        if not callable(self.transform):
            raise PipelineValidationError("Transform must be a callable function")
        if not isinstance(self.rules, dict):
            raise PipelineValidationError("Rules must be a dictionary")
        if not self.table_name or not isinstance(self.table_name, str):
            raise PipelineValidationError("Table name must be a non-empty string")
        if self.source_silvers is not None and not isinstance(
            self.source_silvers, list
        ):
            raise PipelineValidationError("Source silvers must be a list or None")
