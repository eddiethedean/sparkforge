"""
Step models for the Pipeline Builder.

# Depends on:
#   errors
#   models.base
#   models.types
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

# TypeAlias is available in Python 3.10+, use typing_extensions for 3.8/3.9
# Mypy prefers typing_extensions even for Python 3.11
from typing_extensions import TypeAlias

from pipeline_builder_base.errors import PipelineValidationError, ValidationError

from .base import BaseModel
from .enums import PipelinePhase
from .types import ColumnRules, GoldTransformFunction, SilverTransformFunction

if TYPE_CHECKING:
    # Engine-specific StructType should satisfy the TypesProtocol.StructType
    # Import the actual type from pyspark.sql.types for type checking
    try:
        from pyspark.sql.types import StructType as _StructTypeBase
    except ImportError:
        # Fallback if PySpark not available during type checking
        from typing import Any as _StructTypeBase  # type: ignore[assignment]

    StructType: TypeAlias = _StructTypeBase
else:
    try:
        from ..compat import types as compat_types

        StructType: TypeAlias = compat_types.StructType  # type: ignore[assignment]
    except Exception:
        # Use object instead of Any for Python 3.8 compatibility
        # Any cannot be used with isinstance() in Python 3.8
        # For runtime, we use object, but mypy will use the TypeAlias from TYPE_CHECKING
        StructType: TypeAlias = object  # type: ignore[assignment, misc]


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
        >>> from pipeline_builder.functions import get_default_functions
        >>> F = get_default_functions()
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
    incremental_col: Optional[str] = None
    schema: Optional[str] = None

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

    @property
    def step_type(self) -> PipelinePhase:
        """Return the pipeline phase for this step."""
        return PipelinePhase.BRONZE


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
                 (spark: SparkSession  # type: ignore[valid-type], bronze_df: DataFrame  # type: ignore[valid-type], prior_silvers: Dict[str, DataFrame]  # type: ignore[valid-type]) -> DataFrame
                 Must be callable and cannot be None.
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a PySpark Column expression.
        table_name: Target Delta table name where results will be stored
        watermark_col: Column name for watermarking (e.g., "timestamp", "updated_at").
                      If provided, enables incremental processing with append mode.
                      If None, uses overwrite mode for full refresh.
        existing: Whether this represents an existing table (for validation-only steps)
        schema: Optional schema name for writing silver data
        schema_override: Optional PySpark StructType schema to override DataFrame schema
                        when creating tables. Uses Delta Lake's overwriteSchema option.
                        Applied during initial runs and when table doesn't exist.

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
    watermark_col: Optional[str] = None
    existing: bool = False
    schema: Optional[str] = None
    source_incremental_col: Optional[str] = None
    schema_override: Optional[StructType] = None
    source_silvers: Optional[list[str]] = None

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
        if self.source_incremental_col is not None and not isinstance(
            self.source_incremental_col, str
        ):
            raise ValidationError("source_incremental_col must be a string")
        if self.schema_override is not None:
            # Accept any StructType-like object; engine enforces correctness at write time.
            pass

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
        if self.source_incremental_col is not None and not isinstance(
            self.source_incremental_col, str
        ):
            raise PipelineValidationError(
                "source_incremental_col must be a string when provided"
            )
        if self.schema_override is not None:
            # Accept any StructType-like object; engine enforces correctness at write time.
            pass

    @property
    def step_type(self) -> PipelinePhase:
        """Return the pipeline phase for this step."""
        return PipelinePhase.SILVER


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
                 (spark: SparkSession  # type: ignore[valid-type], silvers: Dict[str, DataFrame]  # type: ignore[valid-type]) -> DataFrame
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
        schema_override: Optional PySpark StructType schema to override DataFrame schema
                        when writing to gold tables. Uses Delta Lake's overwriteSchema option.
                        Always applied for gold table writes.

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
    source_silvers: Optional[list[str]] = None
    schema: Optional[str] = None
    schema_override: Optional[StructType] = None

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
        if self.schema_override is not None:
            # Accept any StructType-like object; engine enforces correctness.
            pass

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
        if self.schema_override is not None:
            # Accept any StructType-like object; engine enforces correctness.
            pass

    @property
    def step_type(self) -> PipelinePhase:
        """Return the pipeline phase for this step."""
        return PipelinePhase.GOLD
