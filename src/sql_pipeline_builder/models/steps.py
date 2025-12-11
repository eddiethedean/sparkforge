"""
SQL step models for the Pipeline Builder.

This module provides SQL-specific step implementations using SQLAlchemy ORM.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from pipeline_builder_base.errors import PipelineValidationError, ValidationError
from pipeline_builder_base.models import BaseModel

from sql_pipeline_builder.types import (
    GoldTransformFunction,
    SilverTransformFunction,
    SqlColumnRules,
)


@dataclass
class SqlBronzeStep(BaseModel):
    """
    SQL Bronze layer step configuration for raw data validation and ingestion.

    Bronze steps represent the first layer of the Medallion Architecture,
    handling raw data validation using SQLAlchemy ORM objects and expressions.

    **Validation Requirements:**
        - `name`: Must be a non-empty string
        - `rules`: Must be a non-empty dictionary with SQLAlchemy validation rules
        - `incremental_col`: Must be a string if provided

    Attributes:
        name: Unique identifier for this Bronze step
        rules: Dictionary mapping column names to validation rule lists.
               Each rule should be a SQLAlchemy ColumnElement expression.
               Examples:
               - User.email.is_not(None)
               - column('age').between(18, 65)
               - User.status.in_(['active', 'inactive'])
        incremental_col: Column name for incremental processing (e.g., "timestamp").
                        If provided, enables watermarking for efficient updates.
                        If None, forces full refresh mode for downstream steps.
        schema: Optional schema name for reading bronze data
        model_class: SQLAlchemy ORM model class for this bronze step

    Raises:
        ValidationError: If validation requirements are not met during construction

    Example:
        >>> from sqlalchemy import Column, Integer, String
        >>> from sqlalchemy.ext.declarative import declarative_base
        >>> from sqlalchemy.sql import column
        >>>
        >>> Base = declarative_base()
        >>>
        >>> class User(Base):
        ...     __tablename__ = 'users'
        ...     id = Column(Integer, primary_key=True)
        ...     email = Column(String)
        ...     age = Column(Integer)
        >>>
        >>> # Valid Bronze step with SQLAlchemy expressions
        >>> bronze_step = SqlBronzeStep(
        ...     name="user_events",
        ...     rules={
        ...         "email": [User.email.is_not(None)],
        ...         "age": [column('age').between(18, 65)],
        ...     },
        ...     incremental_col="timestamp",
        ...     model_class=User
        ... )
    """

    name: str
    rules: SqlColumnRules
    incremental_col: Optional[str] = None
    schema: Optional[str] = None
    model_class: Optional[Any] = None  # SQLAlchemy ORM model class

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
class SqlSilverStep(BaseModel):
    """
    SQL Silver layer step configuration for data cleaning and enrichment.

    Silver steps represent the second layer of the Medallion Architecture,
    transforming raw Bronze data into clean, business-ready datasets using
    SQLAlchemy ORM queries.

    **Validation Requirements:**
        - `name`: Must be a non-empty string
        - `source_bronze`: Must be a non-empty string
        - `transform`: Must be callable and cannot be None
        - `rules`: Must be a non-empty dictionary with validation rules
        - `table_name`: Must be a non-empty string

    Attributes:
        name: Unique identifier for this Silver step
        source_bronze: Name of the Bronze step providing input data
        transform: Transformation function with signature:
                 (session: Session, bronze_query: Query, prior_silvers: Dict[str, Query]) -> Query
                 Must be callable and cannot be None.
        rules: Dictionary mapping column names to SQLAlchemy validation rule lists.
               Each rule should be a SQLAlchemy ColumnElement expression.
        table_name: Target SQL table name where results will be stored
        watermark_col: Column name for watermarking (e.g., "timestamp", "updated_at").
                      If provided, enables incremental processing with append mode.
                      If None, uses overwrite mode for full refresh.
        schema: Optional schema name for writing silver data
        model_class: SQLAlchemy ORM model class for this silver step

    Raises:
        ValidationError: If validation requirements are not met during construction

    Example:
        >>> def clean_user_events(session, bronze_query, prior_silvers):
        ...     from sqlalchemy import func
        ...     return (bronze_query
        ...         .filter(User.email.is_not(None))
        ...         .with_entities(
        ...             User.id,
        ...             User.email,
        ...             func.date_trunc('day', User.timestamp).label('event_date')
        ...         )
        ...     )
        >>>
        >>> silver_step = SqlSilverStep(
        ...     name="clean_events",
        ...     source_bronze="user_events",
        ...     transform=clean_user_events,
        ...     rules={
        ...         "email": [User.email.is_not(None)],
        ...     },
        ...     table_name="clean_user_events",
        ...     model_class=CleanEvent
        ... )
    """

    name: str
    source_bronze: str
    transform: SilverTransformFunction
    rules: SqlColumnRules
    table_name: str
    watermark_col: Optional[str] = None
    schema: Optional[str] = None
    model_class: Optional[Any] = None  # SQLAlchemy ORM model class

    def __post_init__(self) -> None:
        """Validate required fields after initialization."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("Step name must be a non-empty string")
        if not self.source_bronze or not isinstance(self.source_bronze, str):
            raise ValidationError("Source bronze step name must be a non-empty string")
        if self.transform is None or not callable(self.transform):
            raise ValidationError("Transform function is required and must be callable")
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValidationError("Table name must be a non-empty string")
        if self.model_class is None or not hasattr(self.model_class, "__table__"):
            raise ValidationError(
                "Silver steps require a SQLAlchemy model_class with __table__ metadata"
            )

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
        if self.model_class is None or not hasattr(self.model_class, "__table__"):
            raise PipelineValidationError(
                "Silver step model_class is required to create destination tables"
            )


@dataclass
class SqlGoldStep(BaseModel):
    """
    SQL Gold layer step configuration for business analytics and reporting.

    Gold steps represent the third layer of the Medallion Architecture,
    creating business-ready datasets for analytics, reporting, and dashboards
    using SQLAlchemy ORM queries.

    **Validation Requirements:**
        - `name`: Must be a non-empty string
        - `transform`: Must be callable and cannot be None
        - `rules`: Must be a non-empty dictionary with validation rules
        - `table_name`: Must be a non-empty string
        - `source_silvers`: Must be a non-empty list if provided

    Attributes:
        name: Unique identifier for this Gold step
        transform: Transformation function with signature:
                 (session: Session, silvers: Dict[str, Query]) -> Query
                 - session: Active SQLAlchemy Session for operations
                 - silvers: Dictionary of all Silver Queries by step name
                 Must be callable and cannot be None.
        rules: Dictionary mapping column names to SQLAlchemy validation rule lists.
               Each rule should be a SQLAlchemy ColumnElement expression.
        table_name: Target SQL table name where results will be stored
        source_silvers: List of Silver step names to use as input sources.
                       If None, uses all available Silver steps.
                       Allows selective consumption of Silver data.
        schema: Optional schema name for writing gold data
        model_class: SQLAlchemy ORM model class for this gold step

    Raises:
        ValidationError: If validation requirements are not met during construction

    Example:
        >>> def daily_metrics(session, silvers):
        ...     from sqlalchemy import func
        ...     clean_events = silvers["clean_events"]
        ...     return (clean_events
        ...         .with_entities(
        ...             clean_events.c.event_date,
        ...             func.count(clean_events.c.id).label('total_events'),
        ...             func.count(func.distinct(clean_events.c.user_id)).label('unique_users')
        ...         )
        ...         .group_by(clean_events.c.event_date)
        ...     )
        >>>
        >>> gold_step = SqlGoldStep(
        ...     name="daily_metrics",
        ...     transform=daily_metrics,
        ...     rules={
        ...         "event_date": [column('event_date').is_not(None)],
        ...     },
        ...     table_name="daily_metrics",
        ...     source_silvers=["clean_events"],
        ...     model_class=DailyMetric
        ... )
    """

    name: str
    transform: GoldTransformFunction
    rules: SqlColumnRules
    table_name: str
    source_silvers: Optional[list[str]] = None
    schema: Optional[str] = None
    model_class: Optional[Any] = None  # SQLAlchemy ORM model class

    def __post_init__(self) -> None:
        """Validate required fields after initialization."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("Step name must be a non-empty string")
        if self.transform is None or not callable(self.transform):
            raise ValidationError("Transform function is required and must be callable")
        if not isinstance(self.rules, dict) or not self.rules:
            raise ValidationError("Rules must be a non-empty dictionary")
        if not self.table_name or not isinstance(self.table_name, str):
            raise ValidationError("Table name must be a non-empty string")
        if self.source_silvers is not None and not isinstance(
            self.source_silvers, list
        ):
            raise ValidationError("source_silvers must be a list or None")
        if self.model_class is None or not hasattr(self.model_class, "__table__"):
            raise ValidationError(
                "Gold steps require a SQLAlchemy model_class with __table__ metadata"
            )

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
            raise PipelineValidationError("source_silvers must be a list or None")
        if self.model_class is None or not hasattr(self.model_class, "__table__"):
            raise PipelineValidationError(
                "Gold step model_class is required to create destination tables"
            )
