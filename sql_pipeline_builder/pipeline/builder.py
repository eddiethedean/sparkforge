"""
SQL PipelineBuilder for the framework.

This module provides a clean, maintainable SqlPipelineBuilder that handles
pipeline construction with the Medallion Architecture (Bronze → Silver → Gold)
using SQLAlchemy.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from pipeline_builder_base.errors import ConfigurationError, ValidationError
from pipeline_builder_base.models.exceptions import PipelineConfigurationError
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    ParallelConfig,
    PipelineConfig,
    ValidationThresholds,
)
from sql_pipeline_builder.engine import SqlEngine
from sql_pipeline_builder.models import SqlBronzeStep, SqlGoldStep, SqlSilverStep
from sql_pipeline_builder.types import (
    GoldTransformFunction,
    SilverTransformFunction,
    SqlColumnRules,
)
from .runner import SqlPipelineRunner


class SqlPipelineBuilder:
    """
    Production-ready builder for creating SQL pipelines with Bronze → Silver → Gold architecture.

    The SqlPipelineBuilder provides a fluent API for constructing robust data pipelines with
    comprehensive validation, automatic dependency management, and enterprise-grade features
    using SQLAlchemy ORM.

    Key Features:
    - **Fluent API**: Chain methods for intuitive pipeline construction
    - **Robust Validation**: Early error detection with clear validation messages
    - **Auto-inference**: Automatic dependency detection and validation
    - **SQLAlchemy Expressions**: Use native SQLAlchemy ORM methods for validation
    - **Multi-schema Support**: Cross-schema data flows for enterprise environments
    - **Comprehensive Error Handling**: Detailed error messages with suggestions

    Validation Requirements:
        All pipeline steps must have validation rules using SQLAlchemy expressions.
        Invalid configurations are rejected during construction with clear error messages.

    Example:
        from sql_pipeline_builder import SqlPipelineBuilder
        from sqlalchemy import Column, Integer, String
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.orm import Session
        from sqlalchemy.sql import column

        Base = declarative_base()

        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            email = Column(String)
            age = Column(Integer)

        # Initialize builder
        session = Session(engine)
        builder = SqlPipelineBuilder(session=session, schema="analytics")

        # Bronze: Raw data validation (required)
        builder.with_bronze_rules(
            name="events",
            rules={"email": [User.email.is_not(None)], "age": [column('age').between(18, 65)]},
            incremental_col="timestamp",
            model_class=User
        )

        # Silver: Data transformation (required)
        def clean_events(session, bronze_query, silvers):
            return bronze_query.filter(User.email.is_not(None))

        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=clean_events,
            rules={"email": [User.email.is_not(None)]},
            table_name="clean_events"
        )

        # Gold: Business analytics (required)
        def daily_metrics(session, silvers):
            clean_events = silvers["clean_events"]
            return clean_events.group_by(clean_events.c.event_date).with_entities(...)

        builder.add_gold_transform(
            name="daily_metrics",
            transform=daily_metrics,
            rules={"event_date": [column('event_date').is_not(None)]},
            table_name="daily_metrics",
            source_silvers=["clean_events"]
        )

        # Execute pipeline
        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": session.query(User)})
    """

    def __init__(
        self,
        *,
        session: Any,  # SQLAlchemy Session or AsyncSession
        schema: str,
        min_bronze_rate: float = 95.0,
        min_silver_rate: float = 98.0,
        min_gold_rate: float = 99.0,
        verbose: bool = True,
    ) -> None:
        """
        Initialize a new SqlPipelineBuilder instance.

        Args:
            session: SQLAlchemy Session or AsyncSession instance
            schema: Database schema name where tables will be created
            min_bronze_rate: Minimum data quality rate for Bronze layer (0-100)
            min_silver_rate: Minimum data quality rate for Silver layer (0-100)
            min_gold_rate: Minimum data quality rate for Gold layer (0-100)
            verbose: Enable verbose logging output

        Raises:
            ValueError: If quality rates are not between 0 and 100
            RuntimeError: If session is not valid
        """
        # Validate inputs
        if not session:
            raise ConfigurationError(
                "SQLAlchemy session is required",
                suggestions=[
                    "Ensure Session is properly initialized",
                    "Check database connection",
                ],
            )
        if not schema:
            raise ConfigurationError(
                "Schema name cannot be empty",
                suggestions=[
                    "Provide a valid schema name",
                    "Check database configuration",
                ],
            )

        # Store configuration
        thresholds = ValidationThresholds(
            bronze=min_bronze_rate, silver=min_silver_rate, gold=min_gold_rate
        )
        parallel_config = ParallelConfig.create_default()
        self.config = PipelineConfig(
            schema=schema,
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=verbose,
        )

        # Initialize components
        self.session = session
        self.logger = PipelineLogger(verbose=verbose)
        self.schema = schema
        self.pipeline_id = (
            f"sql_pipeline_{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        # Pipeline definition
        self.bronze_steps: Dict[str, SqlBronzeStep] = {}
        self.silver_steps: Dict[str, SqlSilverStep] = {}
        self.gold_steps: Dict[str, SqlGoldStep] = {}

        # Create SqlEngine
        self.sql_engine = SqlEngine(
            session=self.session,
            config=self.config,
            logger=self.logger,
        )

        self.logger.info(f"🔧 SqlPipelineBuilder initialized (schema: {schema})")

    def with_bronze_rules(
        self,
        *,
        name: str,
        rules: SqlColumnRules,
        incremental_col: str | None = None,
        schema: str | None = None,
        model_class: Any | None = None,
    ) -> SqlPipelineBuilder:
        """
        Add Bronze layer validation rules for raw data ingestion.

        Args:
            name: Unique identifier for this Bronze step
            rules: Dictionary mapping column names to SQLAlchemy validation rule lists.
                   Examples:
                   - User.email.is_not(None)
                   - column('age').between(18, 65)
                   - User.status.in_(['active', 'inactive'])
            incremental_col: Column name for incremental processing
            schema: Optional schema name for reading bronze data
            model_class: Optional SQLAlchemy ORM model class

        Returns:
            Self for method chaining
        """
        if name in self.bronze_steps:
            raise ValidationError(f"Bronze step '{name}' already exists")

        step = SqlBronzeStep(
            name=name,
            rules=rules,
            incremental_col=incremental_col,
            schema=schema,
            model_class=model_class,
        )
        step.validate()

        self.bronze_steps[name] = step
        self.logger.info(f"✅ Added Bronze step: {name}")
        return self

    def add_silver_transform(
        self,
        *,
        name: str,
        source_bronze: str,
        transform: SilverTransformFunction,
        rules: SqlColumnRules,
        table_name: str,
        watermark_col: str | None = None,
        schema: str | None = None,
        model_class: Any | None = None,
    ) -> SqlPipelineBuilder:
        """
        Add Silver layer transformation step.

        Args:
            name: Unique identifier for this Silver step
            source_bronze: Name of the Bronze step providing input data
            transform: Transformation function (session, bronze_query, silvers) -> Query
            rules: Dictionary mapping column names to SQLAlchemy validation rules
            table_name: Target SQL table name
            watermark_col: Column name for watermarking
            schema: Optional schema name for writing silver data
            model_class: Optional SQLAlchemy ORM model class

        Returns:
            Self for method chaining
        """
        if name in self.silver_steps:
            raise ValidationError(f"Silver step '{name}' already exists")
        if source_bronze not in self.bronze_steps:
            raise ValidationError(f"Bronze step '{source_bronze}' not found")

        step = SqlSilverStep(
            name=name,
            source_bronze=source_bronze,
            transform=transform,
            rules=rules,
            table_name=table_name,
            watermark_col=watermark_col,
            schema=schema,
            model_class=model_class,
        )
        step.validate()

        self.silver_steps[name] = step
        self.logger.info(f"✅ Added Silver step: {name}")
        return self

    def add_gold_transform(
        self,
        *,
        name: str,
        transform: GoldTransformFunction,
        rules: SqlColumnRules,
        table_name: str,
        source_silvers: list[str] | None = None,
        schema: str | None = None,
        model_class: Any | None = None,
    ) -> SqlPipelineBuilder:
        """
        Add Gold layer transformation step.

        Args:
            name: Unique identifier for this Gold step
            transform: Transformation function (session, silvers) -> Query
            rules: Dictionary mapping column names to SQLAlchemy validation rules
            table_name: Target SQL table name
            source_silvers: List of Silver step names to use as input
            schema: Optional schema name for writing gold data
            model_class: Optional SQLAlchemy ORM model class

        Returns:
            Self for method chaining
        """
        if name in self.gold_steps:
            raise ValidationError(f"Gold step '{name}' already exists")
        if source_silvers:
            for silver_name in source_silvers:
                if silver_name not in self.silver_steps:
                    raise ValidationError(f"Silver step '{silver_name}' not found")

        step = SqlGoldStep(
            name=name,
            transform=transform,
            rules=rules,
            table_name=table_name,
            source_silvers=source_silvers,
            schema=schema,
            model_class=model_class,
        )
        step.validate()

        self.gold_steps[name] = step
        self.logger.info(f"✅ Added Gold step: {name}")
        return self

    def to_pipeline(self) -> SqlPipelineRunner:
        """
        Build and return a SqlPipelineRunner instance.

        Returns:
            SqlPipelineRunner ready for execution
        """
        # Allow flexible pipeline construction - users can build partial pipelines
        # (e.g., only Bronze validation, or only Silver transforms)
        if not self.bronze_steps and not self.silver_steps and not self.gold_steps:
            raise ValidationError("Pipeline must have at least one step (Bronze, Silver, or Gold)")

        return SqlPipelineRunner(
            session=self.session,
            config=self.config,
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
            logger=self.logger,
        )

