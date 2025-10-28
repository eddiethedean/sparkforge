"""
Simplified PipelineBuilder for the framework.

This module provides a clean, maintainable PipelineBuilder that handles
pipeline construction with the Medallion Architecture (Bronze → Silver → Gold).
The builder creates pipelines that can be executed with the simplified execution engine.

# Depends on:
#   compat
#   errors
#   functions
#   logging
#   models.base
#   models.pipeline
#   models.steps
#   pipeline.runner
#   types
#   validation.data_validation
#   validation.pipeline_validation
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from ..compat import DataFrame, SparkSession
from ..errors import ConfigurationError as PipelineConfigurationError
from ..errors import ExecutionError as StepError
from ..functions import FunctionsProtocol, get_default_functions
from ..logging import PipelineLogger
from ..models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)
from ..types import (
    ColumnRules,
    GoldTransformFunction,
    SilverTransformFunction,
    StepName,
    TableName,
)
from ..validation import UnifiedValidator as PipelineValidator
from ..validation import _convert_rules_to_expressions
from .runner import PipelineRunner


class PipelineBuilder:
    """
    Production-ready builder for creating data pipelines with Bronze → Silver → Gold architecture.

    The PipelineBuilder provides a fluent API for constructing robust data pipelines with
    comprehensive validation, automatic dependency management, and enterprise-grade features.

    Key Features:
    - **Fluent API**: Chain methods for intuitive pipeline construction
    - **Robust Validation**: Early error detection with clear validation messages
    - **Auto-inference**: Automatic dependency detection and validation
    - **String Rules**: Convert human-readable rules to PySpark expressions
    - **Multi-schema Support**: Cross-schema data flows for enterprise environments
    - **Comprehensive Error Handling**: Detailed error messages with suggestions

    Validation Requirements:
        All pipeline steps must have validation rules. Invalid configurations are rejected
        during construction with clear error messages.

    Example:
        from the framework import PipelineBuilder
        from pyspark.sql import functions as F

        # Initialize builder
        builder = PipelineBuilder(spark=spark, schema="analytics")

        # Bronze: Raw data validation (required)
        builder.with_bronze_rules(
            name="events",
            rules={"user_id": ["not_null"], "timestamp": ["not_null"]},  # String rules
            incremental_col="timestamp"
        )

        # Silver: Data transformation (required)
        builder.add_silver_transform(
            name="clean_events",
            source_bronze="events",
            transform=lambda spark, df, silvers: df.filter(F.col("value") > 0),
            rules={"value": ["gt", 0]},  # String rules
            table_name="clean_events"
        )

        # Gold: Business analytics (required)
        builder.add_gold_transform(
            name="daily_metrics",
            transform=lambda spark, silvers: silvers["clean_events"].groupBy("date").agg(F.count("*").alias("count")),
            rules={"count": ["gt", 0]},  # String rules
            table_name="daily_metrics",
            source_silvers=["clean_events"]
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()
        result = pipeline.run_initial_load(bronze_sources={"events": source_df})

    String Rules Support:
        You can use human-readable string rules that are automatically converted to PySpark expressions:

        - "not_null" → F.col("column").isNotNull()
        - "gt", value → F.col("column") > value
        - "lt", value → F.col("column") < value
        - "eq", value → F.col("column") == value
        - "in", [values] → F.col("column").isin(values)
        - "between", min, max → F.col("column").between(min, max)

    Args:
        spark: Active SparkSession instance
        schema: Target schema name for pipeline tables
        quality_thresholds: Validation thresholds for each layer (default: Bronze=90%, Silver=95%, Gold=98%)
        parallel_config: Parallel execution configuration
        logger: Optional logger instance

    Raises:
        ValidationError: If validation rules are invalid or missing
        ConfigurationError: If configuration parameters are invalid
        StepError: If step dependencies cannot be resolved

    Example:
        >>> from the framework import PipelineBuilder
        >>> from pyspark.sql import SparkSession, functions as F
        >>>
        >>> spark = SparkSession.builder.appName("My Pipeline").getOrCreate()
        >>> builder = PipelineBuilder(spark=spark, schema="my_schema")
        >>>
        >>> # Bronze layer - raw data validation
        >>> builder.with_bronze_rules(
        ...     name="events",
        ...     rules={"user_id": [F.col("user_id").isNotNull()]},
        ...     incremental_col="timestamp"
        ... )
        >>>
        >>> # Silver layer - data transformation
        >>> builder.add_silver_transform(
        ...     name="clean_events",
        ...     source_bronze="events",
        ...     transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
        ...     rules={"status": [F.col("status").isNotNull()]},
        ...     table_name="clean_events",
        ...     watermark_col="timestamp"
        ... )
        >>>
        >>> # Gold layer - business analytics
        >>> builder.add_gold_transform(
        ...     name="user_analytics",
        ...     transform=lambda spark, silvers: silvers["clean_events"].groupBy("user_id").count(),
        ...     rules={"user_id": [F.col("user_id").isNotNull()]},
        ...     table_name="user_analytics",
        ...     source_silvers=["clean_events"]
        ... )
        >>>
        >>> # Build and execute pipeline
        >>> pipeline = builder.to_pipeline()
        >>> result = pipeline.initial_load(bronze_sources={"events": source_df})
    """

    def __init__(
        self,
        *,
        spark: SparkSession,
        schema: str,
        min_bronze_rate: float = 95.0,
        min_silver_rate: float = 98.0,
        min_gold_rate: float = 99.0,
        verbose: bool = True,
        functions: FunctionsProtocol | None = None,
    ) -> None:
        """
        Initialize a new PipelineBuilder instance.

        Args:
            spark: Active SparkSession instance for data processing
            schema: Database schema name where tables will be created
            min_bronze_rate: Minimum data quality rate for Bronze layer (0-100)
            min_silver_rate: Minimum data quality rate for Silver layer (0-100)
            min_gold_rate: Minimum data quality rate for Gold layer (0-100)
            verbose: Enable verbose logging output

        Raises:
            ValueError: If quality rates are not between 0 and 100
            RuntimeError: If Spark session is not active
        """
        # Validate inputs
        if not spark:
            raise PipelineConfigurationError(
                "Spark session is required",
                suggestions=[
                    "Ensure SparkSession is properly initialized",
                    "Check Spark configuration",
                ],
            )
        if not schema:
            raise PipelineConfigurationError(
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
        # Use default parallel config (enabled with 4 workers)
        parallel_config = ParallelConfig.create_default()
        self.config = PipelineConfig(
            schema=schema,
            thresholds=thresholds,
            parallel=parallel_config,
            verbose=verbose,
        )

        # Initialize components
        self.spark = spark
        self.logger = PipelineLogger(verbose=verbose)
        self.validator = PipelineValidator(self.logger)
        self.functions = functions if functions is not None else get_default_functions()

        # Expose schema for backward compatibility
        self.schema = schema
        self.pipeline_id = (
            f"pipeline_{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        # Expose validators for backward compatibility
        self.validators = self.validator.custom_validators

        # Pipeline definition
        self.bronze_steps: Dict[str, BronzeStep] = {}
        self.silver_steps: Dict[str, SilverStep] = {}
        self.gold_steps: Dict[str, GoldStep] = {}

        self.logger.info(f"🔧 PipelineBuilder initialized (schema: {schema})")

    def with_bronze_rules(
        self,
        *,
        name: StepName,
        rules: ColumnRules,
        incremental_col: str | None = None,
        description: str | None = None,
        schema: str | None = None,
    ) -> PipelineBuilder:
        """
        Add Bronze layer validation rules for raw data ingestion.

        Bronze steps represent the first layer of the Medallion Architecture,
        handling raw data ingestion and initial validation. All Bronze steps
        must have non-empty validation rules.

        Args:
            name: Unique identifier for this Bronze step
            rules: Dictionary mapping column names to validation rule lists.
                   Supports both PySpark Column expressions and string rules:
                   - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                   - String: {"user_id": ["not_null"], "age": ["gt", 0]}
            incremental_col: Column name for incremental processing (e.g., "timestamp", "updated_at").
                            If provided, enables incremental processing with append mode.
            description: Optional description of this Bronze step
            schema: Optional schema name for reading bronze data. If not provided, uses the builder's default schema.

        Returns:
            Self for method chaining

        Raises:
            ValidationError: If rules are empty or invalid
            ConfigurationError: If step name conflicts or configuration is invalid

        Example:
            >>> # Using PySpark Column expressions
            >>> builder.with_bronze_rules(
            ...     name="events",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     incremental_col="timestamp"
            ... )

            >>> # Using string rules (automatically converted)
            >>> builder.with_bronze_rules(
            ...     name="users",
            ...     rules={"user_id": ["not_null"], "age": ["gt", 0], "status": ["in", ["active", "inactive"]]},
            ...     incremental_col="updated_at"
            ... )

        String Rules Support:
            - "not_null" → F.col("column").isNotNull()
            - "gt", value → F.col("column") > value
            - "lt", value → F.col("column") < value
            - "eq", value → F.col("column") == value
            - "in", [values] → F.col("column").isin(values)
            - "between", min, max → F.col("column").between(min, max)
            ...     name="user_events",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     incremental_col="timestamp",
            ...     schema="raw_data"  # Read from different schema
            ... )
        """
        if not name:
            raise StepError(
                "Bronze step name cannot be empty",
                context={"step_name": name or "unknown", "step_type": "bronze"},
                suggestions=[
                    "Provide a valid step name",
                    "Check step naming conventions",
                ],
            )

        if name in self.bronze_steps:
            raise StepError(
                f"Bronze step '{name}' already exists",
                context={"step_name": name, "step_type": "bronze"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            )

        # Validate schema if provided
        if schema is not None:
            self._validate_schema(schema)

        # Convert string rules to PySpark Column objects
        converted_rules = _convert_rules_to_expressions(rules, self.functions)

        # Create bronze step
        bronze_step = BronzeStep(
            name=name,
            rules=converted_rules,
            incremental_col=incremental_col,
            schema=schema,
        )

        self.bronze_steps[name] = bronze_step
        self.logger.info(f"✅ Added Bronze step: {name}")

        return self

    def with_silver_rules(
        self,
        *,
        name: StepName,
        table_name: TableName,
        rules: ColumnRules,
        watermark_col: str | None = None,
        description: str | None = None,
        schema: str | None = None,
    ) -> PipelineBuilder:
        """
        Add existing Silver layer table for validation and monitoring.

        This method is used when you have an existing Silver table that you want to
        include in the pipeline for validation and monitoring purposes, but don't
        need to transform the data.

        Args:
            name: Unique identifier for this Silver step
            table_name: Existing Delta table name
            rules: Dictionary mapping column names to validation rule lists
            watermark_col: Column name for watermarking (optional)
            description: Optional description of this Silver step
            schema: Optional schema name for reading silver data. If not provided, uses the builder's default schema.

        Returns:
            Self for method chaining

        Example:
            >>> builder.with_silver_rules(
            ...     name="existing_clean_events",
            ...     table_name="clean_events",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     watermark_col="updated_at",
            ...     schema="staging"  # Read from different schema
            ... )
        """
        if not name:
            raise StepError(
                "Silver step name cannot be empty",
                context={"step_name": name or "unknown", "step_type": "silver"},
                suggestions=[
                    "Provide a valid step name",
                    "Check step naming conventions",
                ],
            )

        if name in self.silver_steps:
            raise StepError(
                f"Silver step '{name}' already exists",
                context={"step_name": name, "step_type": "silver"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            )

        # Validate schema if provided
        if schema is not None:
            self._validate_schema(schema)

        # Create SilverStep for existing table
        # Create a dummy transform function for existing tables
        def dummy_transform_func(
            spark: SparkSession,
            bronze_df: DataFrame,
            prior_silvers: Dict[str, DataFrame],
        ) -> DataFrame:
            return bronze_df

        # Type the function properly
        dummy_transform: SilverTransformFunction = dummy_transform_func

        # Convert string rules to PySpark Column objects
        converted_rules = _convert_rules_to_expressions(rules, self.functions)

        silver_step = SilverStep(
            name=name,
            source_bronze="",  # No source for existing tables
            transform=dummy_transform,
            rules=converted_rules,
            table_name=table_name,
            watermark_col=watermark_col,
            existing=True,
            schema=schema,
        )

        self.silver_steps[name] = silver_step
        self.logger.info(f"✅ Added existing Silver step: {name}")

        return self

    def add_validator(self, validator: Any) -> PipelineBuilder:
        """
        Add a custom step validator to the pipeline.

        Custom validators allow you to add additional validation logic
        beyond the built-in validation rules.

        Args:
            validator: Custom validator implementing StepValidator protocol

        Returns:
            Self for method chaining

        Example:
            >>> class CustomValidator(StepValidator):
            ...     def validate(self, step, context):
            ...         if step.name == "special_step":
            ...             return ["Special validation failed"]
            ...         return []
            >>>
            >>> builder.add_validator(CustomValidator())
        """
        self.validator.add_validator(validator)
        return self

    def add_silver_transform(
        self,
        *,
        name: StepName,
        source_bronze: StepName | None = None,
        transform: SilverTransformFunction,
        rules: ColumnRules,
        table_name: TableName,
        watermark_col: str | None = None,
        description: str | None = None,
        depends_on: list[StepName] | None = None,
        schema: str | None = None,
    ) -> PipelineBuilder:
        """
        Add Silver layer transformation step for data cleaning and enrichment.

        Silver steps represent the second layer of the Medallion Architecture,
        transforming raw Bronze data into clean, business-ready datasets. All Silver steps
        must have non-empty validation rules and a valid transform function.

        Args:
            name: Unique identifier for this Silver step
            source_bronze: Name of the Bronze step this Silver step depends on.
                          If not provided, will automatically infer from the most recent
                          with_bronze_rules() call. If no bronze steps exist, will raise an error.
            transform: Transformation function with signature:
                     (spark: SparkSession, bronze_df: DataFrame, prior_silvers: Dict[str, DataFrame]) -> DataFrame
                     Must be callable and cannot be None.
            rules: Dictionary mapping column names to validation rule lists.
                   Supports both PySpark Column expressions and string rules:
                   - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                   - String: {"user_id": ["not_null"], "age": ["gt", 0]}
            table_name: Target Delta table name where results will be stored
            watermark_col: Column name for watermarking (e.g., "timestamp", "updated_at").
                          If provided, enables incremental processing with append mode.
            description: Optional description of this Silver step
            depends_on: List of other Silver step names that must complete before this step.
            schema: Optional schema name for writing silver data. If not provided, uses the builder's default schema.

        Returns:
            Self for method chaining

        Raises:
            ValidationError: If rules are empty, transform is None, or configuration is invalid
            ConfigurationError: If step name conflicts or dependencies cannot be resolved

        Example:
            >>> def clean_user_events(spark, bronze_df, prior_silvers):
            ...     return (bronze_df
            ...         .filter(F.col("user_id").isNotNull())
            ...         .withColumn("event_date", F.date_trunc("day", "timestamp"))
            ...     )
            >>>
            >>> # Using PySpark Column expressions
            >>> builder.add_silver_transform(
            ...     name="clean_events",
            ...     source_bronze="user_events",
            ...     transform=clean_user_events,
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     table_name="clean_events"
            ... )

            >>> # Using string rules (automatically converted)
            >>> builder.add_silver_transform(
            ...     name="enriched_events",
            ...     source_bronze="user_events",
            ...     transform=lambda spark, df, silvers: df.withColumn("processed_at", F.current_timestamp()),
            ...     rules={"user_id": ["not_null"], "processed_at": ["not_null"]},
            ...     table_name="enriched_events",
            ...     watermark_col="processed_at"
            ... )

        String Rules Support:
            - "not_null" → F.col("column").isNotNull()
            - "gt", value → F.col("column") > value
            - "lt", value → F.col("column") < value
            - "eq", value → F.col("column") == value
            - "in", [values] → F.col("column").isin(values)
            - "between", min, max → F.col("column").between(min, max)
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     table_name="clean_user_events",
            ...     watermark_col="timestamp"
            ... )
            >>>
            >>> # Auto-infer source_bronze from most recent with_bronze_rules()
            >>> builder.add_silver_transform(
            ...     name="enriched_events",
            ...     transform=enrich_user_events,
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     table_name="enriched_user_events",
            ...     schema="processing"  # Write to different schema
            ... )
        """
        if not name:
            raise StepError(
                "Silver step name cannot be empty",
                context={"step_name": name or "unknown", "step_type": "silver"},
                suggestions=[
                    "Provide a valid step name",
                    "Check step naming conventions",
                ],
            )

        if name in self.silver_steps:
            raise StepError(
                f"Silver step '{name}' already exists",
                context={"step_name": name, "step_type": "silver"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            )

        # Auto-infer source_bronze if not provided
        if source_bronze is None:
            if not self.bronze_steps:
                raise StepError(
                    "No bronze steps available for auto-inference",
                    context={"step_name": name, "step_type": "silver"},
                    suggestions=[
                        "Add a bronze step first using with_bronze_rules()",
                        "Explicitly specify source_bronze parameter",
                    ],
                )

            # Use the most recently added bronze step
            source_bronze = list(self.bronze_steps.keys())[-1]
            self.logger.info(f"🔍 Auto-inferred source_bronze: {source_bronze}")

        # Validate that the source_bronze exists
        if source_bronze not in self.bronze_steps:
            raise StepError(
                f"Bronze step '{source_bronze}' not found",
                context={"step_name": name, "step_type": "silver"},
                suggestions=[
                    f"Available bronze steps: {list(self.bronze_steps.keys())}",
                    "Add the bronze step first using with_bronze_rules()",
                ],
            )

        # Note: Dependency validation is deferred to validate_pipeline()
        # This allows for more flexible pipeline construction

        # Use builder's schema if not provided
        if schema is None:
            schema = self.config.schema
        else:
            self._validate_schema(schema)

        # Convert string rules to PySpark Column objects
        converted_rules = _convert_rules_to_expressions(rules, self.functions)

        # Create silver step
        silver_step = SilverStep(
            name=name,
            source_bronze=source_bronze,
            transform=transform,
            rules=converted_rules,
            table_name=table_name,
            watermark_col=watermark_col,
            schema=schema,
        )

        self.silver_steps[name] = silver_step
        self.logger.info(f"✅ Added Silver step: {name} (source: {source_bronze})")

        return self

    def add_gold_transform(
        self,
        *,
        name: StepName,
        transform: GoldTransformFunction,
        rules: ColumnRules,
        table_name: TableName,
        source_silvers: list[StepName] | None = None,
        description: str | None = None,
        schema: str | None = None,
    ) -> PipelineBuilder:
        """
        Add Gold layer transformation step for business analytics and aggregations.

        Gold steps represent the third layer of the Medallion Architecture,
        creating business-ready datasets for analytics and reporting. All Gold steps
        must have non-empty validation rules and a valid transform function.

        Args:
            name: Unique identifier for this Gold step
            transform: Transformation function with signature:
                     (spark: SparkSession, silvers: Dict[str, DataFrame]) -> DataFrame
                     Must be callable and cannot be None.
            rules: Dictionary mapping column names to validation rule lists.
                   Supports both PySpark Column expressions and string rules:
                   - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                   - String: {"user_id": ["not_null"], "count": ["gt", 0]}
            table_name: Target Delta table name where results will be stored
            source_silvers: List of Silver step names this Gold step depends on.
                           If not provided, will automatically use all available Silver steps.
                           If no Silver steps exist, will raise an error.
            description: Optional description of this Gold step
            schema: Optional schema name for writing gold data. If not provided, uses the builder's default schema.

        Returns:
            Self for method chaining

        Raises:
            ValidationError: If rules are empty, transform is None, or configuration is invalid
            ConfigurationError: If step name conflicts or dependencies cannot be resolved

        Example:
            >>> def user_daily_metrics(spark, silvers):
            ...     events_df = silvers["clean_events"]
            ...     return (events_df
            ...         .groupBy("user_id", "event_date")
            ...         .agg(F.count("*").alias("event_count"))
            ...     )
            >>>
            >>> # Using PySpark Column expressions
            >>> builder.add_gold_transform(
            ...     name="user_metrics",
            ...     transform=user_daily_metrics,
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     table_name="user_daily_metrics",
            ...     source_silvers=["clean_events"]
            ... )

            >>> # Using string rules (automatically converted)
            >>> builder.add_gold_transform(
            ...     name="daily_analytics",
            ...     transform=lambda spark, silvers: silvers["clean_events"].groupBy("date").agg(F.count("*").alias("count")),
            ...     rules={"date": ["not_null"], "count": ["gt", 0]},
            ...     table_name="daily_analytics",
            ...     source_silvers=["clean_events"]
            ... )

        String Rules Support:
            - "not_null" → F.col("column").isNotNull()
            - "gt", value → F.col("column") > value
            - "lt", value → F.col("column") < value
            - "eq", value → F.col("column") == value
            - "in", [values] → F.col("column").isin(values)
            - "between", min, max → F.col("column").between(min, max)
            >>> # Auto-infer source_silvers from all available Silver steps
            >>> builder.add_gold_transform(
            ...     name="daily_analytics",
            ...     transform=daily_analytics,
            ...     rules={"event_date": [F.col("event_date").isNotNull()]},
            ...     table_name="daily_analytics",
            ...     schema="analytics"  # Write to different schema
            ... )
        """
        if not name:
            raise StepError(
                "Gold step name cannot be empty",
                context={"step_name": name or "unknown", "step_type": "gold"},
                suggestions=[
                    "Provide a valid step name",
                    "Check step naming conventions",
                ],
            )

        if name in self.gold_steps:
            raise StepError(
                f"Gold step '{name}' already exists",
                context={"step_name": name, "step_type": "gold"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            )

        # Auto-infer source_silvers if not provided
        if source_silvers is None:
            if not self.silver_steps:
                raise StepError(
                    "No silver steps available for auto-inference",
                    context={"step_name": name, "step_type": "gold"},
                    suggestions=[
                        "Add a silver step first using add_silver_transform()",
                        "Explicitly specify source_silvers parameter",
                    ],
                )

            # Use all available silver steps
            source_silvers = list(self.silver_steps.keys())
            self.logger.info(f"🔍 Auto-inferred source_silvers: {source_silvers}")

        # Validate that all source_silvers exist
        invalid_silvers = [s for s in source_silvers if s not in self.silver_steps]
        if invalid_silvers:
            raise StepError(
                f"Silver steps not found: {invalid_silvers}",
                context={"step_name": name, "step_type": "gold"},
                suggestions=[
                    f"Available silver steps: {list(self.silver_steps.keys())}",
                    "Add the missing silver steps first using add_silver_transform()",
                ],
            )

        # Note: Dependency validation is deferred to validate_pipeline()
        # This allows for more flexible pipeline construction

        # Use builder's schema if not provided
        if schema is None:
            schema = self.config.schema
        else:
            self._validate_schema(schema)

        # Convert string rules to PySpark Column objects
        converted_rules = _convert_rules_to_expressions(rules, self.functions)

        # Create gold step
        gold_step = GoldStep(
            name=name,
            transform=transform,
            rules=converted_rules,
            table_name=table_name,
            source_silvers=source_silvers,
            schema=schema,
        )

        self.gold_steps[name] = gold_step
        self.logger.info(f"✅ Added Gold step: {name} (sources: {source_silvers})")

        return self

    def validate_pipeline(self) -> list[str]:
        """
        Validate the entire pipeline configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        validation_result = self.validator.validate_pipeline(
            self.config, self.bronze_steps, self.silver_steps, self.gold_steps
        )

        if validation_result.errors:
            self.logger.error(
                f"Pipeline validation failed with {len(validation_result.errors)} errors"
            )
            for error in validation_result.errors:
                self.logger.error(f"  - {error}")
        else:
            self.logger.info("✅ Pipeline validation passed")

        return validation_result.errors

    # ============================================================================
    # PRESET CONFIGURATIONS AND HELPER METHODS
    # ============================================================================

    @classmethod
    def for_development(
        cls,
        spark: SparkSession,
        schema: str,
        functions: FunctionsProtocol | None = None,
        **kwargs: Any,
    ) -> PipelineBuilder:
        """
        Create a PipelineBuilder optimized for development with relaxed validation.

        Args:
            spark: Active SparkSession instance
            schema: Database schema name
            **kwargs: Additional configuration parameters

        Returns:
            PipelineBuilder instance with development-optimized settings

        Example:
            >>> builder = PipelineBuilder.for_development(
            ...     spark=spark,
            ...     schema="dev_schema"
            ... )
        """
        return cls(
            spark=spark,
            schema=schema,
            min_bronze_rate=80.0,  # Relaxed validation
            min_silver_rate=85.0,
            min_gold_rate=90.0,
            verbose=True,
            functions=functions,
            **kwargs,
        )

    @classmethod
    def for_production(
        cls,
        spark: SparkSession,
        schema: str,
        functions: FunctionsProtocol | None = None,
        **kwargs: Any,
    ) -> PipelineBuilder:
        """
        Create a PipelineBuilder optimized for production with strict validation.

        Args:
            spark: Active SparkSession instance
            schema: Database schema name
            **kwargs: Additional configuration parameters

        Returns:
            PipelineBuilder instance with production-optimized settings

        Example:
            >>> builder = PipelineBuilder.for_production(
            ...     spark=spark,
            ...     schema="prod_schema"
            ... )
        """
        return cls(
            spark=spark,
            schema=schema,
            min_bronze_rate=95.0,  # Strict validation
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=False,
            functions=functions,
            **kwargs,
        )

    @classmethod
    def for_testing(
        cls,
        spark: SparkSession,
        schema: str,
        functions: FunctionsProtocol | None = None,
        **kwargs: Any,
    ) -> PipelineBuilder:
        """
        Create a PipelineBuilder optimized for testing with minimal validation.

        Args:
            spark: Active SparkSession instance
            schema: Database schema name
            **kwargs: Additional configuration parameters

        Returns:
            PipelineBuilder instance with testing-optimized settings

        Example:
            >>> builder = PipelineBuilder.for_testing(
            ...     spark=spark,
            ...     schema="my_schema"
            ... )
        """
        return cls(
            spark=spark,
            schema=schema,
            min_bronze_rate=70.0,  # Very relaxed validation
            min_silver_rate=75.0,
            min_gold_rate=80.0,
            verbose=True,
            functions=functions,
            **kwargs,
        )

    # ============================================================================
    # VALIDATION HELPER METHODS
    # ============================================================================

    @staticmethod
    def not_null_rules(
        columns: list[str], functions: FunctionsProtocol | None = None
    ) -> ColumnRules:
        """
        Create validation rules for non-null constraints on multiple columns.

        Args:
            columns: List of column names to validate for non-null
            functions: Optional functions object for column operations

        Returns:
            Dictionary of validation rules

        Example:
            >>> rules = PipelineBuilder.not_null_rules(["user_id", "timestamp", "value"])
            >>> # Equivalent to:
            >>> # {
            >>> #     "user_id": [F.col("user_id").isNotNull()],
            >>> #     "timestamp": [F.col("timestamp").isNotNull()],
            >>> #     "value": [F.col("value").isNotNull()]
            >>> # }
        """
        if functions is None:
            functions = get_default_functions()
        return {col: [functions.col(col).isNotNull()] for col in columns}

    @staticmethod
    def positive_number_rules(
        columns: list[str], functions: FunctionsProtocol | None = None
    ) -> ColumnRules:
        """
        Create validation rules for positive number constraints on multiple columns.

        Args:
            columns: List of column names to validate for positive numbers
            functions: Optional functions object for column operations

        Returns:
            Dictionary of validation rules

        Example:
            >>> rules = PipelineBuilder.positive_number_rules(["value", "count"])
            >>> # Equivalent to:
            >>> # {
            >>> #     "value": [F.col("value").isNotNull(), F.col("value") > 0],
            >>> #     "count": [F.col("count").isNotNull(), F.col("count") > 0]
            >>> # }
        """
        if functions is None:
            functions = get_default_functions()
        return {
            col: [functions.col(col).isNotNull(), functions.col(col) > 0]  # type: ignore[list-item]
            for col in columns
        }

    @staticmethod
    def string_not_empty_rules(
        columns: list[str], functions: FunctionsProtocol | None = None
    ) -> ColumnRules:
        """
        Create validation rules for non-empty string constraints on multiple columns.

        Args:
            columns: List of column names to validate for non-empty strings
            functions: Optional functions object for column operations

        Returns:
            Dictionary of validation rules

        Example:
            >>> rules = PipelineBuilder.string_not_empty_rules(["name", "category"])
            >>> # Equivalent to:
            >>> # {
            >>> #     "name": [F.col("name").isNotNull(), F.length(F.col("name")) > 0],
            >>> #     "category": [F.col("category").isNotNull(), F.length(F.col("category")) > 0]
            >>> # }
        """
        if functions is None:
            functions = get_default_functions()
        return {
            col: [
                functions.col(col).isNotNull(),
                functions.length(functions.col(col)) > 0,  # type: ignore[list-item]
            ]
            for col in columns
        }

    @staticmethod
    def timestamp_rules(
        columns: list[str], functions: FunctionsProtocol | None = None
    ) -> ColumnRules:
        """
        Create validation rules for timestamp constraints on multiple columns.

        Args:
            columns: List of column names to validate as timestamps
            functions: Optional functions object for column operations

        Returns:
            Dictionary of validation rules

        Example:
            >>> rules = PipelineBuilder.timestamp_rules(["created_at", "updated_at"])
            >>> # Equivalent to:
            >>> # {
            >>> #     "created_at": [F.col("created_at").isNotNull(), F.col("created_at").isNotNull()],
            >>> #     "updated_at": [F.col("updated_at").isNotNull(), F.col("updated_at").isNotNull()]
            >>> # }
        """
        if functions is None:
            functions = get_default_functions()
        return {
            col: [functions.col(col).isNotNull(), functions.col(col).isNotNull()]
            for col in columns
        }

    @staticmethod
    def detect_timestamp_columns(df_schema: Any) -> list[str]:
        """
        Detect timestamp columns from a DataFrame schema.

        Args:
            df_schema: DataFrame schema or list of column names with types

        Returns:
            List of column names that appear to be timestamps

        Example:
            >>> timestamp_cols = PipelineBuilder.detect_timestamp_columns(df.schema)
            >>> # Returns columns like ["timestamp", "created_at", "updated_at"]
        """
        timestamp_keywords = [
            "timestamp",
            "created_at",
            "updated_at",
            "event_time",
            "process_time",
            "ingestion_time",
            "load_time",
            "modified_at",
            "date_time",
            "ts",
        ]

        if hasattr(df_schema, "fields"):
            # DataFrame schema
            columns = [field.name.lower() for field in df_schema.fields]
        else:
            # List of column names
            columns = [col.lower() for col in df_schema]

        # Find columns that match timestamp patterns
        timestamp_cols = []
        for col in columns:
            if any(keyword in col for keyword in timestamp_keywords):
                timestamp_cols.append(col)

        return timestamp_cols

    def _validate_schema(self, schema: str) -> None:
        """
        Validate that a schema exists and is accessible.

        Args:
            schema: Schema name to validate

        Raises:
            StepError: If schema doesn't exist or is not accessible
        """
        try:
            # Check if schema exists using catalog API
            databases = [db.name for db in self.spark.catalog.listDatabases()]
            if schema not in databases:
                raise StepError(
                    f"Schema '{schema}' does not exist",
                    context={
                        "step_name": "schema_validation",
                        "step_type": "validation",
                    },
                    suggestions=[
                        f"Create the schema first: CREATE SCHEMA IF NOT EXISTS {schema}",
                        "Check schema permissions",
                        "Verify schema name spelling",
                    ],
                )
            self.logger.debug(f"✅ Schema '{schema}' is accessible")
        except StepError:
            # Re-raise StepError as-is
            raise
        except Exception as e:
            raise StepError(
                f"Schema '{schema}' is not accessible: {str(e)}",
                context={"step_name": "schema_validation", "step_type": "validation"},
                suggestions=[
                    f"Create the schema first: CREATE SCHEMA IF NOT EXISTS {schema}",
                    "Check schema permissions",
                    "Verify schema name spelling",
                ],
            ) from e

    def _create_schema_if_not_exists(self, schema: str) -> None:
        """
        Create a schema if it doesn't exist.

        Args:
            schema: Schema name to create
        """
        try:
            # Use SQL to create schema
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            self.logger.info(f"✅ Schema '{schema}' created or already exists")
        except Exception as e:
            raise StepError(
                f"Failed to create schema '{schema}': {str(e)}",
                context={"step_name": "schema_creation", "step_type": "validation"},
                suggestions=[
                    "Check schema permissions",
                    "Verify schema name is valid",
                    "Check for naming conflicts",
                ],
            ) from e

    def _get_effective_schema(self, step_schema: str | None) -> str:
        """
        Get the effective schema for a step, falling back to the builder's default schema.

        Args:
            step_schema: Schema specified for the step

        Returns:
            The effective schema name
        """
        return step_schema if step_schema is not None else self.schema

    def to_pipeline(self) -> PipelineRunner:
        """
        Build and return a PipelineRunner for executing this pipeline.

        Returns:
            PipelineRunner instance ready for execution

        Raises:
            ValueError: If pipeline validation fails
        """
        # Validate pipeline before building
        validation_errors = self.validate_pipeline()
        if validation_errors:
            raise ValueError(
                f"Pipeline validation failed with {len(validation_errors)} errors: {', '.join(validation_errors)}"
            )

        # Create pipeline runner
        runner = PipelineRunner(
            spark=self.spark,
            config=self.config,
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
            logger=self.logger,
            functions=self.functions,
        )

        self.logger.info(
            f"🚀 Pipeline built successfully with {len(self.bronze_steps)} bronze, {len(self.silver_steps)} silver, {len(self.gold_steps)} gold steps"
        )

        return runner
