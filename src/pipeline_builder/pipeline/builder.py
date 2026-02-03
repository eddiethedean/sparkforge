"""PipelineBuilder for constructing data pipelines.

This module provides a clean, maintainable PipelineBuilder that handles
pipeline construction with the Medallion Architecture (Bronze → Silver → Gold).
The builder creates pipelines that can be executed with the execution engine
using a service-oriented architecture.

Key Features:
    - Fluent API for intuitive pipeline construction
    - Automatic dependency management
    - String-based validation rules
    - Multi-schema support
    - Comprehensive validation and error handling

The builder uses a service-oriented architecture internally:
    - StepFactory: Creates step instances
    - ExecutionValidator: Validates pipeline configuration
    - Step Executors: Execute steps during pipeline run

Example:
    >>> from pipeline_builder.pipeline.builder import PipelineBuilder
    >>> from pipeline_builder.functions import get_default_functions
    >>> F = get_default_functions()
    >>>
    >>> builder = PipelineBuilder(spark=spark, schema="analytics")
    >>> builder.with_bronze_rules(
    ...     name="events",
    ...     rules={"user_id": ["not_null"], "timestamp": ["not_null"]},
    ...     incremental_col="timestamp"
    ... )
    >>> builder.add_silver_transform(
    ...     name="clean_events",
    ...     source_bronze="events",
    ...     transform=lambda spark, df, silvers: df.filter(F.col("value") > 0),
    ...     rules={"value": ["gt", 0]},
    ...     table_name="clean_events"
    ... )
    >>> pipeline = builder.to_pipeline()
    >>> result = pipeline.run_initial_load(bronze_sources={"events": source_df})

Note:
    This module depends on:
    - compat: Spark compatibility layer
    - errors: Error handling
    - functions: PySpark function protocols
    - logging: Pipeline logging
    - models: Pipeline and step models
    - pipeline.runner: Pipeline execution
    - types: Type definitions
    - validation: Data and pipeline validation
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

# Engine-specific StructType should satisfy the TypesProtocol.StructType
from abstracts.builder import PipelineBuilder as AbstractsPipelineBuilder
from pipeline_builder_base.builder import BasePipelineBuilder
from pipeline_builder_base.errors import (
    ConfigurationError as PipelineConfigurationError,
)
from pipeline_builder_base.errors import (
    ExecutionError as StepError,
)
from pipeline_builder_base.errors import (
    ValidationError,
)
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    PipelineConfig,
    ValidationThresholds,
)

from ..compat import SparkSession
from ..engine import SparkEngine
from ..functions import FunctionsProtocol, get_default_functions
from ..models import (
    BronzeStep,
    GoldStep,
    SilverStep,
)
from ..table_operations import fqn, table_exists
from ..types import (
    ColumnRules,
    GoldTransformFunction,
    SilverTransformFunction,
    StepName,
    TableName,
)
from ..validation import ValidationResult, _convert_rules_to_expressions
from .runner import PipelineRunner


class PipelineBuilder(BasePipelineBuilder):
    """Production-ready builder for creating data pipelines.

    The PipelineBuilder provides a fluent API for constructing robust data
    pipelines with comprehensive validation, automatic dependency management,
    and enterprise-grade features. Uses the Medallion Architecture
    (Bronze → Silver → Gold) for data layering.

    Key Features:
        - Fluent API: Chain methods for intuitive pipeline construction
        - Robust Validation: Early error detection with clear validation messages
        - Auto-inference: Automatic dependency detection and validation
        - String Rules: Convert human-readable rules to PySpark expressions
        - Multi-schema Support: Cross-schema data flows for enterprise environments
        - Comprehensive Error Handling: Detailed error messages with suggestions
        - Service-Oriented Architecture: Clean separation of concerns internally

    Validation Requirements:
        All pipeline steps must have validation rules. Invalid configurations
        are rejected during construction with clear error messages.

    String Rules Support:
        You can use human-readable string rules that are automatically converted
        to PySpark expressions:

        - "not_null" → F.col("column").isNotNull()
        - "gt", value → F.col("column") > value
        - "lt", value → F.col("column") < value
        - "eq", value → F.col("column") == value
        - "in", [values] → F.col("column").isin(values)
        - "between", min, max → F.col("column").between(min, max)

    Attributes:
        spark: SparkSession instance for DataFrame operations.
        schema: Target schema name for pipeline tables.
        config: PipelineConfig instance with pipeline configuration.
        logger: PipelineLogger instance for logging.
        functions: FunctionsProtocol instance for PySpark operations.
        bronze_steps: Dictionary of BronzeStep instances.
        silver_steps: Dictionary of SilverStep instances.
        gold_steps: Dictionary of GoldStep instances.
        execution_order: List of step names in execution order (topological sort).
            Populated automatically after successful pipeline validation.
            None if validation hasn't been run or failed.

    Example:
        Basic pipeline construction:

        >>> from pipeline_builder.pipeline.builder import PipelineBuilder
        >>> from pipeline_builder.functions import get_default_functions
        >>> F = get_default_functions()
        >>>
        >>> builder = PipelineBuilder(spark=spark, schema="analytics")
        >>> builder.with_bronze_rules(
        ...     name="events",
        ...     rules={"user_id": ["not_null"], "timestamp": ["not_null"]},
        ...     incremental_col="timestamp"
        ... )
        >>> builder.add_silver_transform(
        ...     name="clean_events",
        ...     source_bronze="events",
        ...     transform=lambda spark, df, silvers: df.filter(F.col("value") > 0),
        ...     rules={"value": ["gt", 0]},
        ...     table_name="clean_events"
        ... )
        >>> builder.add_gold_transform(
        ...     name="daily_metrics",
        ...     transform=lambda spark, silvers: silvers["clean_events"]
        ...     .groupBy("date")
        ...     .agg(F.count("*").alias("count")),
        ...     rules={"count": ["gt", 0]},
        ...     table_name="daily_metrics",
        ...     source_silvers=["clean_events"]
        ... )
        >>> pipeline = builder.to_pipeline()
        >>> result = pipeline.run_initial_load(bronze_sources={"events": source_df})

    Raises:
        ValidationError: If validation rules are invalid or missing.
        ConfigurationError: If configuration parameters are invalid.
        StepError: If step dependencies cannot be resolved.
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
        functions: Optional[FunctionsProtocol] = None,
    ) -> None:
        """Initialize a new PipelineBuilder instance.

        Creates a PipelineBuilder with the specified configuration. Initializes
        all required services including validators, step storage, and execution
        engine.

        Args:
            spark: Active SparkSession instance for data processing.
            schema: Database schema name where tables will be created.
            min_bronze_rate: Minimum data quality rate for Bronze layer (0-100).
                Defaults to 95.0.
            min_silver_rate: Minimum data quality rate for Silver layer (0-100).
                Defaults to 98.0.
            min_gold_rate: Minimum data quality rate for Gold layer (0-100).
                Defaults to 99.0.
            verbose: Enable verbose logging output. Defaults to True.
            functions: Optional FunctionsProtocol instance for PySpark operations.
                If None, uses get_default_functions().

        Raises:
            PipelineConfigurationError: If Spark session is None, schema is empty,
                or quality rates are invalid.

        Note:
            The builder initializes:
            - PipelineConfig with validation thresholds
            - UnifiedValidator for Spark-specific validation
            - SparkEngine for execution
            - Step storage dictionaries (bronze_steps, silver_steps, gold_steps)
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
        config = PipelineConfig(
            schema=schema,
            thresholds=thresholds,
            verbose=verbose,
        )

        # Initialize base class (this sets up self.config, self.logger, self.validator, self.step_validator)
        super().__init__(config, logger=PipelineLogger(verbose=verbose))

        # Initialize Spark-specific components
        self.spark = spark
        self.functions = functions if functions is not None else get_default_functions()

        # Expose schema for backward compatibility
        self.schema = schema
        self.pipeline_id = (
            f"pipeline_{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        # Use Spark-specific validator (UnifiedValidator) in addition to base validator
        # Keep the existing UnifiedValidator for Spark-specific validation
        from ..validation import UnifiedValidator

        self.spark_validator = UnifiedValidator(self.logger)
        # Store base validator before overriding
        # Type annotation needed for mypy - validator is set in BasePipelineBuilder.__init__
        from typing import cast

        from pipeline_builder_base.validation import PipelineValidator

        # Type cast: BasePipelineBuilder.__init__ creates PipelineValidator instance
        # Runtime check: We verify this is actually a PipelineValidator via isinstance
        # This cast is safe because BasePipelineBuilder always creates PipelineValidator
        # NOTE: We use runtime type checks in validate_pipeline() to catch any mismatches
        validator: PipelineValidator = cast(PipelineValidator, self.validator)  # type: ignore[redundant-cast]
        self._base_validator: PipelineValidator = validator

        # Execution order will be populated after validation
        self.execution_order: Optional[List[str]] = None

        # Track step creation order for deterministic ordering when no explicit dependencies
        self._step_creation_order: Dict[str, int] = {}
        self._creation_counter: int = 0

        # Override base validator with spark_validator for backward compatibility
        # This allows add_validator() to work on self.validator
        # Type cast: UnifiedValidator implements the PipelineValidator interface
        # but has different return types. We cast for interface compatibility.
        # NOTE: Runtime type checks in validate_pipeline() handle the return type differences
        from pipeline_builder_base.validation import (
            PipelineValidator as BasePipelineValidator,
        )

        self.validator = cast(BasePipelineValidator, self.spark_validator)
        # Expose validators for backward compatibility
        self.validators = self.spark_validator.custom_validators

        # Step storage is already initialized by BasePipelineBuilder
        # but we need to type them correctly for Spark steps
        self.bronze_steps: Dict[str, BronzeStep] = {}
        self.silver_steps: Dict[str, SilverStep] = {}
        self.gold_steps: Dict[str, GoldStep] = {}

        # Create SparkEngine for abstracts layer
        self.spark_engine = SparkEngine(
            spark=self.spark,
            config=self.config,
            logger=self.logger,
            functions=self.functions,
        )

        # Create abstracts.PipelineBuilder with SparkEngine injection
        # We'll use PipelineRunner as the runner class
        self._abstracts_builder = AbstractsPipelineBuilder(
            runner_cls=PipelineRunner,
            engine=self.spark_engine,
        )

        self.logger.info(f"🔧 PipelineBuilder initialized (schema: {schema})")

    def with_bronze_rules(
        self,
        *,
        name: StepName,
        rules: ColumnRules,
        incremental_col: Optional[str] = None,
        description: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> PipelineBuilder:
        """Add Bronze layer validation rules for raw data ingestion.

        Bronze steps represent the first layer of the Medallion Architecture,
        handling raw data ingestion and initial validation. All Bronze steps
        must have non-empty validation rules.

        Args:
            name: Unique identifier for this Bronze step.
            rules: Dictionary mapping column names to validation rule lists.
                Supports both PySpark Column expressions and string rules:
                - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                - String: {"user_id": ["not_null"], "age": ["gt", 0]}
            incremental_col: Optional column name for incremental processing
                (e.g., "timestamp", "updated_at"). If provided, enables
                incremental processing with append mode.
            description: Optional description of this Bronze step.
            schema: Optional schema name for reading bronze data. If not
                provided, uses the builder's default schema.

        Returns:
            Self for method chaining.

        Raises:
            StepError: If step name is empty, conflicts with existing step,
                or schema validation fails.
            ValidationError: If rules are empty or invalid.

        Example:
            Using PySpark Column expressions:

            >>> builder.with_bronze_rules(
            ...     name="events",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     incremental_col="timestamp"
            ... )

            Using string rules (automatically converted):

            >>> builder.with_bronze_rules(
            ...     name="users",
            ...     rules={"user_id": ["not_null"], "age": ["gt", 0]},
            ...     incremental_col="updated_at"
            ... )

            Cross-schema bronze data:

            >>> builder.with_bronze_rules(
            ...     name="user_events",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     incremental_col="timestamp",
            ...     schema="raw_data"  # Read from different schema
            ... )

        Note:
            String rules are automatically converted to PySpark expressions:
            - "not_null" → F.col("column").isNotNull()
            - "gt", value → F.col("column") > value
            - "lt", value → F.col("column") < value
            - "eq", value → F.col("column") == value
            - "in", [values] → F.col("column").isin(values)
            - "between", min, max → F.col("column").between(min, max)
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

        # Use base class method for duplicate checking
        try:
            self._check_duplicate_step_name(name, "bronze")
        except Exception as e:
            # Convert to StepError for consistency
            raise StepError(
                str(e),
                context={"step_name": name, "step_type": "bronze"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            ) from e

        # Validate schema if provided (use base class method)
        if schema is not None:
            try:
                self._validate_schema(schema)
            except Exception as e:
                # Convert to StepError for consistency
                raise StepError(
                    str(e),
                    context={
                        "step_name": name,
                        "step_type": "bronze",
                        "schema": schema,
                    },
                ) from e

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
        # Track creation order for deterministic ordering
        self._step_creation_order[name] = self._creation_counter
        self._creation_counter += 1
        self.logger.info(f"✅ Added Bronze step: {name}")

        return self

    def with_silver_rules(
        self,
        *,
        name: StepName,
        table_name: TableName,
        rules: ColumnRules,
        description: Optional[str] = None,
        schema: Optional[str] = None,
        optional: bool = False,
    ) -> PipelineBuilder:
        """Add Silver layer validation rules for existing silver tables.

        Silver steps created with this method represent validation-only steps
        for existing silver tables. They allow subsequent transform functions
        to access validated existing silver and gold tables via `prior_silvers`
        and `prior_golds` arguments.

        Args:
            name: Unique identifier for this Silver step.
            table_name: Existing Delta table name (without schema).
            rules: Dictionary mapping column names to validation rule lists.
                Supports both PySpark Column expressions and string rules:
                - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                - String: {"user_id": ["not_null"], "age": ["gt", 0]}
            description: Optional description of this Silver step.
            schema: Optional schema name for reading silver data. If not
                provided, uses the builder's default schema.
            optional: If True, step does not fail when the table does not exist;
                an empty DataFrame is returned so downstream steps can run.

        Returns:
            Self for method chaining.

        Raises:
            StepError: If step name is empty, conflicts with existing step,
                or schema validation fails.
            ValidationError: If rules are empty or invalid.

        Example:
            Using PySpark Column expressions:

            >>> builder.with_silver_rules(
            ...     name="existing_clean_events",
            ...     table_name="clean_events",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]}
            ... )

            Using string rules (automatically converted):

            >>> builder.with_silver_rules(
            ...     name="validated_events",
            ...     table_name="events",
            ...     rules={"user_id": ["not_null"], "value": ["gt", 0]},
            ...     schema="staging"
            ... )

        Note:
            String rules are automatically converted to PySpark expressions.
            See with_bronze_rules() for supported string rule formats.
            This creates a validation-only step that can be accessed by
            subsequent transform functions via prior_silvers.
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

        # Use base class method for duplicate checking
        try:
            self._check_duplicate_step_name(name, "silver")
        except Exception as e:
            # Convert to StepError for consistency
            raise StepError(
                str(e),
                context={"step_name": name, "step_type": "silver"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            ) from e

        # Validate schema if provided (use base class method)
        if schema is not None:
            try:
                self._validate_schema(schema)
            except Exception as e:
                # Convert to StepError for consistency
                raise StepError(
                    str(e),
                    context={
                        "step_name": name,
                        "step_type": "silver",
                        "schema": schema,
                    },
                ) from e

        # Convert string rules to PySpark Column objects
        converted_rules = _convert_rules_to_expressions(rules, self.functions)

        # Get effective schema (use builder's default if not provided)
        effective_schema = self._get_effective_schema(schema)

        # Create SilverStep for validation-only (no transform function)
        silver_step = SilverStep(
            name=name,
            source_bronze="",  # No source bronze for existing tables
            transform=None,  # No transform function for validation-only steps
            rules=converted_rules,
            table_name=table_name,
            watermark_col=None,  # No watermark needed for validation-only steps
            existing=True,
            optional=optional,
            schema=effective_schema,
            source_incremental_col=None,
        )

        self.silver_steps[name] = silver_step
        # Track creation order for deterministic ordering
        self._step_creation_order[name] = self._creation_counter
        self._creation_counter += 1
        self.logger.info(f"✅ Added Silver step (validation-only): {name}")

        return self

    def with_gold_rules(
        self,
        *,
        name: StepName,
        table_name: TableName,
        rules: ColumnRules,
        description: Optional[str] = None,
        schema: Optional[str] = None,
        optional: bool = False,
    ) -> PipelineBuilder:
        """Add Gold layer validation rules for existing gold tables.

        Gold steps created with this method represent validation-only steps
        for existing gold tables. They allow subsequent transform functions
        to access validated existing silver and gold tables via `prior_silvers`
        and `prior_golds` arguments.

        Args:
            name: Unique identifier for this Gold step.
            table_name: Existing Delta table name (without schema).
            rules: Dictionary mapping column names to validation rule lists.
                Supports both PySpark Column expressions and string rules:
                - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                - String: {"user_id": ["not_null"], "count": ["gt", 0]}
            description: Optional description of this Gold step.
            schema: Optional schema name for reading gold data. If not
                provided, uses the builder's default schema.
            optional: If True, step does not fail when the table does not exist;
                an empty DataFrame is returned so downstream steps can run.

        Returns:
            Self for method chaining.

        Raises:
            StepError: If step name is empty, conflicts with existing step,
                or schema validation fails.
            ValidationError: If rules are empty or invalid.

        Example:
            Using PySpark Column expressions:

            >>> builder.with_gold_rules(
            ...     name="existing_user_metrics",
            ...     table_name="user_metrics",
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ... )

            Using string rules (automatically converted):

            >>> builder.with_gold_rules(
            ...     name="validated_metrics",
            ...     table_name="metrics",
            ...     rules={"user_id": ["not_null"], "count": ["gt", 0]},
            ...     schema="analytics"
            ... )

        Note:
            String rules are automatically converted to PySpark expressions.
            See with_bronze_rules() for supported string rule formats.
            This creates a validation-only step that can be accessed by
            subsequent transform functions via prior_golds.
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

        # Use base class method for duplicate checking
        try:
            self._check_duplicate_step_name(name, "gold")
        except Exception as e:
            # Convert to StepError for consistency
            raise StepError(
                str(e),
                context={"step_name": name, "step_type": "gold"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            ) from e

        # Validate schema if provided (use base class method)
        if schema is not None:
            try:
                self._validate_schema(schema)
            except Exception as e:
                # Convert to StepError for consistency
                raise StepError(
                    str(e),
                    context={
                        "step_name": name,
                        "step_type": "gold",
                        "schema": schema,
                    },
                ) from e

        # Convert string rules to PySpark Column objects
        converted_rules = _convert_rules_to_expressions(rules, self.functions)

        # Get effective schema (use builder's default if not provided)
        effective_schema = self._get_effective_schema(schema)

        # Create GoldStep for validation-only (no transform function)
        gold_step = GoldStep(
            name=name,
            transform=None,  # No transform function for validation-only steps
            rules=converted_rules,
            table_name=table_name,
            existing=True,
            optional=optional,
            schema=effective_schema,
            source_silvers=None,  # No source silvers for existing tables
        )

        self.gold_steps[name] = gold_step
        # Track creation order for deterministic ordering
        self._step_creation_order[name] = self._creation_counter
        self._creation_counter += 1
        self.logger.info(f"✅ Added Gold step (validation-only): {name}")

        return self

    def add_validator(self, validator: Any) -> PipelineBuilder:
        """Add a custom step validator to the pipeline.

        Custom validators allow you to add additional validation logic beyond
        the built-in validation rules. Validators are called during pipeline
        validation to check step configurations.

        Args:
            validator: Custom validator implementing StepValidator protocol.
                Must have a validate() method that accepts step and context
                parameters.

        Returns:
            Self for method chaining.

        Example:
            >>> class CustomValidator(StepValidator):
            ...     def validate(self, step, context):
            ...         if step.name == "special_step":
            ...             return ["Special validation failed"]
            ...         return []
            >>>
            >>> builder.add_validator(CustomValidator())

        Note:
            Custom validators are added to the UnifiedValidator and called
            during validate_pipeline(). They can return ValidationResult or
            List[str] of error messages.
        """
        self.spark_validator.add_validator(validator)
        return self

    def add_silver_transform(
        self,
        *,
        name: StepName,
        source_bronze: Optional[StepName] = None,
        transform: SilverTransformFunction,
        rules: ColumnRules,
        table_name: TableName,
        watermark_col: Optional[str] = None,
        description: Optional[str] = None,
        depends_on: Optional[list[StepName]] = None,
        source_silvers: Optional[list[StepName]] = None,
        schema: Optional[str] = None,
        schema_override: Optional[Any] = None,
    ) -> PipelineBuilder:
        """Add Silver layer transformation step for data cleaning and enrichment.

        Silver steps represent the second layer of the Medallion Architecture,
        transforming raw Bronze data into clean, business-ready datasets. All
        Silver steps must have non-empty validation rules and a valid transform
        function.

        Args:
            name: Unique identifier for this Silver step.
            source_bronze: Optional name of the Bronze step this Silver step
                depends on. If not provided, automatically infers from the most
                recent with_bronze_rules() call. If no bronze steps exist,
                raises an error.
            transform: Transformation function with signature:
                (spark: SparkSession, bronze_df: DataFrame,
                prior_silvers: Dict[str, DataFrame]) -> DataFrame
                Must be callable and cannot be None.
            rules: Dictionary mapping column names to validation rule lists.
                Supports both PySpark Column expressions and string rules:
                - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                - String: {"user_id": ["not_null"], "age": ["gt", 0]}
            table_name: Target Delta table name where results will be stored
                (without schema).
            watermark_col: Optional column name for watermarking (e.g.,
                "timestamp", "updated_at"). If provided, enables incremental
                processing with append mode.
            description: Optional description of this Silver step.
            depends_on: Optional list of other Silver step names that must
                complete before this step. Deprecated - use source_silvers instead.
            source_silvers: Optional list of Silver step names this Silver step
                depends on. These steps will be available in the prior_silvers
                dictionary passed to the transform function. If provided, ensures
                correct execution order.
            schema: Optional schema name for writing silver data. If not
                provided, uses the builder's default schema.
            schema_override: Optional PySpark StructType schema to override
                DataFrame schema when creating tables. Uses Delta Lake's
                overwriteSchema option. Applied during initial runs and when
                table doesn't exist.

        Returns:
            Self for method chaining.

        Raises:
            StepError: If step name is empty, conflicts with existing step,
                source_bronze not found, or schema validation fails.
            ValidationError: If rules are empty, transform is None, or
                configuration is invalid.

        Example:
            Using PySpark Column expressions:

            >>> def clean_user_events(spark, bronze_df, prior_silvers):
            ...     return (bronze_df
            ...         .filter(F.col("user_id").isNotNull())
            ...         .withColumn("event_date", F.date_trunc("day", "timestamp"))
            ...     )
            >>>
            >>> builder.add_silver_transform(
            ...     name="clean_events",
            ...     source_bronze="user_events",
            ...     transform=clean_user_events,
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     table_name="clean_events"
            ... )

            Using string rules with auto-inferred source:

            >>> builder.add_silver_transform(
            ...     name="enriched_events",
            ...     transform=lambda spark, df, silvers: df.withColumn(
            ...         "processed_at", F.current_timestamp()
            ...     ),
            ...     rules={"user_id": ["not_null"], "processed_at": ["not_null"]},
            ...     table_name="enriched_events",
            ...     watermark_col="processed_at"
            ... )

        Note:
            String rules are automatically converted to PySpark expressions.
            See with_bronze_rules() for supported string rule formats.
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

        # Use base class method for duplicate checking
        try:
            self._check_duplicate_step_name(name, "silver")
        except Exception as e:
            # Convert to StepError for consistency
            raise StepError(
                str(e),
                context={"step_name": name, "step_type": "silver"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            ) from e

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

        # Capture the incremental column from the source bronze step (if any)
        source_incremental_col = self.bronze_steps[source_bronze].incremental_col

        # Use source_silvers if provided, otherwise fall back to depends_on for backward compatibility
        final_source_silvers = (
            source_silvers if source_silvers is not None else depends_on
        )

        # Create silver step
        silver_step = SilverStep(
            name=name,
            source_bronze=source_bronze,
            transform=transform,
            rules=converted_rules,
            table_name=table_name,
            watermark_col=watermark_col,
            schema=schema,
            source_incremental_col=source_incremental_col,
            schema_override=schema_override,
            source_silvers=final_source_silvers,
        )

        self.silver_steps[name] = silver_step
        # Track creation order for deterministic ordering
        self._step_creation_order[name] = self._creation_counter
        self._creation_counter += 1
        self.logger.info(f"✅ Added Silver step: {name} (source: {source_bronze})")

        return self

    def add_gold_transform(
        self,
        *,
        name: StepName,
        transform: GoldTransformFunction,
        rules: ColumnRules,
        table_name: TableName,
        source_silvers: Optional[list[StepName]] = None,
        description: Optional[str] = None,
        schema: Optional[str] = None,
        schema_override: Optional[Any] = None,
    ) -> PipelineBuilder:
        """Add Gold layer transformation step for business analytics and aggregations.

        Gold steps represent the third layer of the Medallion Architecture,
        creating business-ready datasets for analytics and reporting. All Gold
        steps must have non-empty validation rules and a valid transform function.

        Args:
            name: Unique identifier for this Gold step.
            transform: Transformation function with signature:
                (spark: SparkSession, silvers: Dict[str, DataFrame]) -> DataFrame
                Must be callable and cannot be None.
            rules: Dictionary mapping column names to validation rule lists.
                Supports both PySpark Column expressions and string rules:
                - PySpark: {"user_id": [F.col("user_id").isNotNull()]}
                - String: {"user_id": ["not_null"], "count": ["gt", 0]}
            table_name: Target Delta table name where results will be stored
                (without schema).
            source_silvers: Optional list of Silver step names this Gold step
                depends on. If not provided, automatically uses all available
                Silver steps. If no Silver steps exist, raises an error.
            description: Optional description of this Gold step.
            schema: Optional schema name for writing gold data. If not provided,
                uses the builder's default schema.
            schema_override: Optional PySpark StructType schema to override
                DataFrame schema when writing to gold tables. Uses Delta Lake's
                overwriteSchema option. Always applied for gold table writes.

        Returns:
            Self for method chaining.

        Raises:
            StepError: If step name is empty, conflicts with existing step,
                source_silvers not found, or schema validation fails.
            ValidationError: If rules are empty, transform is None, or
                configuration is invalid.

        Example:
            Using PySpark Column expressions:

            >>> def user_daily_metrics(spark, silvers):
            ...     events_df = silvers["clean_events"]
            ...     return (events_df
            ...         .groupBy("user_id", "event_date")
            ...         .agg(F.count("*").alias("event_count"))
            ...     )
            >>>
            >>> builder.add_gold_transform(
            ...     name="user_metrics",
            ...     transform=user_daily_metrics,
            ...     rules={"user_id": [F.col("user_id").isNotNull()]},
            ...     table_name="user_daily_metrics",
            ...     source_silvers=["clean_events"]
            ... )

            Using string rules with auto-inferred sources:

            >>> builder.add_gold_transform(
            ...     name="daily_analytics",
            ...     transform=lambda spark, silvers: (
            ...         silvers["clean_events"]
            ...         .groupBy("date")
            ...         .agg(F.count("*").alias("count"))
            ...     ),
            ...     rules={"date": ["not_null"], "count": ["gt", 0]},
            ...     table_name="daily_analytics"
            ...     # source_silvers auto-inferred from all silver steps
            ... )

        Note:
            String rules are automatically converted to PySpark expressions.
            See with_bronze_rules() for supported string rule formats.
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

        # Use base class method for duplicate checking
        try:
            self._check_duplicate_step_name(name, "gold")
        except Exception as e:
            # Convert to StepError for consistency
            raise StepError(
                str(e),
                context={"step_name": name, "step_type": "gold"},
                suggestions=[
                    "Use a different step name",
                    "Remove the existing step first",
                ],
            ) from e

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
            schema_override=schema_override,
        )

        self.gold_steps[name] = gold_step
        # Track creation order for deterministic ordering
        self._step_creation_order[name] = self._creation_counter
        self._creation_counter += 1
        self.logger.info(f"✅ Added Gold step: {name} (sources: {source_silvers})")

        return self

    @staticmethod
    def _extract_errors_from_validator_result(
        result: Union[ValidationResult, List[str]],
        validator_name: str,
        logger: PipelineLogger,
    ) -> List[str]:
        """
        Type guard function to safely extract errors from validator results.

        This function handles both ValidationResult and List[str] return types,
        providing runtime type safety and clear error messages. This prevents
        type mismatch bugs like the ValidationResult + List[str] concatenation error.

        Different validators return different types:
        - PipelineValidator.validate_pipeline() returns List[str]
        - UnifiedValidator.validate_pipeline() returns ValidationResult

        This function normalizes both to List[str] for safe concatenation.

        Args:
            result: Result from validator (ValidationResult or List[str])
            validator_name: Name of the validator for error messages
            logger: Logger instance for warnings/errors

        Returns:
            List of validation error strings (guaranteed to be List[str])

        Raises:
            TypeError: If result is neither ValidationResult nor List[str],
                      or if List contains non-string items

        Example:
            >>> base_result = validator.validate_pipeline(...)
            >>> errors = PipelineBuilder._extract_errors_from_validator_result(
            ...     base_result, "base_validator", logger
            ... )
            >>> # errors is guaranteed to be List[str]
        """
        if isinstance(result, ValidationResult):
            # UnifiedValidator returns ValidationResult
            return result.errors
        elif isinstance(result, list):
            # PipelineValidator returns List[str]
            # Verify all items are strings
            if not all(isinstance(item, str) for item in result):
                error_msg = (
                    f"Validator {validator_name} returned List with non-string items. "
                    f"Expected List[str]. Got: {result}"
                )
                logger.error(error_msg)
                raise TypeError(error_msg)
            return result
        else:
            # Unexpected type - this is reachable at runtime even though
            # the type hint suggests it shouldn't be (defensive programming)
            error_msg = (
                f"Unexpected return type from {validator_name}: {type(result)}. "
                f"Expected ValidationResult or List[str]. Got: {result}"
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

    def validate_pipeline(self) -> List[str]:
        """Validate the entire pipeline configuration.

        Runs both base validator (PipelineValidator) and spark validator
        (UnifiedValidator), then combines their errors. Runtime type checks
        ensure that return types match expectations, preventing type mismatch bugs.

        Args:
            None (uses instance state).

        Returns:
            List of validation error strings (empty if valid). Each string
            describes a validation issue found in the pipeline configuration.

        Raises:
            TypeError: If validators return unexpected types (caught by runtime
                checks).

        Note:
            Return types from validators:
            - PipelineValidator.validate_pipeline() returns List[str]
            - UnifiedValidator.validate_pipeline() returns ValidationResult

            Both are normalized to List[str] using type guard functions before
            concatenation. Errors are logged to the logger.
        """
        # Use base class validation first (from BasePipelineBuilder)
        # PipelineValidator.validate_pipeline() returns List[str]
        base_result = self._base_validator.validate_pipeline(
            self.config, self.bronze_steps, self.silver_steps, self.gold_steps
        )

        # Extract errors using type guard function for runtime safety
        base_errors = self._extract_errors_from_validator_result(
            base_result, "base_validator", self.logger
        )

        # Also run Spark-specific validation
        # UnifiedValidator.validate_pipeline() returns ValidationResult
        spark_result = self.spark_validator.validate_pipeline(
            self.config, self.bronze_steps, self.silver_steps, self.gold_steps
        )

        # Extract errors using type guard function for runtime safety
        spark_errors = self._extract_errors_from_validator_result(
            spark_result, "spark_validator", self.logger
        )

        # Combine errors - both are now guaranteed to be lists
        all_errors = base_errors + spark_errors

        if all_errors:
            self.logger.error(
                f"Pipeline validation failed with {len(all_errors)} errors"
            )
            for error in all_errors:
                self.logger.error(f"  - {error}")
            # Clear execution order if validation fails
            self.execution_order = None
        else:
            self.logger.info("✅ Pipeline validation passed")
            # Calculate execution order after successful validation
            self._calculate_execution_order()

        return all_errors

    def _calculate_execution_order(self) -> None:
        """Calculate and store execution order based on step dependencies.

        Uses DependencyAnalyzer to determine the topological sort order of steps.
        This is called automatically after successful pipeline validation.

        Note:
            Execution order is stored in self.execution_order attribute.
            If dependency analysis fails, execution_order is set to None.
            Creation order is used as a tie-breaker for deterministic ordering
            when steps have no explicit dependencies.
        """
        try:
            from pipeline_builder_base.dependencies import DependencyAnalyzer

            # Convert step dictionaries to format expected by DependencyAnalyzer
            bronze_dict = dict(self.bronze_steps)
            silver_dict = dict(self.silver_steps)
            gold_dict = dict(self.gold_steps)

            # Analyze dependencies with creation order for deterministic tie-breaking
            analyzer = DependencyAnalyzer()
            analysis = analyzer.analyze_dependencies(
                bronze_steps=bronze_dict,
                silver_steps=silver_dict,
                gold_steps=gold_dict,
                creation_order=self._step_creation_order,  # Pass creation order
            )

            # Store execution order
            self.execution_order = analysis.execution_order

            # Log execution order
            if self.execution_order:
                self.logger.info(
                    f"📋 Execution order ({len(self.execution_order)} steps): "
                    f"{' → '.join(self.execution_order)}"
                )
            else:
                self.logger.warning("Execution order is empty - no steps to execute")
        except Exception as e:
            self.logger.warning(
                f"Could not calculate execution order: {e}. "
                f"Execution order will not be available."
            )
            self.execution_order = None

    # ============================================================================
    # PRESET CONFIGURATIONS AND HELPER METHODS
    # ============================================================================

    @classmethod
    def for_development(
        cls,
        spark: SparkSession,
        schema: str,
        functions: Optional[FunctionsProtocol] = None,
        **kwargs: Any,
    ) -> PipelineBuilder:
        """Create a PipelineBuilder optimized for development with relaxed validation.

        Creates a PipelineBuilder instance with relaxed validation thresholds
        suitable for development environments. Allows faster iteration with
        lower quality gates.

        Args:
            spark: Active SparkSession instance for data processing.
            schema: Database schema name where tables will be created.
            functions: Optional FunctionsProtocol instance for PySpark operations.
            **kwargs: Additional configuration parameters passed to __init__.

        Returns:
            PipelineBuilder instance with development-optimized settings:
            - min_bronze_rate: 80.0%
            - min_silver_rate: 85.0%
            - min_gold_rate: 90.0%
            - verbose: True

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
        functions: Optional[FunctionsProtocol] = None,
        **kwargs: Any,
    ) -> PipelineBuilder:
        """Create a PipelineBuilder optimized for production with strict validation.

        Creates a PipelineBuilder instance with strict validation thresholds
        suitable for production environments. Enforces high data quality standards.

        Args:
            spark: Active SparkSession instance for data processing.
            schema: Database schema name where tables will be created.
            functions: Optional FunctionsProtocol instance for PySpark operations.
            **kwargs: Additional configuration parameters passed to __init__.

        Returns:
            PipelineBuilder instance with production-optimized settings:
            - min_bronze_rate: 95.0%
            - min_silver_rate: 98.0%
            - min_gold_rate: 99.0%
            - verbose: False

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
        functions: Optional[FunctionsProtocol] = None,
        **kwargs: Any,
    ) -> PipelineBuilder:
        """Create a PipelineBuilder optimized for testing with minimal validation.

        Creates a PipelineBuilder instance with very relaxed validation thresholds
        suitable for testing environments. Allows maximum flexibility for test
        scenarios.

        Args:
            spark: Active SparkSession instance for data processing.
            schema: Database schema name where tables will be created.
            functions: Optional FunctionsProtocol instance for PySpark operations.
            **kwargs: Additional configuration parameters passed to __init__.

        Returns:
            PipelineBuilder instance with testing-optimized settings:
            - min_bronze_rate: 70.0%
            - min_silver_rate: 75.0%
            - min_gold_rate: 80.0%
            - verbose: True

        Example:
            >>> builder = PipelineBuilder.for_testing(
            ...     spark=spark,
            ...     schema="test_schema"
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
        columns: list[str], functions: Optional[FunctionsProtocol] = None
    ) -> ColumnRules:
        """Create validation rules for non-null constraints on multiple columns.

        Helper method to quickly create validation rules requiring columns to
        be non-null. Useful for common validation patterns.

        Args:
            columns: List of column names to validate for non-null.
            functions: Optional FunctionsProtocol instance for column operations.
                If None, uses get_default_functions().

        Returns:
            Dictionary mapping column names to lists of validation rules.
            Each column gets a single rule: F.col(column).isNotNull().

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
        columns: list[str], functions: Optional[FunctionsProtocol] = None
    ) -> ColumnRules:
        """Create validation rules for positive number constraints on multiple columns.

        Helper method to quickly create validation rules requiring columns to
        be non-null and greater than zero. Useful for count, amount, and quantity
        columns.

        Args:
            columns: List of column names to validate for positive numbers.
            functions: Optional FunctionsProtocol instance for column operations.
                If None, uses get_default_functions().

        Returns:
            Dictionary mapping column names to lists of validation rules.
            Each column gets two rules: isNotNull() and > 0.

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
            col: [functions.col(col).isNotNull(), functions.col(col) > 0]
            for col in columns
        }

    @staticmethod
    def string_not_empty_rules(
        columns: list[str], functions: Optional[FunctionsProtocol] = None
    ) -> ColumnRules:
        """Create validation rules for non-empty string constraints on multiple columns.

        Helper method to quickly create validation rules requiring string columns
        to be non-null and have length greater than zero. Useful for name, category,
        and other string identifier columns.

        Args:
            columns: List of column names to validate for non-empty strings.
            functions: Optional FunctionsProtocol instance for column operations.
                If None, uses get_default_functions().

        Returns:
            Dictionary mapping column names to lists of validation rules.
            Each column gets two rules: isNotNull() and length() > 0.

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
                functions.length(functions.col(col)) > 0,
            ]
            for col in columns
        }

    @staticmethod
    def timestamp_rules(
        columns: list[str], functions: Optional[FunctionsProtocol] = None
    ) -> ColumnRules:
        """Create validation rules for timestamp constraints on multiple columns.

        Helper method to quickly create validation rules requiring timestamp
        columns to be non-null. Useful for created_at, updated_at, and other
        timestamp columns.

        Args:
            columns: List of column names to validate as timestamps.
            functions: Optional FunctionsProtocol instance for column operations.
                If None, uses get_default_functions().

        Returns:
            Dictionary mapping column names to lists of validation rules.
            Each column gets a single rule: isNotNull() (applied twice in
            current implementation - may be simplified in future).

        Example:
            >>> rules = PipelineBuilder.timestamp_rules(["created_at", "updated_at"])
            >>> # Equivalent to:
            >>> # {
            >>> #     "created_at": [F.col("created_at").isNotNull()],
            >>> #     "updated_at": [F.col("updated_at").isNotNull()]
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
        """Detect timestamp columns from a DataFrame schema.

        Analyzes column names to identify potential timestamp columns based
        on common naming patterns. Useful for automatically configuring
        incremental processing.

        Args:
            df_schema: DataFrame schema (StructType) or list of column names
                with types. Can also be a simple list of column name strings.

        Returns:
            List of column names that match timestamp naming patterns.
            Searches for keywords like "timestamp", "created_at", "updated_at",
            etc. in column names (case-insensitive).

        Example:
            >>> timestamp_cols = PipelineBuilder.detect_timestamp_columns(df.schema)
            >>> # Returns columns like ["timestamp", "created_at", "updated_at"]
            >>> # if they exist in the schema

        Note:
            Searches for these keywords in column names (case-insensitive):
            - timestamp, created_at, updated_at, event_time, process_time,
              ingestion_time, load_time, modified_at, date_time, ts
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
        """Validate that a schema exists and is accessible.

        Overrides the base class method to add Spark-specific schema validation.
        First validates schema name format, then checks if schema exists in
        Spark catalog.

        Args:
            schema: Schema name to validate.

        Raises:
            StepError: If schema doesn't exist, is not accessible, or name
                format is invalid.
            ValidationError: If schema name format validation fails.

        Note:
            Uses base validator for format validation, then checks Spark
            catalog for existence. Provides helpful suggestions if schema
            doesn't exist.
        """
        # First validate schema name format using base validator
        try:
            errors = self._base_validator.validate_schema(schema)
            if errors:
                raise ValidationError(errors[0])
        except ValidationError:
            raise
        except Exception as e:
            # Convert to StepError for consistency
            raise StepError(
                str(e),
                context={"step_name": "schema_validation", "step_type": "validation"},
            ) from e

        # Then check if schema exists in Spark catalog
        try:
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
        """Create a schema if it doesn't exist.

        Uses SQL CREATE SCHEMA IF NOT EXISTS to create the schema idempotently.
        Logs success or failure.

        Args:
            schema: Schema name to create.

        Raises:
            StepError: If schema creation fails.

        Note:
            Uses CREATE SCHEMA IF NOT EXISTS for idempotent operation.
            Errors are wrapped in StepError with helpful suggestions.
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

    def _get_effective_schema(self, step_schema: Optional[str]) -> str:
        """Get the effective schema for a step.

        Returns the step-specific schema if provided, otherwise falls back to
        the builder's default schema.

        Args:
            step_schema: Optional schema name specified for the step.

        Returns:
            The effective schema name (step_schema if provided, otherwise
            self.schema).
        """
        return step_schema if step_schema is not None else self.schema

    def to_pipeline(self) -> PipelineRunner:
        """Build and return a PipelineRunner for executing this pipeline.

        Validates the pipeline configuration, then creates a PipelineRunner
        instance ready for execution. The runner implements the abstracts.Runner
        interface and can execute the pipeline using various execution modes.

        Args:
            None (uses instance state).

        Returns:
            PipelineRunner instance ready for execution. Implements
            abstracts.Runner interface.

        Raises:
            ValueError: If pipeline validation fails or step validation fails.

        Example:
            >>> builder = PipelineBuilder(spark=spark, schema="analytics")
            >>> builder.with_bronze_rules(name="events", rules={"id": ["not_null"]})
            >>> builder.add_silver_transform(
            ...     name="clean_events",
            ...     transform=lambda spark, df, silvers: df.filter(F.col("status") == "active"),
            ...     rules={"status": ["not_null"]},
            ...     table_name="clean_events"
            ... )
            >>> pipeline = builder.to_pipeline()
            >>> result = pipeline.run_initial_load(bronze_sources={"events": source_df})

        Note:
            The pipeline is validated before building. All steps are validated
            using the abstracts.PipelineBuilder validation to ensure interface
            compatibility.
        """
        # Validate pipeline before building
        validation_errors = self.validate_pipeline()
        if validation_errors:
            raise ValueError(
                f"Pipeline validation failed with {len(validation_errors)} errors: {', '.join(validation_errors)}"
            )

        # Check that validation-only (with_silver_rules / with_gold_rules) target tables exist
        # when optional=False; fail early at build time instead of at run time.
        missing_tables: list[str] = []
        for step in self.silver_steps.values():
            if (
                getattr(step, "existing", False)
                and step.transform is None
                and not getattr(step, "optional", False)
            ):
                schema = getattr(step, "schema", None) or self.config.schema
                table_name = getattr(step, "table_name", step.name)
                table_fqn = fqn(schema, table_name)
                if not table_exists(self.spark, table_fqn):
                    missing_tables.append(
                        f"Silver step '{step.name}' requires existing table '{table_fqn}' (optional=False)"
                    )
        for step in self.gold_steps.values():
            if (
                getattr(step, "existing", False)
                and step.transform is None
                and not getattr(step, "optional", False)
            ):
                schema = getattr(step, "schema", None) or self.config.schema
                table_name = getattr(step, "table_name", step.name)
                table_fqn = fqn(schema, table_name)
                if not table_exists(self.spark, table_fqn):
                    missing_tables.append(
                        f"Gold step '{step.name}' requires existing table '{table_fqn}' (optional=False)"
                    )
        if missing_tables:
            raise ValueError(
                "Validation-only step target table(s) do not exist. "
                "Create the table(s) or use optional=True for those steps: "
                + "; ".join(missing_tables)
            )

        # Build steps list for abstracts.PipelineBuilder validation
        all_steps = (
            list(self.bronze_steps.values())
            + list(self.silver_steps.values())
            + list(self.gold_steps.values())
        )

        # Use abstracts.PipelineBuilder to validate steps
        # This ensures step validation follows the abstracts interface
        # Type cast needed because BronzeStep/SilverStep/GoldStep satisfy Step Protocol
        try:
            from abstracts.step import Step as AbstractsStep

            # Type ignore needed because BronzeStep/SilverStep/GoldStep satisfy Step Protocol
            steps_for_validation: list[AbstractsStep] = all_steps  # type: ignore[assignment]
            self._abstracts_builder.validate_steps(steps_for_validation)
        except ValueError as e:
            raise ValueError(f"Step validation failed: {e}") from e

        # Create PipelineRunner with proper configuration
        # PipelineRunner implements abstracts.Runner, so this satisfies the interface
        # Note: steps and engine are optional parameters for abstracts compatibility
        # but we pass them to ensure the runner is properly initialized
        runner = PipelineRunner(
            spark=self.spark,
            config=self.config,
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
            logger=self.logger,
            functions=self.functions,
            steps=all_steps
            if all_steps
            else None,  # Pass steps for abstracts.Runner compatibility
            engine=self.spark_engine,  # Pass engine for abstracts.Runner compatibility
        )

        self.logger.info(
            f"🚀 Pipeline built successfully with {len(self.bronze_steps)} bronze, {len(self.silver_steps)} silver, {len(self.gold_steps)} gold steps"
        )

        return runner
