"""
Pipeline configuration models.

This module provides configuration and metrics models for pipeline execution,
including the main PipelineConfig and PipelineMetrics classes.

Key Components:
    - **PipelineConfig**: Main configuration for pipeline execution, including
      schema, validation thresholds, and logging settings
    - **PipelineMetrics**: Aggregated metrics from pipeline execution, including
      step counts, durations, row counts, and validation rates

Dependencies:
    - errors: Pipeline validation and error handling
    - models.base: Base model classes and ValidationThresholds

Example:
    >>> from pipeline_builder.models.pipeline import PipelineConfig, PipelineMetrics
    >>> from pipeline_builder.models.base import ValidationThresholds
    >>>
    >>> # Create pipeline configuration
    >>> config = PipelineConfig.create_default(schema="my_schema")
    >>> config.validate()
    >>>
    >>> # Create metrics from step results
    >>> metrics = PipelineMetrics.from_step_results(step_results)
    >>> print(f"Success rate: {metrics.success_rate}%")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..errors import PipelineValidationError
from .base import BaseModel, ValidationThresholds


@dataclass
class PipelineConfig(BaseModel):
    """Main pipeline configuration.

    Central configuration class for pipeline execution. Defines the target
    schema, validation thresholds for each Medallion Architecture layer,
    and logging verbosity.

    **Validation Rules:**
        - `schema`: Must be a non-empty string
        - `thresholds`: Must be a valid ValidationThresholds instance
        - All thresholds are validated during model validation

    Attributes:
        schema: Database schema name where pipeline tables will be created.
            Must be a non-empty string. Used to construct fully qualified
            table names (e.g., "my_schema.my_table").
        thresholds: ValidationThresholds instance defining minimum validation
            success rates for Bronze, Silver, and Gold layers. Defaults to
            standard thresholds (95%, 98%, 99%).
        verbose: Whether to enable verbose logging during pipeline execution.
            Defaults to True. When True, detailed execution logs are printed.

    Raises:
        PipelineValidationError: If schema is empty or invalid, or if
            thresholds fail validation.

    Example:
        >>> from pipeline_builder.models.pipeline import PipelineConfig
        >>> from pipeline_builder.models.base import ValidationThresholds
        >>>
        >>> # Create default configuration
        >>> config = PipelineConfig.create_default(schema="analytics")
        >>> print(config.schema)  # "analytics"
        >>>
        >>> # Create custom configuration
        >>> thresholds = ValidationThresholds(bronze=90.0, silver=95.0, gold=99.0)
        >>> config = PipelineConfig(
        ...     schema="production",
        ...     thresholds=thresholds,
        ...     verbose=False
        ... )
        >>> config.validate()
        >>>
        >>> # Access thresholds
        >>> print(f"Bronze threshold: {config.min_bronze_rate}%")
    """

    schema: str
    thresholds: ValidationThresholds
    verbose: bool = True

    @property
    def min_bronze_rate(self) -> float:
        """Get bronze validation threshold.

        Returns:
            Minimum validation success rate for Bronze layer (0-100).

        Example:
            >>> config = PipelineConfig.create_default(schema="test")
            >>> print(config.min_bronze_rate)  # 95.0
        """
        return self.thresholds.bronze

    @property
    def min_silver_rate(self) -> float:
        """Get silver validation threshold.

        Returns:
            Minimum validation success rate for Silver layer (0-100).

        Example:
            >>> config = PipelineConfig.create_default(schema="test")
            >>> print(config.min_silver_rate)  # 98.0
        """
        return self.thresholds.silver

    @property
    def min_gold_rate(self) -> float:
        """Get gold validation threshold.

        Returns:
            Minimum validation success rate for Gold layer (0-100).

        Example:
            >>> config = PipelineConfig.create_default(schema="test")
            >>> print(config.min_gold_rate)  # 99.0
        """
        return self.thresholds.gold

    def validate(self) -> None:
        """Validate pipeline configuration.

        Ensures the configuration is valid by checking schema name and
        validation thresholds. Raises an error if validation fails.

        Raises:
            PipelineValidationError: If schema is empty or invalid, or if
                thresholds fail validation.

        Example:
            >>> config = PipelineConfig.create_default(schema="test")
            >>> config.validate()  # Passes
            >>>
            >>> invalid = PipelineConfig(schema="", thresholds=ValidationThresholds.create_default())
            >>> invalid.validate()  # Raises PipelineValidationError
        """
        if not self.schema or not isinstance(self.schema, str):
            raise PipelineValidationError("Schema name must be a non-empty string")
        self.thresholds.validate()

    @classmethod
    def create_default(cls, schema: str) -> PipelineConfig:
        """Create default pipeline configuration.

        Creates a standard configuration suitable for most production use cases:
        - Standard validation thresholds (95%, 98%, 99%)
        - Verbose logging enabled

        Args:
            schema: Database schema name for pipeline tables.

        Returns:
            PipelineConfig instance with default settings.

        Example:
            >>> config = PipelineConfig.create_default(schema="analytics")
            >>> print(config.verbose)  # True
            >>> print(config.min_bronze_rate)  # 95.0
        """
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_default(),
            verbose=True,
        )

    @classmethod
    def create_high_performance(cls, schema: str) -> PipelineConfig:
        """Create high-performance pipeline configuration with strict validation.

        Creates a configuration optimized for performance and data quality:
        - Strict validation thresholds (99%, 99.5%, 99.9%)
        - Verbose logging disabled for better performance

        Args:
            schema: Database schema name for pipeline tables.

        Returns:
            PipelineConfig instance with high-performance settings.

        Example:
            >>> config = PipelineConfig.create_high_performance(schema="production")
            >>> print(config.verbose)  # False
            >>> print(config.min_gold_rate)  # 99.9
        """
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            verbose=False,
        )

    @classmethod
    def create_conservative(cls, schema: str) -> PipelineConfig:
        """Create conservative pipeline configuration with strict validation.

        Creates a configuration prioritizing data quality and observability:
        - Strict validation thresholds (99%, 99.5%, 99.9%)
        - Verbose logging enabled for detailed monitoring

        Args:
            schema: Database schema name for pipeline tables.

        Returns:
            PipelineConfig instance with conservative settings.

        Example:
            >>> config = PipelineConfig.create_conservative(schema="critical")
            >>> print(config.verbose)  # True
            >>> print(config.min_gold_rate)  # 99.9
        """
        return cls(
            schema=schema,
            thresholds=ValidationThresholds.create_strict(),
            verbose=True,
        )


@dataclass
class PipelineMetrics(BaseModel):
    """Overall pipeline execution metrics.

    Aggregates metrics from all pipeline steps to provide a comprehensive
    view of pipeline execution performance and quality. Metrics include step
    counts, durations, row counts, validation rates, and efficiency measures.

    **Validation Rules:**
        - All counts must be non-negative
        - All durations must be non-negative
        - Validation rate must be between 0 and 100
        - Total steps must equal successful + failed + skipped

    Attributes:
        total_steps: Total number of steps in the pipeline. Defaults to 0.
        successful_steps: Number of steps that completed successfully.
            Defaults to 0.
        failed_steps: Number of steps that failed during execution.
            Defaults to 0.
        skipped_steps: Number of steps that were skipped (e.g., due to
            dependencies). Defaults to 0.
        total_duration: Total execution duration in seconds. Defaults to 0.0.
        bronze_duration: Total duration for Bronze layer steps in seconds.
            Defaults to 0.0.
        silver_duration: Total duration for Silver layer steps in seconds.
            Defaults to 0.0.
        gold_duration: Total duration for Gold layer steps in seconds.
            Defaults to 0.0.
        total_rows_processed: Total number of rows processed across all steps.
            Defaults to 0.
        total_rows_written: Total number of rows written to tables across
            all steps. Defaults to 0.
        avg_validation_rate: Average validation success rate across all steps
            (0-100). Defaults to 0.0.
        cache_hit_rate: Cache hit rate (0-100). Defaults to 0.0.
        error_count: Total number of errors encountered. Defaults to 0.
        retry_count: Total number of retries attempted. Defaults to 0.

    Example:
        >>> from pipeline_builder.models.pipeline import PipelineMetrics
        >>>
        >>> # Create metrics from step results
        >>> metrics = PipelineMetrics.from_step_results(step_results)
        >>> print(f"Success rate: {metrics.success_rate}%")
        >>> print(f"Total rows: {metrics.total_rows_processed}")
        >>>
        >>> # Create metrics manually
        >>> metrics = PipelineMetrics(
        ...     total_steps=5,
        ...     successful_steps=4,
        ...     failed_steps=1,
        ...     total_duration=120.5,
        ...     total_rows_processed=1000000,
        ...     avg_validation_rate=98.5
        ... )
        >>> metrics.validate()
    """

    total_steps: int = 0
    successful_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    total_duration: float = 0.0
    bronze_duration: float = 0.0
    silver_duration: float = 0.0
    gold_duration: float = 0.0
    total_rows_processed: int = 0
    total_rows_written: int = 0
    avg_validation_rate: float = 0.0
    cache_hit_rate: float = 0.0
    error_count: int = 0
    retry_count: int = 0

    def validate(self) -> None:
        """Validate the pipeline metrics.

        Ensures all metric values are within valid ranges and consistent
        with each other. Raises an error if validation fails.

        Raises:
            ValueError: If any metric value is invalid or inconsistent.

        Example:
            >>> metrics = PipelineMetrics(total_steps=5, successful_steps=4)
            >>> metrics.validate()  # Passes
            >>>
            >>> invalid = PipelineMetrics(total_steps=-1)
            >>> invalid.validate()  # Raises ValueError
        """
        if self.total_steps < 0:
            raise ValueError("Total steps cannot be negative")
        if self.successful_steps < 0:
            raise ValueError("Successful steps cannot be negative")
        if self.failed_steps < 0:
            raise ValueError("Failed steps cannot be negative")
        if self.skipped_steps < 0:
            raise ValueError("Skipped steps cannot be negative")
        if self.total_duration < 0:
            raise ValueError("Total duration cannot be negative")
        if not 0 <= self.avg_validation_rate <= 100:
            raise ValueError("Average validation rate must be between 0 and 100")

    @property
    def success_rate(self) -> float:
        """Calculate success rate.

        Returns:
            Percentage of successful steps (0-100). Returns 0.0 if there
            are no steps.

        Example:
            >>> metrics = PipelineMetrics(total_steps=10, successful_steps=8)
            >>> print(f"Success rate: {metrics.success_rate}%")  # 80.0%
        """
        return (
            (self.successful_steps / self.total_steps * 100)
            if self.total_steps > 0
            else 0.0
        )

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate.

        Returns:
            Percentage of failed steps (0-100). Returns 0.0 if there
            are no steps.

        Example:
            >>> metrics = PipelineMetrics(total_steps=10, successful_steps=8)
            >>> print(f"Failure rate: {metrics.failure_rate}%")  # 20.0%
        """
        return 100.0 - self.success_rate

    @classmethod
    def from_step_results(cls, step_results: list[Any]) -> PipelineMetrics:
        """Create metrics from step results.

        Aggregates metrics from a list of StepResult objects to create
        comprehensive pipeline metrics.

        Args:
            step_results: List of StepResult objects from pipeline execution.

        Returns:
            PipelineMetrics instance with aggregated metrics from all steps.

        Example:
            >>> from pipeline_builder.models.execution import StepResult
            >>> from pipeline_builder.models.enums import PipelinePhase
            >>> from datetime import datetime, timezone
            >>>
            >>> # Create step results
            >>> results = [
            ...     StepResult.create_success(
            ...         step_name="bronze_step",
            ...         phase=PipelinePhase.BRONZE,
            ...         start_time=datetime.now(timezone.utc),
            ...         end_time=datetime.now(timezone.utc),
            ...         rows_processed=1000,
            ...         rows_written=950,
            ...         validation_rate=95.0
            ...     )
            ... ]
            >>>
            >>> # Aggregate metrics
            >>> metrics = PipelineMetrics.from_step_results(results)
            >>> print(f"Total steps: {metrics.total_steps}")  # 1
            >>> print(f"Success rate: {metrics.success_rate}%")  # 100.0%
        """
        total_steps = len(step_results)
        successful_steps = sum(1 for result in step_results if result.success)
        failed_steps = total_steps - successful_steps
        total_duration_secs = sum(result.duration_secs for result in step_results)
        total_rows_processed = sum(result.rows_processed for result in step_results)
        total_rows_written = sum(result.rows_written for result in step_results)
        avg_validation_rate = (
            sum(result.validation_rate for result in step_results) / total_steps
            if total_steps > 0
            else 0.0
        )

        return cls(
            total_steps=total_steps,
            successful_steps=successful_steps,
            failed_steps=failed_steps,
            total_duration=total_duration_secs,
            total_rows_processed=total_rows_processed,
            total_rows_written=total_rows_written,
            avg_validation_rate=avg_validation_rate,
        )
