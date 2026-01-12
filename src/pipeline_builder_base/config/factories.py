"""
Configuration factory functions for creating preset configurations.

This module provides factory functions for creating PipelineConfig instances
with preset configurations for different environments.
"""

from __future__ import annotations

from typing import Any

from ..models import PipelineConfig, ValidationThresholds


def create_development_config(schema: str, **overrides: Any) -> PipelineConfig:
    """
    Create a PipelineConfig optimized for development with relaxed validation.

    Args:
        schema: Database schema name
        **overrides: Additional configuration parameters to override defaults

    Returns:
        PipelineConfig instance with development-optimized settings

    Example:
        >>> config = create_development_config("dev_schema", verbose=True)
    """
    thresholds = ValidationThresholds(
        bronze=overrides.pop("min_bronze_rate", 80.0),
        silver=overrides.pop("min_silver_rate", 85.0),
        gold=overrides.pop("min_gold_rate", 90.0),
    )

    return PipelineConfig(
        schema=schema,
        thresholds=thresholds,
        verbose=overrides.pop("verbose", True),
        **overrides,
    )


def create_production_config(schema: str, **overrides: Any) -> PipelineConfig:
    """
    Create a PipelineConfig optimized for production with strict validation.

    Args:
        schema: Database schema name
        **overrides: Additional configuration parameters to override defaults

    Returns:
        PipelineConfig instance with production-optimized settings

    Example:
        >>> config = create_production_config("prod_schema", verbose=False)
    """
    thresholds = ValidationThresholds(
        bronze=overrides.pop("min_bronze_rate", 95.0),
        silver=overrides.pop("min_silver_rate", 98.0),
        gold=overrides.pop("min_gold_rate", 99.5),
    )

    return PipelineConfig(
        schema=schema,
        thresholds=thresholds,
        verbose=overrides.pop("verbose", False),
        **overrides,
    )


def create_test_config(schema: str, **overrides: Any) -> PipelineConfig:
    """
    Create a PipelineConfig optimized for testing with minimal validation.

    Args:
        schema: Database schema name
        **overrides: Additional configuration parameters to override defaults

    Returns:
        PipelineConfig instance with test-optimized settings

    Example:
        >>> config = create_test_config("test_schema")
    """
    thresholds = ValidationThresholds(
        bronze=overrides.pop("min_bronze_rate", 50.0),
        silver=overrides.pop("min_silver_rate", 50.0),
        gold=overrides.pop("min_gold_rate", 50.0),
    )

    return PipelineConfig(
        schema=schema,
        thresholds=thresholds,
        verbose=overrides.pop("verbose", False),
        **overrides,
    )
