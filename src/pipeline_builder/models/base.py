"""
Base classes and configuration models for the Pipeline Builder.

This module provides the foundational model classes that all pipeline components
inherit from, including base validation, serialization, and configuration models.

Key Components:
    - **BaseModel**: Abstract base class for all pipeline models with common
      functionality for validation, serialization, and representation
    - **ValidationThresholds**: Configuration for validation thresholds across
      pipeline phases (Bronze, Silver, Gold)

Dependencies:
    - errors: Pipeline validation and error handling
    - models.enums: Pipeline phase enumerations
    - models.types: Type definitions and protocols

Example:
    >>> from pipeline_builder.models.base import BaseModel, ValidationThresholds
    >>> from dataclasses import dataclass
    >>>
    >>> @dataclass
    >>> class MyStep(BaseModel):
    ...     name: str
    ...     value: int
    ...
    ...     def validate(self) -> None:
    ...         if not self.name:
    ...             raise ValueError("Name required")
    ...         if self.value < 0:
    ...             raise ValueError("Value must be non-negative")
    >>>
    >>> step = MyStep(name="test", value=42)
    >>> step.validate()
    >>> print(step.to_json())
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict

from ..errors import PipelineValidationError
from .enums import PipelinePhase
from .types import ModelValue


@dataclass
class BaseModel(ABC):
    """
    Base class for all pipeline models with common functionality.

    Provides standard validation, serialization, and representation methods
    for all pipeline data models. All models in the pipeline system inherit
    from this base class to ensure consistent behavior.

    Features:
    - Automatic validation support
    - JSON serialization and deserialization
    - Dictionary conversion for easy data exchange
    - String representation for debugging
    - Type-safe field access

    Example:
        >>> @dataclass
        >>> class MyStep(BaseModel):
        ...     name: str
        ...     rules: Dict[str, List[ColumnRule]]
        ...
        ...     def validate(self) -> None:
        ...         if not self.name:
        ...             raise ValueError("Name cannot be empty")
        ...         if not self.rules:
        ...             raise ValueError("Rules cannot be empty")
        >>>
        >>> step = MyStep(name="test", rules={"id": [F.col("id").isNotNull()]})
        >>> step.validate()
        >>> print(step.to_json())
    """

    @abstractmethod
    def validate(self) -> None:
        """Validate the model.

        This method must be implemented by all subclasses to ensure model
        integrity. It should raise appropriate exceptions if validation fails.

        Raises:
            ValidationError: If the model is invalid. Subclasses should raise
                specific error types (e.g., PipelineValidationError).

        Example:
            >>> @dataclass
            >>> class MyModel(BaseModel):
            ...     name: str
            ...
            ...     def validate(self) -> None:
            ...         if not self.name:
            ...             raise ValueError("Name cannot be empty")
            >>>
            >>> model = MyModel(name="test")
            >>> model.validate()  # Passes
        """
        pass

    def to_dict(self) -> Dict[str, ModelValue]:
        """Convert model to dictionary.

        Recursively converts the model and all nested models to dictionaries.
        Nested models that have a `to_dict` method will be converted recursively.

        Returns:
            Dictionary representation of the model with all fields converted
            to primitive types or dictionaries.

        Example:
            >>> step = BronzeStep(name="test", rules={"id": [F.col("id").isNotNull()]})
            >>> step_dict = step.to_dict()
            >>> print(step_dict["name"])  # "test"
        """
        result: Dict[str, ModelValue] = {}
        for field_info in self.__dataclass_fields__.values():
            value = getattr(self, field_info.name)
            if hasattr(value, "to_dict"):
                result[field_info.name] = value.to_dict()
            else:
                result[field_info.name] = value
        return result

    def to_json(self) -> str:
        """Convert model to JSON string.

        Serializes the model to a formatted JSON string with indentation.
        Uses the model's `to_dict` method for conversion.

        Returns:
            JSON string representation of the model, formatted with 2-space
            indentation.

        Example:
            >>> step = BronzeStep(name="test", rules={"id": [F.col("id").isNotNull()]})
            >>> json_str = step.to_json()
            >>> print(json_str)
            {
              "name": "test",
              "rules": {...}
            }
        """
        return json.dumps(self.to_dict(), default=str, indent=2)

    def __str__(self) -> str:
        """String representation of the model.

        Returns:
            Human-readable string representation showing the class name and
            all field values.

        Example:
            >>> step = BronzeStep(name="test", rules={"id": [F.col("id").isNotNull()]})
            >>> print(str(step))
            BronzeStep(name=test, rules={'id': [...]})
        """
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.to_dict().items())})"


@dataclass
class ValidationThresholds(BaseModel):
    """Validation thresholds for different pipeline phases.

    Defines the minimum validation success rates required for each layer
    of the Medallion Architecture. Thresholds are expressed as percentages
    (0-100) and are used to determine if pipeline execution meets quality
    requirements.

    **Validation Rules:**
        - All thresholds must be between 0 and 100 (inclusive)
        - Thresholds are validated during model validation

    Attributes:
        bronze: Bronze layer validation threshold (0-100). Defaults to 95.0
            for standard configurations. Represents the minimum percentage
            of rows that must pass validation in the Bronze layer.
        silver: Silver layer validation threshold (0-100). Defaults to 98.0
            for standard configurations. Represents the minimum percentage
            of rows that must pass validation in the Silver layer.
        gold: Gold layer validation threshold (0-100). Defaults to 99.0
            for standard configurations. Represents the minimum percentage
            of rows that must pass validation in the Gold layer.

    Raises:
        PipelineValidationError: If any threshold is outside the valid range
            (0-100) during validation.

    Example:
        >>> # Create default thresholds
        >>> thresholds = ValidationThresholds.create_default()
        >>> print(f"Bronze: {thresholds.bronze}%")  # Bronze: 95.0%
        >>>
        >>> # Create custom thresholds
        >>> thresholds = ValidationThresholds(
        ...     bronze=90.0,
        ...     silver=95.0,
        ...     gold=99.0
        ... )
        >>> thresholds.validate()
        >>>
        >>> # Get threshold for specific phase
        >>> from pipeline_builder.models.enums import PipelinePhase
        >>> bronze_threshold = thresholds.get_threshold(PipelinePhase.BRONZE)
    """

    bronze: float
    silver: float
    gold: float

    def validate(self) -> None:
        """Validate threshold values.

        Ensures all thresholds are within the valid range (0-100).
        Raises an error if any threshold is invalid.

        Raises:
            PipelineValidationError: If any threshold is outside the valid
                range (0-100).

        Example:
            >>> thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
            >>> thresholds.validate()  # Passes
            >>>
            >>> invalid = ValidationThresholds(bronze=150.0, silver=98.0, gold=99.0)
            >>> invalid.validate()  # Raises PipelineValidationError
        """
        for phase, threshold in [
            ("bronze", self.bronze),
            ("silver", self.silver),
            ("gold", self.gold),
        ]:
            if not 0 <= threshold <= 100:
                raise PipelineValidationError(
                    f"{phase} threshold must be between 0 and 100, got {threshold}"
                )

    def get_threshold(self, phase: PipelinePhase) -> float:
        """Get threshold for a specific phase.

        Args:
            phase: The pipeline phase to get the threshold for.

        Returns:
            The validation threshold for the specified phase (0-100).

        Example:
            >>> thresholds = ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0)
            >>> from pipeline_builder.models.enums import PipelinePhase
            >>> bronze_threshold = thresholds.get_threshold(PipelinePhase.BRONZE)
            >>> print(bronze_threshold)  # 95.0
        """
        phase_map = {
            PipelinePhase.BRONZE: self.bronze,
            PipelinePhase.SILVER: self.silver,
            PipelinePhase.GOLD: self.gold,
        }
        return phase_map[phase]

    @classmethod
    def create_default(cls) -> ValidationThresholds:
        """Create default validation thresholds.

        Returns a standard configuration suitable for most production use cases:
        - Bronze: 95.0% (allows some data quality issues in raw data)
        - Silver: 98.0% (higher quality after cleaning)
        - Gold: 99.0% (very high quality for analytics)

        Returns:
            ValidationThresholds instance with default values.

        Example:
            >>> thresholds = ValidationThresholds.create_default()
            >>> print(f"Bronze: {thresholds.bronze}%")  # Bronze: 95.0%
        """
        return cls(bronze=95.0, silver=98.0, gold=99.0)

    @classmethod
    def create_strict(cls) -> ValidationThresholds:
        """Create strict validation thresholds.

        Returns a high-quality configuration for critical data pipelines:
        - Bronze: 99.0% (very high quality raw data)
        - Silver: 99.5% (extremely high quality after cleaning)
        - Gold: 99.9% (near-perfect quality for analytics)

        Use this configuration when data quality is critical and you can
        afford to reject more rows.

        Returns:
            ValidationThresholds instance with strict values.

        Example:
            >>> thresholds = ValidationThresholds.create_strict()
            >>> print(f"Gold: {thresholds.gold}%")  # Gold: 99.9%
        """
        return cls(bronze=99.0, silver=99.5, gold=99.9)

    @classmethod
    def create_loose(cls) -> ValidationThresholds:
        """Create loose validation thresholds.

        Returns a permissive configuration for exploratory or development use:
        - Bronze: 80.0% (allows significant data quality issues)
        - Silver: 85.0% (moderate quality after cleaning)
        - Gold: 90.0% (acceptable quality for analytics)

        Use this configuration for development, testing, or when working
        with noisy data sources.

        Returns:
            ValidationThresholds instance with loose values.

        Example:
            >>> thresholds = ValidationThresholds.create_loose()
            >>> print(f"Bronze: {thresholds.bronze}%")  # Bronze: 80.0%
        """
        return cls(bronze=80.0, silver=85.0, gold=90.0)
