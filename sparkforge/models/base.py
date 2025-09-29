"""
Base classes and configuration models for the Pipeline Builder.
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
        """Validate the model. Override in subclasses."""
        pass

    def to_dict(self) -> Dict[str, ModelValue]:
        """Convert model to dictionary."""
        result: Dict[str, ModelValue] = {}
        for field_info in self.__dataclass_fields__.values():
            value = getattr(self, field_info.name)
            if hasattr(value, "to_dict"):
                result[field_info.name] = value.to_dict()
            else:
                result[field_info.name] = value
        return result

    def to_json(self) -> str:
        """Convert model to JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=2)

    def __str__(self) -> str:
        """String representation of the model."""
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.to_dict().items())})"


@dataclass
class ValidationThresholds(BaseModel):
    """
    Validation thresholds for different pipeline phases.

    Attributes:
        bronze: Bronze layer validation threshold (0-100)
        silver: Silver layer validation threshold (0-100)
        gold: Gold layer validation threshold (0-100)
    """

    bronze: float
    silver: float
    gold: float

    def validate(self) -> None:
        """Validate threshold values."""
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
        """Get threshold for a specific phase."""
        phase_map = {
            PipelinePhase.BRONZE: self.bronze,
            PipelinePhase.SILVER: self.silver,
            PipelinePhase.GOLD: self.gold,
        }
        return phase_map[phase]

    @classmethod
    def create_default(cls) -> "ValidationThresholds":
        """Create default validation thresholds."""
        return cls(bronze=95.0, silver=98.0, gold=99.0)

    @classmethod
    def create_strict(cls) -> "ValidationThresholds":
        """Create strict validation thresholds."""
        return cls(bronze=99.0, silver=99.5, gold=99.9)

    @classmethod
    def create_loose(cls) -> "ValidationThresholds":
        """Create loose validation thresholds."""
        return cls(bronze=80.0, silver=85.0, gold=90.0)


@dataclass
class ParallelConfig(BaseModel):
    """
    Configuration for parallel execution.

    Attributes:
        enabled: Whether parallel execution is enabled
        max_workers: Maximum number of parallel workers
        timeout_secs: Timeout for parallel operations in seconds
    """

    enabled: bool
    max_workers: int
    timeout_secs: int = 300

    def validate(self) -> None:
        """Validate parallel configuration."""
        if self.max_workers < 1:
            raise PipelineValidationError(
                f"max_workers must be at least 1, got {self.max_workers}"
            )
        if self.max_workers > 32:
            raise PipelineValidationError(
                f"max_workers should not exceed 32, got {self.max_workers}"
            )
        if self.timeout_secs < 1:
            raise PipelineValidationError(
                f"timeout_secs must be at least 1, got {self.timeout_secs}"
            )

    @classmethod
    def create_default(cls) -> "ParallelConfig":
        """Create default parallel configuration."""
        return cls(enabled=True, max_workers=4, timeout_secs=300)

    @classmethod
    def create_sequential(cls) -> "ParallelConfig":
        """Create sequential execution configuration."""
        return cls(enabled=False, max_workers=1, timeout_secs=600)

    @classmethod
    def create_high_performance(cls) -> "ParallelConfig":
        """Create high-performance parallel configuration."""
        return cls(enabled=True, max_workers=16, timeout_secs=1200)
