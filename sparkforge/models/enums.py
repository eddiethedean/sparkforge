"""
Enums for the Pipeline Builder models.
"""

from enum import Enum


class PipelinePhase(Enum):
    """Enumeration of pipeline phases."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ExecutionMode(Enum):
    """Enumeration of execution modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"


class WriteMode(Enum):
    """Enumeration of write modes."""

    OVERWRITE = "overwrite"
    APPEND = "append"


class ValidationResult(Enum):
    """Enumeration of validation results."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
