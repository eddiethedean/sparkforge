"""
Step model protocols for pipeline builders.

This module defines Protocol classes that step implementations should follow.
These protocols allow the base package to work with any step implementation
without knowing the specific details of Spark or SQL steps.
"""

from typing import Dict, List, Protocol, Any


class StepProtocol(Protocol):
    """Protocol for all pipeline steps."""

    name: str
    rules: Dict[str, Any]

    def validate(self) -> None:
        """Validate the step configuration."""
        ...


class BronzeStepProtocol(StepProtocol, Protocol):
    """Protocol for bronze layer steps."""

    incremental_col: str | None


class SilverStepProtocol(StepProtocol, Protocol):
    """Protocol for silver layer steps."""

    source_bronze: str
    table_name: str


class GoldStepProtocol(StepProtocol, Protocol):
    """Protocol for gold layer steps."""

    source_silvers: list[str] | None
    table_name: str

