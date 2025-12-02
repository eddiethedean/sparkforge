from __future__ import annotations

from typing import Literal, Optional, Protocol

from abstracts.rules import Rules
from abstracts.transformer import Transformer


class Step(Protocol):
    """
    Protocol for pipeline steps that BronzeStep, SilverStep, and GoldStep naturally satisfy.

    This Protocol defines the interface that all step types must implement,
    allowing duck typing compatibility between abstracts and pipeline_builder.
    """

    name: str
    type: Literal["bronze", "silver", "gold"]
    rules: Rules
    source: Optional[str]
    target: Optional[str]
    transform: Optional[Transformer]
    write_mode: Optional[Literal["overwrite", "append"]]
    write_schema: Optional[str]
