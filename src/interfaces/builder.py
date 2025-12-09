from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from reader import Reader  # type: ignore[import-not-found]
    from writer import Writer  # type: ignore[import-not-found]

from rules import Rules  # type: ignore[import-not-found]
from runner import Runner  # type: ignore[import-not-found]
from transformer import Transformer  # type: ignore[import-not-found]


class Builder(ABC):
    @abstractmethod
    def __init__(self, reader: Reader, writer: Writer): ...

    @abstractmethod
    def to_pipeline(self) -> Runner: ...

    @abstractmethod
    def with_bronze_rules(self, name: str, rules: Rules) -> Builder: ...

    @abstractmethod
    def add_silver_transform(
        self, name: str, transform: Transformer, rules: Rules
    ) -> Builder: ...

    @abstractmethod
    def add_gold_transform(
        self, name: str, transform: Transformer, rules: Rules
    ) -> Builder: ...
