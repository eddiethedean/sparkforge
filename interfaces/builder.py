from __future__ import annotations
from abc import ABC, abstractmethod

from runner import Runner
from rules import Rules
from transformer import Transformer


class Builder(ABC):

    def __init__(
        self,
        reader: Reader
        writer: Writer
        
    ):
        

    def to_pipeline(self) -> Runner:
        ...

    def with_bronze_rules(
        self,
        name: str,
        rules: Rules
    ) -> Builder:
        ...

    def add_silver_transform(
        self,
        name: str,
        transform: Transformer,
        rules: Rules
    ) -> Builder:
        ...
    
    def add_gold_transform(
        self,
        name: str,
        transform: Transformer,
        rules: Rules
    ) -> Builder:
        ...