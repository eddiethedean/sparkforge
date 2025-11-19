from abc import ABC, abstractmethod
from typing import Literal, Optional

from rules import Rules
from transformer import Transformer


class Step(ABC):

    def __init__(
        self,
        name: str,
        type: Literal["bronze", "silver", "gold"],
        rules: Rules,
        write_mode: Optional[Literal["overwrite", "append"]] = None,
        source: Optional[str] = None,
        target: Optional[str] = None,
        transform: Optional[Transformer] = None,
        write_schema: Optional[str] = None
    ) -> None:
        self.name = name
        self.type = type
        self.source = source
        self.target = target
        self.rules = rules
        self.transform = transform
        self.write_mode = write_mode
        self.write_schema = write_schema