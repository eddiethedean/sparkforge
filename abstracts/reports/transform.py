from dataclasses import dataclass
from typing import Optional

from source import Source


@dataclass
class TransformReport:
    source: Source
    error: Optional[Exception] = None