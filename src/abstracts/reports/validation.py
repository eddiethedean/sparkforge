from dataclasses import dataclass
from typing import Optional

from abstracts.source import Source


@dataclass
class ValidationReport:
    source: Source
    valid_rows: int
    invalid_rows: int
    error: Optional[Exception] = None
