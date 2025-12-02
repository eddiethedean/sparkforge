from dataclasses import dataclass
from typing import Optional

from abstracts.source import Source


@dataclass
class WriteReport:
    source: Source
    written_rows: int
    failed_rows: int
    error: Optional[Exception] = None
