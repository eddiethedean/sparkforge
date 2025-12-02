from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Protocol


class Report(Protocol):
    """
    Protocol for pipeline execution reports.

    This Protocol is satisfied by PipelineReport and any object that provides
    pipeline execution results and metrics.
    """

    pipeline_id: str
    execution_id: str
    status: (
        str  # or enum - can be accessed via .value for enums or directly for strings
    )
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: float
    errors: List[str]

    @property
    def success(self) -> bool:
        """Whether the pipeline executed successfully."""
        ...
