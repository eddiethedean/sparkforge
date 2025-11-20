from typing import Protocol

from report import Report
from sources import Sources


class Runner(Protocol):
    def run_initial_load(self, bronze_sources: Sources) -> Report: ...

    def run_incremental(self, bronze_sources: Sources) -> Report: ...
