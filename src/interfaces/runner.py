from typing import Protocol

from report import Report  # type: ignore[import-not-found]
from sources import Sources  # type: ignore[import-not-found]


class Runner(Protocol):
    def run_initial_load(self, bronze_sources: Sources) -> Report: ...

    def run_incremental(self, bronze_sources: Sources) -> Report: ...
