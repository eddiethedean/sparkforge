from typing import List, Dict
from abc import ABC, abstractmethod

from abstracts.reports.run import Report
from abstracts.source import Source
from step import Step
from engine import Engine


class Runner(ABC):

    def __init__(
        self,
        steps: List[Step],
        engine: Engine
    ) -> None:
        self.steps = steps
        self.engine = engine

    @abstractmethod
    def run_initial_load(
        self,
        /,
        bronze_sources: Dict[str, Source]
    ) -> Report:
        prior_silvers = {}
        step_reports = {}
        for step in self.steps:
            if step.type == "bronze":
                # Implement initial load logic for bronze steps
                validation_report = self.engine.validate_source(step, bronze_sources[step.name])
                step_reports[step.name] = validation_report
            if step.type == 'silver':
                # Implement initial load logic for silver steps
                transform_report = self.engine.transform_source(step, prior_silvers[step.source])
                validation_report = self.engine.validate_source(step, transform_report.source)
                prior_silvers[step.name] = transform_report.source
                step_reports[step.name] = validation_report
            if step.type == 'gold':
                # Implement initial load logic for gold steps
                transform_report = self.engine.transform_source(step, prior_silvers[step.source])
                write_report = self.engine.write_target(step, transform_report.source)
                step_reports[step.name] = write_report
        # TODO: Aggregate step_reports into a final Report

    @abstractmethod
    def run_incremental(
        self,
        /,
        bronze_sources: Dict[str, Source]
    ) -> Report:
        for step in self.steps:
            if step.type == "bronze":
                # Implement incremental load logic for bronze steps
                pass
            if step.type in ["silver", "gold"]:
                # Implement incremental load logic for silver and gold steps
                pass