from typing import List, Dict, Union
from abc import ABC, abstractmethod

from abstracts.reports.run import Report
from abstracts.source import Source
from step import Step
from engine import Engine
from reports.run import Report
from reports.validation import ValidationReport
from reports.transform import TransformReport
from reports.write import WriteReport


class StepRunner:

    def __init__(
        self,
        steps: List[Step],
        engine: Engine
    ) -> None:
        self.steps = steps
        self.engine = engine
        self.bronze_sources: Dict[str, Source]
        self.prior_silvers = {}
        self.step_reports = {}

    def __iter__(self):
        self._current_step = 0
        return self
    
    def __next__(self) -> Union[ValidationReport, TransformReport, WriteReport]:
        if self._current_step < len(self.steps):
            step = self.steps[self._current_step]
            self._current_step += 1
            return self.run_step(step)
        raise StopIteration

    def run_next_step(self) -> Union[ValidationReport, TransformReport, WriteReport]:
        return next(self)
    
    def run_step(
        self,
        step: Step
    ) -> Union[ValidationReport, TransformReport, WriteReport]:
        if step.type == "bronze":
            validation_report = self.engine.validate_source(step, self.bronze_sources[step.name])
            self.step_reports[step.name] = validation_report
            return validation_report    
        if step.type == 'silver':
            transform_report = self.engine.transform_source(step, self. prior_silvers[step.source])
            validation_report = self.engine.validate_source(step, transform_report.source)
            self.prior_silvers[step.name] = transform_report.source
            self.step_reports[step.name] = validation_report
            write_report = self.engine.write_target(step, transform_report.source)
            return write_report
        if step.type == 'gold':
            transform_report = self.engine.transform_source(step, self.prior_silvers[step.source])
            write_report = self.engine.write_target(step, transform_report.source)
            self.step_reports[step.name] = write_report
            return write_report
        raise ValueError(f"Unknown step type: {step.type}")


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
        step_runner = StepRunner(self.steps, self.engine)
        for report in step_runner:
            ...
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