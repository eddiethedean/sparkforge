from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from abstracts.engine import Engine
from abstracts.reports.run import Report
from abstracts.reports.transform import TransformReport
from abstracts.reports.validation import ValidationReport
from abstracts.reports.write import WriteReport
from abstracts.source import Source
from abstracts.step import Step


class StepRunner:
    def __init__(self, steps: List[Step], engine: Engine) -> None:
        self.steps = steps
        self.engine = engine
        self.bronze_sources: Dict[str, Source] = {}
        self.prior_silvers: Dict[str, Source] = {}
        self.step_reports: Dict[
            str, Union[ValidationReport, TransformReport, WriteReport]
        ] = {}

    def __iter__(self) -> StepRunner:
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
        self, step: Step
    ) -> Union[ValidationReport, TransformReport, WriteReport]:
        if step.type == "bronze":
            if step.name not in self.bronze_sources:
                raise ValueError(
                    f"Bronze source '{step.name}' not found in bronze_sources"
                )
            validation_report = self.engine.validate_source(
                step, self.bronze_sources[step.name]
            )
            self.step_reports[step.name] = validation_report
            return validation_report
        elif step.type == "silver":
            if step.source is None:
                raise ValueError(f"Silver step '{step.name}' requires a source")
            if step.source not in self.prior_silvers:
                raise ValueError(f"Source '{step.source}' not found in prior_silvers")
            transform_report = self.engine.transform_source(
                step, self.prior_silvers[step.source]
            )
            validation_report = self.engine.validate_source(
                step, transform_report.source
            )
            self.prior_silvers[step.name] = transform_report.source
            self.step_reports[step.name] = validation_report
            write_report = self.engine.write_target(step, transform_report.source)
            return write_report
        elif step.type == "gold":
            if step.source is None:
                raise ValueError(f"Gold step '{step.name}' requires a source")
            if step.source not in self.prior_silvers:
                raise ValueError(f"Source '{step.source}' not found in prior_silvers")
            transform_report = self.engine.transform_source(
                step, self.prior_silvers[step.source]
            )
            write_report = self.engine.write_target(step, transform_report.source)
            self.step_reports[step.name] = write_report
            return write_report
        else:
            raise ValueError(f"Unknown step type: {step.type}")


class Runner(ABC):
    """
    Abstract base class for pipeline runners.

    Concrete implementations should provide run_initial_load and run_incremental methods.
    Additional methods like run_full_refresh and run_validation_only can be added
    by concrete implementations beyond the abstract interface.
    """

    def __init__(self, steps: List[Step], engine: Engine) -> None:
        self.steps = steps
        self.engine = engine

    @abstractmethod
    def run_initial_load(
        self, bronze_sources: Optional[Dict[str, Source]] = None
    ) -> Report:
        """
        Run initial load pipeline execution.

        Args:
            bronze_sources: Dictionary mapping bronze step names to source data

        Returns:
            Report with execution results
        """
        ...

    @abstractmethod
    def run_incremental(
        self, bronze_sources: Optional[Dict[str, Source]] = None
    ) -> Report:
        """
        Run incremental pipeline execution.

        Args:
            bronze_sources: Dictionary mapping bronze step names to source data

        Returns:
            Report with execution results
        """
        ...
