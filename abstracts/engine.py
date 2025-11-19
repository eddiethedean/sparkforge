from abc import ABC, abstractmethod

from step import Step
from source import Source
from abstracts.reports.validation import ValidationReport
from abstracts.reports.transform import TransformReport
from abstracts.reports.write import WriteReport


class Engine(ABC):

    @abstractmethod
    def validate_source(
        self,
        step: Step,
        source: Source
    ) -> ValidationReport:
        ...

    @abstractmethod
    def transform_source(
        self,
        step: Step,
        source: Source
    ) -> TransformReport:
        ...

    @abstractmethod
    def write_target(
        self,
        step: Step,
        source: Source
    ) -> WriteReport:
        ...

    