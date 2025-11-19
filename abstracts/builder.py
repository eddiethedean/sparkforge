from __future__ import annotations
from typing import List, Optional, Literal

from runner import Runner
from rules import Rules
from transformer import Transformer
from engine import Engine
from step import Step


class PipelineBuilder:

    def __init__(
        self,
        runner_cls: type[Runner],
        engine: Engine
    ) -> None:
        self.runner_cls = runner_cls
        self.engine = engine
        self.steps = []

    def validate_steps(self, steps: List[Step]) -> bool:
        ...
        # TODO: add step validation logic here

    def to_pipeline(
        self,
        steps: List[Step],
        engine: Engine
    ) -> Runner:
        if self.validate_steps(steps):
            return self.runner_cls(steps, engine)
        raise ValueError("Invalid steps configuration")

    def with_bronze_rules(
        self,
        name: str,
        rules: Rules
    ) -> PipelineBuilder:
        self.steps.append(
            Step(
                name=name,
                type="bronze",
                rules=rules
            )
        )
        return self

    def add_silver_transform(
        self,
        name: str,
        source: str,
        transform: Transformer,
        rules: Rules,
        write_target: str,
        write_mode: Literal["overwrite", "append"],
        write_schema: Optional[str] = None,
    ) -> PipelineBuilder:
        self.steps.append(
            Step(
                name=name,
                type="silver",
                source=source,
                rules=rules,
                transform=transform,
                target=write_target,
                write_mode=write_mode,
                write_schema=write_schema
            )
        )
        return self
    
    def add_gold_transform(
        self,
        name: str,
        source: str,
        transform: Transformer,
        rules: Rules,
        write_target: str,
        write_schema: Optional[str] = None,
    ) -> PipelineBuilder:
        self.steps.append(
            Step(
                name=name,
                type="gold",
                rules=rules,
                transform=transform,
                target=write_target,
                write_schema=write_schema,
                source=source
            )
        )
        return self