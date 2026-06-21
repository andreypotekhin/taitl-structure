from __future__ import annotations

from dataclasses import dataclass

from structure.app.dsl.logic.model.expressions import Expression
from structure.app.dsl.logic.model.Schema import FieldDefinition, Structure


@dataclass(frozen=True)
class InputPlan:
    name: str
    schema: type[Structure]
    ordinal: int


@dataclass(frozen=True)
class ProjectAssignment:
    field: FieldDefinition
    expression: Expression


@dataclass(frozen=True)
class StepPlan:
    name: str
    input_schema: type[Structure]
    output_schema: type[Structure]
    filters: tuple[Expression, ...]
    projection: tuple[ProjectAssignment, ...]
    ordinal: int


@dataclass(frozen=True)
class TransformPlan:
    name: str
    inputs: tuple[InputPlan, ...]
    steps: tuple[StepPlan, ...]

    @property
    def output_schema(self) -> type[Structure]:
        return self.steps[-1].output_schema
