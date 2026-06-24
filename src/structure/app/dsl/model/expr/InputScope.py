from __future__ import annotations

from typing import TypeVar, cast

from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.RowScope import RowScope
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinHint import JoinHint


class InputScope(RowScope):

    def __init__(self, *, name: str, schema: type[Structure], source: str | None = None) -> None:
        super().__init__(name=name, schema=schema)
        self._structure_input_name = name
        self._structure_source = source or name
        self._structure_input_schema = schema

    def join_one(self, *, on: Expression, how: Join = Join.LEFT, hint: JoinHint | None = None) -> RowScope:
        return join_one(self, on=on, how=how, hint=hint)


Relation = TypeVar("Relation", bound=Structure | InputScope)


def join_one(
    relation: Relation,
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
) -> Relation:
    context = current_context()
    if context is None:
        raise RuntimeError("join_one(...) can only be used inside a compiled Structure subtransform")
    if not isinstance(relation, InputScope):
        raise TypeError("join_one(relation, ...) requires a Structure relation parameter or transform input")
    if not isinstance(on, Expression):
        raise TypeError("join_one(on=...) requires a Structure expression")

    context.joins.append(
        JoinPlan(
            input_name=relation._structure_input_name,
            source=relation._structure_source,
            input_schema=relation._structure_input_schema,
            predicate=on,
            how=how,
            hint=hint,
        )
    )
    return cast(
        Relation,
        RowScope(
            name=relation._structure_input_name,
            schema=relation._structure_input_schema,
            nullable=how is Join.LEFT,
        ),
    )
