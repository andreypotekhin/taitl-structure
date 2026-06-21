from __future__ import annotations

from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.dsl.logic.model.expr.RowScope import RowScope
from structure.app.dsl.logic.model.plans.JoinPlan import JoinPlan
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.CompileContext import current_context
from structure.app.dsl.logic.model.transforms.Join import Join
from structure.app.dsl.logic.model.transforms.JoinHint import JoinHint


class InputScope(RowScope):

    def __init__(self, *, name: str, schema: type[Structure]) -> None:
        super().__init__(name=name, schema=schema)
        self._structure_input_name = name
        self._structure_input_schema = schema

    def join_one(self, *, on: Expression, how: Join = Join.LEFT, hint: JoinHint | None = None) -> RowScope:
        context = current_context()
        if context is None:
            raise RuntimeError("join_one(...) can only be used inside a compiled Structure subtransform")
        if not isinstance(on, Expression):
            raise TypeError("join_one(on=...) requires a Structure expression")

        context.joins.append(
            JoinPlan(
                input_name=self._structure_input_name,
                input_schema=self._structure_input_schema,
                predicate=on,
                how=how,
                hint=hint,
            )
        )
        return RowScope(name=self._structure_input_name, schema=self._structure_input_schema)
