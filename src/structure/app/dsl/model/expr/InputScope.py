from __future__ import annotations

from typing import TypeVar, cast

from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
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
        self._structure_joined_scope: RowScope | None = None

    def join_one(self, *, on: Expression, how: Join = Join.LEFT, hint: JoinHint | None = None) -> RowScope:
        raise TypeError(
            "self.customers.join_one(...) is no longer supported. "
            "Use join_one(self.customers, on=...) or add a relation parameter "
            "and use join_one(customer, on=...)."
        )

    def __getattr__(self, name: str) -> Expression:
        if self._structure_joined_scope is not None:
            return getattr(self._structure_joined_scope, name)
        return super().__getattr__(name)

    def where(self, predicate: object):
        from structure.app.dsl.model.transforms.transform_api import where

        return where(predicate)

    def project(self, *args: object) -> object:
        from structure.app.dsl.model.transforms.transform_api import project

        return project(*args)


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

    join = JoinPlan(
        input_name=relation._structure_input_name,
        source=relation._structure_source,
        input_schema=relation._structure_input_schema,
        predicate=on,
        how=how,
        hint=hint,
    )
    context.joins.append(join)
    context.operations.append(OperationPlan.join_operation(join))
    relation._structure_joined_scope = RowScope(
        name=relation._structure_input_name,
        schema=relation._structure_input_schema,
        nullable=how is Join.LEFT,
    )
    return cast(Relation, relation)
