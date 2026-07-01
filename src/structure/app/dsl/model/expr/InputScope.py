from __future__ import annotations

from typing import TypeVar, cast, overload

from structure.app.compiler.compileability.streaming_compatibility.model.StreamingSupport import StreamingSupport
from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationCardinality import OperationCardinality
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.RowScope import RowScope
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinHint import JoinHint
from structure.app.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.app.dsl.model.types.BooleanType import BooleanType


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

    def exists(self, *, on: Expression, hint: JoinHint | None = None) -> Expression:
        return self._existence(JoinMethod.EXISTS, on=on, hint=hint)

    def not_exists(self, *, on: Expression, hint: JoinHint | None = None) -> Expression:
        return self._existence(JoinMethod.NOT_EXISTS, on=on, hint=hint)

    def join_many(
        self,
        *,
        on: Expression,
        how: Join = Join.INNER,
        strategy: JoinStrategy | None = None,
    ) -> RowScope:
        context = current_context()
        if context is None:
            raise RuntimeError("join_many(...) can only be used inside a compiled Structure subtransform")
        if not isinstance(on, Expression):
            raise TypeError("join_many(on=...) requires a Structure expression")
        if not isinstance(on.type, BooleanType):
            raise TypeError("join_many(on=...) requires a boolean Structure expression")
        if not isinstance(how, Join):
            raise TypeError("join_many(how=...) requires a Join value")
        if strategy is not None and not isinstance(strategy, JoinStrategy):
            raise TypeError("join_many(strategy=...) requires a JoinStrategy value")

        context.operations.append(
            OperationPlan.reserved_operation(
                "join_many",
                group="join",
                name="join_many",
                cardinality=OperationCardinality.ROW_MULTIPLYING,
                streaming=StreamingSupport.UNKNOWN,
            )
        )
        self._structure_joined_scope = RowScope(
            name=self._structure_input_name,
            schema=self._structure_input_schema,
            nullable=how is Join.LEFT,
        )
        return self._structure_joined_scope

    def __getattr__(self, name: str) -> Expression:
        if self._structure_joined_scope is not None:
            return getattr(self._structure_joined_scope, name)
        return super().__getattr__(name)

    def _existence(self, method: JoinMethod, *, on: Expression, hint: JoinHint | None) -> Expression:
        context = current_context()
        if context is None:
            raise RuntimeError(f"{method.value}(...) can only be used inside a compiled Structure subtransform")
        if not isinstance(on, Expression):
            raise TypeError(f"{method.value}(on=...) requires a Structure expression")
        if not isinstance(on.type, BooleanType):
            raise TypeError(f"{method.value}(on=...) requires a boolean Structure expression")
        if hint is not None and not isinstance(hint, JoinHint):
            raise TypeError(f"{method.value}(hint=...) requires a JoinHint value")

        return Expression(
            kind="existence_join",
            type=BooleanType(),
            nullable=False,
            data={
                "join": JoinPlan(
                    input_name=self._structure_input_name,
                    source=self._structure_source,
                    input_schema=self._structure_input_schema,
                    predicate=on,
                    how=Join.INNER,
                    hint=hint,
                    method=method,
                ),
            },
            args=(on,),
        )

    def where(self, predicate: object):
        from structure.app.dsl.model.transforms.transform_api import where

        return where(predicate)

    def project(self, *args: object) -> object:
        from structure.app.dsl.model.transforms.transform_api import project

        return project(*args)


Relation = TypeVar("Relation", bound=Structure | InputScope)


@overload
def join_one(
    relation: Relation,
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
) -> Relation: ...


@overload
def join_one(
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
) -> InputScope: ...


def join_one(
    relation: Relation | None = None,
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
) -> Relation | InputScope:
    context = current_context()
    if context is None:
        raise RuntimeError("join_one(...) can only be used inside a compiled Structure subtransform")
    if not isinstance(on, Expression):
        raise TypeError("join_one(on=...) requires a Structure expression")
    if not isinstance(on.type, BooleanType):
        raise TypeError("join_one(on=...) requires a boolean Structure expression")
    if relation is None:
        relation = cast(Relation, _infer_relation(context, on))
    if not isinstance(relation, InputScope):
        raise TypeError("join_one(relation, ...) requires a Structure relation parameter or transform input")

    _record_join(context, relation, on, how, hint)
    return relation


def _record_join(
    context,
    relation: InputScope,
    on: Expression,
    how: Join,
    hint: JoinHint | None,
) -> None:
    join = JoinPlan(
        input_name=relation._structure_input_name,
        source=relation._structure_source,
        input_schema=relation._structure_input_schema,
        predicate=on,
        how=how,
        hint=hint,
        method=JoinMethod.ONE,
    )
    context.joins.append(join)
    context.operations.append(OperationPlan.join_operation(join))
    relation._structure_joined_scope = RowScope(
        name=relation._structure_input_name,
        schema=relation._structure_input_schema,
        nullable=how is Join.LEFT,
    )


def _infer_relation(context, on: Expression) -> InputScope:
    candidates = {
        scope
        for scope in _scopes(on)
        if scope in context.relation_scopes
        and getattr(context.relation_scopes[scope], "_structure_joined_scope", None) is None
    }
    if not candidates:
        raise TypeError(
            "Cannot infer joined relation for join_one(...): the join condition does not reference an unjoined "
            "relation. Use join_one(relation, on=...) or compare against a declared input/relation parameter."
        )
    if len(candidates) > 1:
        names = ", ".join(sorted(candidates))
        first = sorted(candidates)[0]
        raise TypeError(
            "Cannot infer joined relation for join_one(...): the join condition references multiple unjoined "
            f"relations: {names}. Use join_one({first}, on=...) or join_one(relation={first}, on=...) to choose one."
        )

    candidate = next(iter(candidates))
    relation = context.relation_scopes[candidate]
    _validate_inferred_pairs(candidate, on)
    if not isinstance(relation, InputScope):
        raise TypeError(f"Cannot infer joined relation for join_one(...): scope {candidate} is not a join relation.")
    return relation


def _validate_inferred_pairs(candidate: str, on: Expression) -> None:
    for condition in _join_conditions(on):
        left, right = condition.args
        left_has_candidate = candidate in _scopes(left)
        right_has_candidate = candidate in _scopes(right)
        if left_has_candidate == right_has_candidate:
            raise TypeError(
                "Each join key pair must compare the inferred joined relation with the current row or an earlier "
                "joined scope. Use join_one(relation, on=...) if the relation cannot be inferred safely."
            )


def _join_conditions(expression: Expression) -> list[Expression]:
    if expression.kind == "and":
        return [condition for argument in expression.args for condition in _join_conditions(argument)]
    if expression.kind in {"eq", "null_safe_eq"}:
        return [expression]
    return []


def _scopes(expression: Expression) -> set[str]:
    scopes = set().union(*(_scopes(argument) for argument in expression.args))
    if expression.kind == "field" and expression.data and "scope" in expression.data:
        scopes.add(str(expression.data["scope"]))
    return scopes
