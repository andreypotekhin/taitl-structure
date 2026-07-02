from __future__ import annotations

from typing import TypeVar, cast, overload

from structure.app.compiler.ir.model.JoinMethod import JoinMethod
from structure.app.compiler.ir.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.model.OperationPlan import OperationPlan
from structure.app.compiler.symbolic_execution.model.CompileContext import current_context
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.expr.RowScope import RowScope
from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.transforms.AsOf import AsOf
from structure.app.dsl.model.transforms.Join import Join
from structure.app.dsl.model.transforms.JoinAsOf import JoinAsOf
from structure.app.dsl.model.transforms.JoinDedupe import JoinDedupe
from structure.app.dsl.model.transforms.JoinHint import JoinHint
from structure.app.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.app.dsl.model.transforms.JoinTemporal import JoinTemporal
from structure.app.dsl.model.transforms.OverlapPolicy import OverlapPolicy
from structure.app.dsl.model.transforms.TiePolicy import TiePolicy
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
        return exists(self, on=on, hint=hint)

    def not_exists(self, *, on: Expression, hint: JoinHint | None = None) -> Expression:
        return not_exists(self, on=on, hint=hint)

    def join_many(
        self,
        *,
        on: Expression,
        how: Join = Join.INNER,
        strategy: JoinStrategy | None = None,
    ) -> RowScope:
        return cast(RowScope, join_many(self, on=on, how=how, strategy=strategy))

    def temporal_one(
        self,
        *,
        on: Expression,
        at: Expression,
        valid_from: Expression,
        valid_to: Expression,
        how: Join = Join.LEFT,
        overlaps: OverlapPolicy = OverlapPolicy.ERROR,
        hint: JoinHint | None = None,
    ) -> RowScope:
        return cast(
            RowScope,
            temporal_one(
                self,
                on=on,
                at=at,
                valid_from=valid_from,
                valid_to=valid_to,
                how=how,
                overlaps=overlaps,
                hint=hint,
            ),
        )

    def as_of_one(
        self,
        *,
        on: Expression,
        left_time: Expression,
        right_time: Expression,
        direction: AsOf = AsOf.BACKWARD,
        tolerance: Expression | None = None,
        how: Join = Join.LEFT,
        ties: TiePolicy = TiePolicy.ERROR,
        hint: JoinHint | None = None,
    ) -> RowScope:
        return cast(
            RowScope,
            as_of_one(
                self,
                on=on,
                left_time=left_time,
                right_time=right_time,
                direction=direction,
                tolerance=tolerance,
                how=how,
                ties=ties,
                hint=hint,
            ),
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


@overload
def join_one(
    relation: Relation,
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
    dedupe: JoinDedupe | None = None,
) -> Relation: ...


@overload
def join_one(
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
    dedupe: JoinDedupe | None = None,
) -> InputScope: ...


def join_one(
    relation: Relation | None = None,
    *,
    on: object,
    how: Join = Join.LEFT,
    hint: JoinHint | None = None,
    dedupe: JoinDedupe | None = None,
) -> Relation | InputScope:
    context = current_context()
    if context is None:
        raise RuntimeError("join_one(...) can only be used inside a compiled Structure subtransform")
    on = _join_predicate("join_one", on)
    if relation is None:
        relation = cast(Relation, _infer_relation("join_one", context, on))
    if not isinstance(relation, InputScope):
        raise TypeError("join_one(relation, ...) requires a Structure relation parameter or transform input")
    if dedupe is not None and not isinstance(dedupe, JoinDedupe):
        raise TypeError("join_one(dedupe=...) requires a JoinDedupe policy")

    _record_join(context, relation, on, how, hint, dedupe)
    return relation


@overload
def exists(
    relation: Relation,
    *,
    on: object,
    hint: JoinHint | None = None,
) -> Expression: ...


@overload
def exists(
    *,
    on: object,
    hint: JoinHint | None = None,
) -> Expression: ...


def exists(
    relation: Relation | None = None,
    *,
    on: object,
    hint: JoinHint | None = None,
) -> Expression:
    return _existence_join(JoinMethod.EXISTS, relation, on=on, hint=hint)


@overload
def not_exists(
    relation: Relation,
    *,
    on: object,
    hint: JoinHint | None = None,
) -> Expression: ...


@overload
def not_exists(
    *,
    on: object,
    hint: JoinHint | None = None,
) -> Expression: ...


def not_exists(
    relation: Relation | None = None,
    *,
    on: object,
    hint: JoinHint | None = None,
) -> Expression:
    return _existence_join(JoinMethod.NOT_EXISTS, relation, on=on, hint=hint)


@overload
def join_many(
    relation: Relation,
    *,
    on: object,
    how: Join = Join.INNER,
    strategy: JoinStrategy | None = None,
) -> Relation: ...


@overload
def join_many(
    *,
    on: object,
    how: Join = Join.INNER,
    strategy: JoinStrategy | None = None,
) -> InputScope: ...


def join_many(
    relation: Relation | None = None,
    *,
    on: object,
    how: Join = Join.INNER,
    strategy: JoinStrategy | None = None,
) -> Relation | InputScope:
    context = _join_context("join_many")
    predicate = _join_predicate("join_many", on)
    if relation is None:
        relation = cast(Relation, _infer_relation("join_many", context, predicate))
    if not isinstance(relation, InputScope):
        raise TypeError("join_many(relation, ...) requires a Structure relation parameter or transform input")
    if not isinstance(how, Join):
        raise TypeError("join_many(how=...) requires a Join value")
    if strategy is not None and not isinstance(strategy, JoinStrategy):
        raise TypeError("join_many(strategy=...) requires a JoinStrategy value")

    join = JoinPlan(
        input_name=relation._structure_input_name,
        source=relation._structure_source,
        input_schema=relation._structure_input_schema,
        predicate=predicate,
        how=how,
        strategy=strategy,
        method=JoinMethod.MANY,
    )
    _record_scoped_join(context, relation, join)
    return relation


@overload
def temporal_one(
    relation: Relation,
    *,
    on: object,
    at: object,
    valid_from: object,
    valid_to: object,
    how: Join = Join.LEFT,
    overlaps: OverlapPolicy = OverlapPolicy.ERROR,
    hint: JoinHint | None = None,
) -> Relation: ...


@overload
def temporal_one(
    *,
    on: object,
    at: object,
    valid_from: object,
    valid_to: object,
    how: Join = Join.LEFT,
    overlaps: OverlapPolicy = OverlapPolicy.ERROR,
    hint: JoinHint | None = None,
) -> InputScope: ...


def temporal_one(
    relation: Relation | None = None,
    *,
    on: object,
    at: object,
    valid_from: object,
    valid_to: object,
    how: Join = Join.LEFT,
    overlaps: OverlapPolicy = OverlapPolicy.ERROR,
    hint: JoinHint | None = None,
) -> Relation | InputScope:
    context = _join_context("temporal_one")
    predicate = _join_predicate("temporal_one", on)
    if relation is None:
        relation = cast(Relation, _infer_relation("temporal_one", context, predicate))
    if not isinstance(relation, InputScope):
        raise TypeError("temporal_one(relation, ...) requires a Structure relation parameter or transform input")
    at_expr = _expression("temporal_one", "at", at)
    valid_from_expr = _expression("temporal_one", "valid_from", valid_from)
    valid_to_expr = _expression("temporal_one", "valid_to", valid_to)
    if not isinstance(how, Join):
        raise TypeError("temporal_one(how=...) requires a Join value")
    if not isinstance(overlaps, OverlapPolicy):
        raise TypeError("temporal_one(overlaps=...) requires an OverlapPolicy value")
    if hint is not None and not isinstance(hint, JoinHint):
        raise TypeError("temporal_one(hint=...) requires a JoinHint value")

    join = JoinPlan(
        input_name=relation._structure_input_name,
        source=relation._structure_source,
        input_schema=relation._structure_input_schema,
        predicate=predicate,
        how=how,
        hint=hint,
        method=JoinMethod.TEMPORAL_ONE,
        temporal=JoinTemporal(
            at=at_expr,
            valid_from=valid_from_expr,
            valid_to=valid_to_expr,
            overlaps=overlaps,
        ),
    )
    _record_scoped_join(context, relation, join)
    return relation


@overload
def as_of_one(
    relation: Relation,
    *,
    on: object,
    left_time: object,
    right_time: object,
    direction: AsOf = AsOf.BACKWARD,
    tolerance: object | None = None,
    how: Join = Join.LEFT,
    ties: TiePolicy = TiePolicy.ERROR,
    hint: JoinHint | None = None,
) -> Relation: ...


@overload
def as_of_one(
    *,
    on: object,
    left_time: object,
    right_time: object,
    direction: AsOf = AsOf.BACKWARD,
    tolerance: object | None = None,
    how: Join = Join.LEFT,
    ties: TiePolicy = TiePolicy.ERROR,
    hint: JoinHint | None = None,
) -> InputScope: ...


def as_of_one(
    relation: Relation | None = None,
    *,
    on: object,
    left_time: object,
    right_time: object,
    direction: AsOf = AsOf.BACKWARD,
    tolerance: object | None = None,
    how: Join = Join.LEFT,
    ties: TiePolicy = TiePolicy.ERROR,
    hint: JoinHint | None = None,
) -> Relation | InputScope:
    context = _join_context("as_of_one")
    predicate = _join_predicate("as_of_one", on)
    if relation is None:
        relation = cast(Relation, _infer_relation("as_of_one", context, predicate))
    if not isinstance(relation, InputScope):
        raise TypeError("as_of_one(relation, ...) requires a Structure relation parameter or transform input")
    left_time_expr = _expression("as_of_one", "left_time", left_time)
    right_time_expr = _expression("as_of_one", "right_time", right_time)
    tolerance_expr = None if tolerance is None else _expression("as_of_one", "tolerance", tolerance)
    if not isinstance(direction, AsOf):
        raise TypeError("as_of_one(direction=...) requires an AsOf value")
    if not isinstance(how, Join):
        raise TypeError("as_of_one(how=...) requires a Join value")
    if not isinstance(ties, TiePolicy):
        raise TypeError("as_of_one(ties=...) requires a TiePolicy value")
    if hint is not None and not isinstance(hint, JoinHint):
        raise TypeError("as_of_one(hint=...) requires a JoinHint value")

    join = JoinPlan(
        input_name=relation._structure_input_name,
        source=relation._structure_source,
        input_schema=relation._structure_input_schema,
        predicate=predicate,
        how=how,
        hint=hint,
        method=JoinMethod.AS_OF_ONE,
        as_of=JoinAsOf(
            left_time=left_time_expr,
            right_time=right_time_expr,
            direction=direction,
            tolerance=tolerance_expr,
            ties=ties,
        ),
    )
    _record_scoped_join(context, relation, join)
    return relation


def _record_join(
    context,
    relation: InputScope,
    on: Expression,
    how: Join,
    hint: JoinHint | None,
    dedupe: JoinDedupe | None,
) -> None:
    join = JoinPlan(
        input_name=relation._structure_input_name,
        source=relation._structure_source,
        input_schema=relation._structure_input_schema,
        predicate=on,
        how=how,
        hint=hint,
        method=JoinMethod.ONE,
        dedupe=dedupe,
    )
    _record_scoped_join(context, relation, join)


def _join_context(function: str):
    context = current_context()
    if context is None:
        raise RuntimeError(f"{function}(...) can only be used inside a compiled Structure subtransform")
    return context


def _join_predicate(function: str, value: object) -> Expression:
    expression = _expression(function, "on", value)
    if not isinstance(expression.type, BooleanType):
        raise TypeError(f"{function}(on=...) requires a boolean Structure expression")
    return expression


def _expression(function: str, argument: str, value: object) -> Expression:
    if not isinstance(value, Expression):
        raise TypeError(f"{function}({argument}=...) requires a Structure expression")
    return value


def _record_scoped_join(context, relation: InputScope, join: JoinPlan) -> None:
    context.joins.append(join)
    context.operations.append(OperationPlan.join_operation(join))
    relation._structure_joined_scope = RowScope(
        name=relation._structure_input_name,
        schema=relation._structure_input_schema,
        nullable=join.how is Join.LEFT,
    )


def _existence_join(
    method: JoinMethod,
    relation: Relation | None,
    *,
    on: object,
    hint: JoinHint | None,
) -> Expression:
    context = _join_context(method.value)
    predicate = _join_predicate(method.value, on)
    if relation is None:
        relation = cast(Relation, _infer_relation(method.value, context, predicate))
    if not isinstance(relation, InputScope):
        raise TypeError(f"{method.value}(relation, ...) requires a Structure relation parameter or transform input")
    if hint is not None and not isinstance(hint, JoinHint):
        raise TypeError(f"{method.value}(hint=...) requires a JoinHint value")

    return Expression(
        kind="existence_join",
        type=BooleanType(),
        nullable=False,
        data={
            "join": JoinPlan(
                input_name=relation._structure_input_name,
                source=relation._structure_source,
                input_schema=relation._structure_input_schema,
                predicate=predicate,
                how=Join.INNER,
                hint=hint,
                method=method,
            ),
        },
        args=(predicate,),
    )


def _infer_relation(function: str, context, on: Expression) -> InputScope:
    candidates = {
        scope
        for scope in _scopes(on)
        if scope in context.relation_scopes
        and getattr(context.relation_scopes[scope], "_structure_joined_scope", None) is None
    }
    if not candidates:
        raise TypeError(
            f"Cannot infer joined relation for {function}(...): the join condition does not reference an unjoined "
            f"relation. Use {function}(relation, on=...) or compare against a declared input/relation parameter."
        )
    if len(candidates) > 1:
        names = ", ".join(sorted(candidates))
        first = sorted(candidates)[0]
        raise TypeError(
            f"Cannot infer joined relation for {function}(...): the join condition references multiple unjoined "
            f"relations: {names}. Use {function}({first}, on=...) or {function}(relation={first}, on=...) to choose one."
        )

    candidate = next(iter(candidates))
    relation = context.relation_scopes[candidate]
    _validate_inferred_pairs(function, candidate, on)
    if not isinstance(relation, InputScope):
        raise TypeError(f"Cannot infer joined relation for {function}(...): scope {candidate} is not a join relation.")
    return relation


def _validate_inferred_pairs(function: str, candidate: str, on: Expression) -> None:
    for condition in _join_conditions(on):
        left, right = condition.args
        left_has_candidate = candidate in _scopes(left)
        right_has_candidate = candidate in _scopes(right)
        if left_has_candidate == right_has_candidate:
            raise TypeError(
                "Each join key pair must compare the inferred joined relation with the current row or an earlier "
                f"joined scope. Use {function}(relation, on=...) if the relation cannot be inferred safely."
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
