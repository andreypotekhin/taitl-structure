from __future__ import annotations

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.operations import (
    OperationPlan,
    RelationAliasPlan,
    RelationAssertionPlan,
    RelationBoundPlan,
    RelationOrderPlan,
    RelationSetPlan,
)
from structure.plugin.pyspark.dsl.RowScope import RowScope

_ORDERABLE_TYPES = frozenset({"date", "decimal", "double", "float", "integer", "long", "string", "timestamp"})


def relation_alias(relation: object, *, name: str) -> InputScope:
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("relation_alias(...) can only be used inside a compiled Structure step method")
    _validate_alias_name(name)
    if name in context.current_scopes or name in context.relation_scopes:
        raise TypeError(f"relation_alias(name=...) scope {name!r} already exists in this step")

    source, schema = _alias_source(relation)
    alias = InputScope(name=name, schema=schema, source=source)
    context.register_relation_scope(name, alias)
    context.operations.append(
        OperationPlan.relation_alias_operation(
            RelationAliasPlan(
                input_name=name,
                source=source,
                schema=schema,
                alias=name,
            )
        )
    )
    return alias


def order_by(*orderings: object) -> RowScope:
    context = _context("order_by(...)")
    source_schema = _current_schema(context.default_project_source, function="order_by")
    order = tuple(_orderable(value, call="order_by(...)") for value in orderings)
    if not order:
        raise TypeError("order_by(...) requires at least one order expression")
    _validate_prior_operations(context.operations, function="order_by")
    context.operations.append(OperationPlan.relation_order_operation(RelationOrderPlan(order_by=order)))
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def limit(count: int) -> RowScope:
    return _bound("limit", count)


def offset(count: int) -> RowScope:
    return _bound("offset", count)


def require_unique(*keys: object) -> RowScope:
    context = _context("require_unique(...)")
    source_schema = _current_schema(context.default_project_source, function="require_unique")
    expressions = tuple(literal(key) for key in keys)
    if not expressions:
        raise TypeError("require_unique(...) requires at least one key expression")
    context.operations.append(
        OperationPlan.relation_assertion_operation(
            RelationAssertionPlan(operation="require_unique", keys=expressions)
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def require_all(predicate: object) -> RowScope:
    context = _context("require_all(...)")
    source_schema = _current_schema(context.default_project_source, function="require_all")
    expression = literal(predicate)
    if expression.type is None or expression.type.name != "boolean":
        raise TypeError("require_all(predicate) requires a Boolean expression")
    context.operations.append(
        OperationPlan.relation_assertion_operation(
            RelationAssertionPlan(operation="require_all", predicate=expression)
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def require_reference(
    value: object,
    reference: object,
    *,
    reference_key: object,
    nulls: str = "allow",
) -> RowScope:
    context = _context("require_reference(...)")
    source_schema = _current_schema(context.default_project_source, function="require_reference")
    if not isinstance(reference, InputScope):
        raise TypeError("require_reference(value, reference, ...) requires a Structure relation parameter")
    if reference._structure_joined_scope is not None:
        raise TypeError("require_reference(value, reference, ...) must be called before that relation is joined")
    if nulls not in {"allow", "reject"}:
        raise TypeError("require_reference(nulls=...) must be 'allow' or 'reject'")
    context.operations.append(
        OperationPlan.relation_assertion_operation(
            RelationAssertionPlan(
                operation="require_reference",
                value=literal(value),
                reference_input=reference._structure_input_name,
                reference_source=reference._structure_source,
                reference_schema=reference._structure_input_schema,
                reference_key=literal(reference_key),
                nulls=nulls,
            )
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def union_all(relation: object) -> RowScope:
    return _set(relation, operation="union_all", by_name=False)


def union_by_name(relation: object) -> RowScope:
    return _set(relation, operation="union_by_name", by_name=True)


def intersect(relation: object) -> RowScope:
    return _set(relation, operation="intersect", by_name=False)


def intersect_all(relation: object) -> RowScope:
    return _set(relation, operation="intersect_all", by_name=False)


def subtract(relation: object) -> RowScope:
    return _set(relation, operation="subtract", by_name=False)


def except_all(relation: object) -> RowScope:
    return _set(relation, operation="except_all", by_name=False)


def _set(relation: object, *, operation: str, by_name: bool) -> RowScope:
    context = _context(f"{operation}(...)")
    if not isinstance(relation, InputScope):
        raise TypeError(f"{operation}(relation) requires a Structure relation parameter or transform input")
    if relation._structure_joined_scope is not None:
        raise TypeError(f"{operation}(relation) must be called before that relation is joined")
    _validate_prior_operations(context.operations, function=operation)

    source_schema = _current_schema(context.default_project_source, function=operation)
    _validate_same_schema(source_schema, relation._structure_input_schema, function=operation)

    context.operations.append(
        OperationPlan.relation_set_operation(
            RelationSetPlan(
                operation=operation,
                input_name=relation._structure_input_name,
                source=relation._structure_source,
                schema=relation._structure_input_schema,
                by_name=by_name,
            )
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def _context(function: str):
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError(f"{function} can only be used inside a compiled Structure step method")
    return context


def _bound(operation: str, count: int) -> RowScope:
    context = _context(f"{operation}(...)")
    if isinstance(count, bool) or not isinstance(count, int):
        raise TypeError(f"{operation}(...) count must be a non-negative integer literal")
    if count < 0:
        raise TypeError(f"{operation}(...) count must be a non-negative integer literal")
    _validate_ordered_state(context.operations, function=operation)
    source_schema = _current_schema(context.default_project_source, function=operation)
    context.operations.append(
        OperationPlan.relation_bound_operation(operation, RelationBoundPlan(count=count))
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def _orderable(value: object, *, call: str) -> Expression:
    expression = literal(value)
    if expression.kind == "order":
        return expression
    if expression.type is None or expression.type.name not in _ORDERABLE_TYPES:
        raise TypeError(f"{call} order_by requires an orderable scalar expression")
    return expression.asc()


def _validate_alias_name(name: str) -> None:
    if not isinstance(name, str) or not name:
        raise TypeError("relation_alias(name=...) requires a non-empty string")
    if not name.isidentifier() or name.startswith("_"):
        raise TypeError(f"relation_alias(name=...) requires a public Python identifier; got {name!r}")


def _alias_source(relation: object) -> tuple[str, type[Schema]]:
    context = current_symbolic_context()
    if isinstance(relation, InputScope):
        if relation._structure_joined_scope is not None:
            raise TypeError("relation_alias(relation, ...) must be called before that relation is joined")
        return relation._structure_source, relation._structure_input_schema
    if isinstance(relation, RowScope):
        source = getattr(context, "default_project_frame", None) if context is not None else None
        schema = getattr(relation, "_structure_scope_schema", None)
        if isinstance(source, str) and isinstance(schema, type) and issubclass(schema, Schema):
            return source, schema
    raise TypeError("relation_alias(relation, ...) requires the current row scope or an unjoined relation")


def _validate_prior_operations(operations, *, function: str) -> None:
    blocked = [
        operation.kind
        for operation in operations
        if operation.kind in {"join", "aggregate", "selected_rows", "posexplode_struct"}
    ]
    if blocked:
        raise TypeError(
            f"{function}(relation) must be called before shape-changing operation(s): {', '.join(blocked)}"
        )


def _validate_ordered_state(operations, *, function: str) -> None:
    latest_order = _last_index(operations, {"order_by"})
    latest_break = _last_index(
        operations,
        {
            "aggregate",
            "except_all",
            "intersect",
            "intersect_all",
            "join",
            "posexplode_struct",
            "selected_rows",
            "subtract",
            "union_all",
            "union_by_name",
        },
    )
    if latest_order < 0 or latest_order < latest_break:
        raise TypeError(f"{function}(...) requires order_by(...) on the current relation state")


def _last_index(operations, kinds: set[str]) -> int:
    indexes = (index for index, operation in enumerate(operations) if operation.kind in kinds)
    return max(indexes, default=-1)


def _current_schema(source: object, *, function: str) -> type[Schema]:
    schema = getattr(source, "_structure_scope_schema", None)
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError(f"{function}(relation) requires a current row scope")
    return schema


def _current_scope(source: object) -> str:
    scope = getattr(source, "_structure_scope_name", None)
    return scope if isinstance(scope, str) and scope else "current"


def _validate_same_schema(left: type[Schema], right: type[Schema], *, function: str) -> None:
    left_fields = tuple(_field_signature(field) for field in left._structure_fields.values())
    right_fields = tuple(_field_signature(field) for field in right._structure_fields.values())
    if left_fields == right_fields:
        return
    raise TypeError(
        f"{function}(relation) requires identical declared schemas; "
        f"got {left.__name__} and {right.__name__}"
    )


def _field_signature(field) -> tuple[str, str, object, bool]:
    return field.name, field.column, field.type, field.nullable
