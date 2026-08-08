"""Relation-level PySpark DSL helpers.

These helpers describe operations that affect the current relation as a whole:
ordering, limits, data-quality assertions, set operations, and hierarchy
expansion.  Each helper records an operation plan in the active symbolic
context; users get immediate validation without executing Spark.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import structure.plugin.pyspark.dsl.options as options
from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.joins import TiePolicy
from structure.plugin.pyspark.dsl.operations import (
    OperationPlan,
    RelationAliasPlan,
    RelationAssertionPlan,
    RelationBoundPlan,
    RelationHierarchyClosurePlan,
    RelationHierarchyFallbackPlan,
    RelationOrderPlan,
    RelationPrioritySelectionPlan,
    RelationSamplePlan,
    RelationSetPlan,
)
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import ArrayType, DecimalType, LongType, MapType, StringType, StructType

_ORDERABLE_TYPES = frozenset({"date", "decimal", "double", "float", "integer", "long", "string", "timestamp"})


def relation_alias(relation: object, *, name: str) -> InputScope:
    """Expose a relation or current row scope under a new public scope name.

    Args:
        relation: Existing relation parameter or current row scope.
        name: Public Python identifier used for the new scope.

    Returns:
        An input scope that can be referenced by later expressions.

    Example:
        historical = relation_alias(customer, name="historical_customer")
    """
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
    """Order the current relation by one or more orderable expressions.

    Args:
        *orderings: Orderable expressions, optionally already decorated with
            ``asc()`` or ``desc()``.

    Returns:
        The current row scope after ordering.

    Example:
        latest = order_by(order.created_at.desc()).limit(1)
    """
    context = _context("order_by(...)")
    source_schema = _current_schema(context.default_project_source, function="order_by")
    order = tuple(_orderable(value, call="order_by(...)") for value in orderings)
    if not order:
        raise TypeError("order_by(...) requires at least one order expression")
    _validate_prior_operations(context.operations, function="order_by")
    context.operations.append(OperationPlan.relation_order_operation(RelationOrderPlan(order_by=order)))
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def limit(count: int) -> RowScope:
    """Keep only the first ``count`` rows after ``order_by(...)``."""
    return _bound("limit", count)


def offset(count: int) -> RowScope:
    """Skip the first ``count`` rows after ``order_by(...)``."""
    return _bound("offset", count)


def sample(
    fraction: float,
    *,
    with_replacement: bool = False,
    seed: int | None = None,
    reproducible: bool = True,
) -> RowScope:
    """Sample the current batch relation with explicit reproducibility policy.

    Args:
        fraction: Sampling fraction. Without replacement it must be in ``[0, 1]``;
            with replacement it must be non-negative.
        with_replacement: Whether rows may be sampled more than once.
        seed: Deterministic Spark sampling seed. Required by default.
        reproducible: Set to ``False`` to opt into non-repeatable sampling.

    Returns:
        The current row scope after sampling.
    """
    context = _context("sample(...)")
    fraction = _sample_fraction(fraction, with_replacement=with_replacement)
    seed = _sample_seed(seed, reproducible=reproducible)
    source_schema = _current_schema(context.default_project_source, function="sample")
    context.operations.append(
        OperationPlan.relation_sample_operation(
            RelationSamplePlan(
                fraction=fraction,
                with_replacement=with_replacement,
                seed=seed,
                reproducible=reproducible,
            )
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def require_unique(*keys: object) -> RowScope:
    """Assert that the current relation has no duplicate key tuple."""
    context = _context("require_unique(...)")
    source_schema = _current_schema(context.default_project_source, function="require_unique")
    expressions = tuple(literal(key) for key in keys)
    if not expressions:
        raise TypeError("require_unique(...) requires at least one key expression")
    context.operations.append(
        OperationPlan.relation_assertion_operation(RelationAssertionPlan(operation="require_unique", keys=expressions))
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def require_all(predicate: object) -> RowScope:
    """Assert that every row in the current relation satisfies ``predicate``."""
    context = _context("require_all(...)")
    source_schema = _current_schema(context.default_project_source, function="require_all")
    expression = literal(predicate)
    if expression.type is None or expression.type.name != "boolean":
        raise TypeError("require_all(predicate) requires a Boolean expression")
    context.operations.append(
        OperationPlan.relation_assertion_operation(RelationAssertionPlan(operation="require_all", predicate=expression))
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def require_reference(
    value: object,
    reference: object,
    *,
    reference_key: object,
    nulls: str = "allow",
) -> RowScope:
    """Assert that a value exists in a referenced relation key.

    Args:
        value: Field expression from the current relation.
        reference: Relation parameter that provides valid keys.
        reference_key: Field expression on ``reference``.
        nulls: ``"allow"`` or ``"reject"`` for null ``value`` handling.

    Returns:
        The current row scope.

    Example:
        require_reference(order.customer_id, customers, reference_key=customers.id, nulls="reject")
    """
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


def require_parent_hierarchy(
    id: object,
    *,
    parent: object,
    order_by: object,
    max_depth: int,
) -> RowScope:
    """Assert that parent pointers form a bounded, valid hierarchy."""
    context = _context("require_parent_hierarchy(...)")
    source_schema = _current_schema(context.default_project_source, function="require_parent_hierarchy")
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
        raise TypeError("require_parent_hierarchy(max_depth=...) must be a positive integer literal")
    context.operations.append(
        OperationPlan.relation_assertion_operation(
            RelationAssertionPlan(
                operation="require_parent_hierarchy",
                keys=(_field_key(id),),
                parent=_field_key(parent),
                order_by=_orderable(order_by, call="require_parent_hierarchy(...)"),
                max_depth=max_depth,
            )
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def hierarchy_closure(
    id: object,
    *,
    parent: object,
    as_: type[Schema],
    node: str = "node_id",
    ancestor: str = "ancestor_id",
    depth: str = "depth",
    max_depth: int,
    scope: str | None = None,
) -> RowScope:
    """Build ancestor rows for a parent hierarchy into a generated scope.

    Args:
        id: Non-null field expression for the current node id.
        parent: Field expression for the parent id.
        as_: Output schema for generated closure rows.
        node: Field in ``as_`` that receives the node id.
        ancestor: Field in ``as_`` that receives the ancestor id.
        depth: Field in ``as_`` that receives distance from node to ancestor.
        max_depth: Positive traversal bound.
        scope: Optional scope name for the generated rows.

    Returns:
        A row scope for the generated hierarchy closure.

    Example:
        closure = hierarchy_closure(
            category.id,
            parent=category.parent_id,
            as_=CategoryClosure,
            max_depth=20,
        )
    """
    context = _context("hierarchy_closure(...)")
    if not isinstance(as_, type) or not issubclass(as_, Schema):
        raise TypeError("hierarchy_closure(as_=...) requires a Structure Schema class")
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
        raise TypeError("hierarchy_closure(max_depth=...) must be a positive integer literal")
    if scope is not None and (not isinstance(scope, str) or not scope):
        raise TypeError("hierarchy_closure(scope=...) requires a non-empty string")
    id_expression = _field_key(id)
    parent_expression = _field_key(parent)
    _validate_prior_operations(context.operations, function="hierarchy_closure")
    _validate_closure_schema(
        as_,
        id_expression=id_expression,
        parent_expression=parent_expression,
        node=node,
        ancestor=ancestor,
        depth=depth,
    )
    closure_scope = scope or _default_scope(as_)
    context.operations.append(
        OperationPlan.relation_hierarchy_closure_operation(
            RelationHierarchyClosurePlan(
                id=id_expression,
                parent=parent_expression,
                schema=as_,
                scope=closure_scope,
                node=node,
                ancestor=ancestor,
                depth=depth,
                max_depth=max_depth,
            )
        )
    )
    context.register_current_scope(closure_scope)
    return RowScope(name=closure_scope, schema=as_)


def hierarchy_fallbacks(
    source_id: object,
    path: object,
    parents: object,
    *,
    parent_id: object,
    parent: object,
    as_: type[Schema],
    source: str = "user_band_id",
    fallback: str = "user_band_fallback_id",
    ordinal: str = "ordinal",
    separator: str = "\u001f",
    max_depth: int,
    scope: str | None = None,
) -> RowScope:
    """Generate ordered fallback rows from hierarchy paths and parent records."""
    context = _context("hierarchy_fallbacks(...)")
    if not isinstance(parents, InputScope):
        raise TypeError("hierarchy_fallbacks(parents, ...) requires a Structure relation parameter")
    if parents._structure_joined_scope is not None:
        raise TypeError("hierarchy_fallbacks(parents, ...) must be called before that relation is joined")
    if not isinstance(as_, type) or not issubclass(as_, Schema):
        raise TypeError("hierarchy_fallbacks(as_=...) requires a Structure Schema class")
    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 1:
        raise TypeError("hierarchy_fallbacks(max_depth=...) must be a positive integer literal")
    if not isinstance(separator, str) or not separator:
        raise TypeError("hierarchy_fallbacks(separator=...) requires a non-empty string")
    if scope is not None and (not isinstance(scope, str) or not scope):
        raise TypeError("hierarchy_fallbacks(scope=...) requires a non-empty string")

    source_expression = _field_key(source_id)
    path_expression = literal(path)
    parent_id_expression = _field_key(parent_id)
    parent_expression = _field_key(parent)
    _validate_prior_operations(context.operations, function="hierarchy_fallbacks")
    _validate_fallback_schema(
        as_,
        source_expression=source_expression,
        path_expression=path_expression,
        parent_id_expression=parent_id_expression,
        parent_expression=parent_expression,
        source=source,
        fallback=fallback,
        ordinal=ordinal,
    )
    fallback_scope = scope or _default_scope(as_)
    context.operations.append(
        OperationPlan.relation_hierarchy_fallback_operation(
            RelationHierarchyFallbackPlan(
                source_id=source_expression,
                path=path_expression,
                parent_input=parents._structure_input_name,
                parent_source=parents._structure_source,
                parent_schema=parents._structure_input_schema,
                parent_id=parent_id_expression,
                parent=parent_expression,
                schema=as_,
                scope=fallback_scope,
                source=source,
                fallback=fallback,
                ordinal=ordinal,
                separator=separator,
                max_depth=max_depth,
            )
        )
    )
    context.register_current_scope(fallback_scope)
    return RowScope(name=fallback_scope, schema=as_)


def select_first_qualified(
    *keys: object,
    where: object,
    order_by: object,
    missing: str = "allow",
    ties: TiePolicy | str = TiePolicy.ERROR,
) -> RowScope:
    """Select the first row per key that satisfies a qualifier predicate.

    Args:
        *keys: Declared field references that define each partition.
        where: Boolean qualifier predicate.
        order_by: Ordering expression used to choose the first qualified row.
        missing: ``"allow"`` or ``"error"`` when no row qualifies.
        ties: Tie policy. Only ``"error"`` is currently supported.

    Returns:
        The current row scope after priority selection.

    Example:
        selected = select_first_qualified(
            offer.customer_id,
            where=offer.active,
            order_by=offer.priority.asc(),
        )
    """
    context = _context("select_first_qualified(...)")
    source_schema = _current_schema(context.default_project_source, function="select_first_qualified")
    expressions = tuple(_field_key(key) for key in keys)
    if not expressions:
        raise TypeError("select_first_qualified(...) requires at least one declared key field")
    predicate = literal(where)
    if predicate.type is None or predicate.type.name != "boolean":
        raise TypeError("select_first_qualified(where=...) requires a Boolean expression")
    if missing not in {"allow", "error"}:
        raise TypeError("select_first_qualified(missing=...) must be 'allow' or 'error'")
    ties = options.tie_policy(ties, call="select_first_qualified(...)")
    if ties is not TiePolicy.ERROR:
        raise TypeError('select_first_qualified(ties=...) requires "error"')
    context.operations.append(
        OperationPlan.relation_priority_selection_operation(
            RelationPrioritySelectionPlan(
                keys=expressions,
                predicate=predicate,
                order_by=_orderable(order_by, call="select_first_qualified(...)"),
                missing=missing,
                ties=ties,
            )
        )
    )
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def union_all(relation: object) -> RowScope:
    """Union the current relation with another relation by position.

    Args:
        relation: Relation parameter or transform input with an identical schema.

    Returns:
        The current row scope after Spark ``union``-style concatenation.

    Example:
        all_orders = union_all(archived_orders)
    """
    return _set(relation, operation="union_all", by_name=False)


def union_by_name(
    relation: object,
    *,
    allow_missing_columns: bool = False,
    defaults: Mapping[str, object] | None = None,
) -> RowScope:
    """Union the current relation with another relation by field name.

    Args:
        relation: Relation parameter or transform input.
        allow_missing_columns: Fill nullable top-level missing columns with null.
        defaults: Typed literal defaults for non-nullable missing fields, keyed by
            canonical Structure field name.
    """
    if not isinstance(allow_missing_columns, bool):
        raise TypeError("union_by_name(allow_missing_columns=...) requires a Boolean")
    if defaults is not None and not isinstance(defaults, Mapping):
        raise TypeError("union_by_name(defaults=...) requires a mapping of field paths to typed literals")
    return _set(
        relation,
        operation="union_by_name",
        by_name=True,
        allow_missing_columns=allow_missing_columns,
        defaults=defaults,
    )


def intersect(relation: object) -> RowScope:
    """Intersect the current relation with another relation by position."""
    return _set(relation, operation="intersect", by_name=False)


def intersect_all(relation: object) -> RowScope:
    """Intersect all rows with duplicate-preserving Spark semantics."""
    return _set(relation, operation="intersect_all", by_name=False)


def subtract(relation: object) -> RowScope:
    """Subtract another relation from the current relation."""
    return _set(relation, operation="subtract", by_name=False)


def except_all(relation: object) -> RowScope:
    """Subtract another relation while preserving duplicate counts."""
    return _set(relation, operation="except_all", by_name=False)


def _set(
    relation: object,
    *,
    operation: str,
    by_name: bool,
    allow_missing_columns: bool = False,
    defaults: Mapping[str, object] | None = None,
) -> RowScope:
    context = _context(f"{operation}(...)")
    if not isinstance(relation, InputScope):
        raise TypeError(f"{operation}(relation) requires a Structure relation parameter or transform input")
    if relation._structure_joined_scope is not None:
        raise TypeError(f"{operation}(relation) must be called before that relation is joined")
    _validate_prior_operations(context.operations, function=operation)

    source_schema = _current_schema(context.default_project_source, function=operation)
    default_expressions: tuple[tuple[str, Expression], ...] = ()
    if allow_missing_columns:
        default_expressions = _validate_missing_column_union(
            source_schema,
            relation._structure_input_schema,
            defaults=defaults or {},
            function=operation,
        )
    elif defaults is not None:
        raise TypeError("union_by_name(defaults=...) requires allow_missing_columns=True")
    else:
        _validate_same_schema(source_schema, relation._structure_input_schema, function=operation)

    context.operations.append(
        OperationPlan.relation_set_operation(
            RelationSetPlan(
                operation=operation,
                input_name=relation._structure_input_name,
                source=relation._structure_source,
                schema=relation._structure_input_schema,
                by_name=by_name,
                allow_missing_columns=allow_missing_columns,
                defaults=default_expressions,
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
    context.operations.append(OperationPlan.relation_bound_operation(operation, RelationBoundPlan(count=count)))
    return RowScope(name=_current_scope(context.default_project_source), schema=source_schema)


def _sample_fraction(fraction: float, *, with_replacement: bool) -> float:
    if not isinstance(with_replacement, bool):
        raise TypeError("sample(with_replacement=...) requires a Boolean")
    if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
        raise TypeError("sample(fraction) requires a numeric literal")
    fraction = float(fraction)
    if fraction != fraction:
        raise TypeError("sample(fraction) must be finite")
    if with_replacement:
        if fraction < 0:
            raise TypeError("sample(fraction) must be non-negative when with_replacement=True")
    elif fraction < 0 or fraction > 1:
        raise TypeError("sample(fraction) must be in [0, 1] when with_replacement=False")
    return fraction


def _sample_seed(seed: int | None, *, reproducible: bool) -> int | None:
    if not isinstance(reproducible, bool):
        raise TypeError("sample(reproducible=...) requires a Boolean")
    if seed is None:
        if reproducible:
            raise TypeError("sample(seed=...) is required unless reproducible=False")
        return None
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("sample(seed=...) requires an integer literal")
    return seed


def _orderable(value: object, *, call: str) -> Expression:
    expression = literal(value)
    if expression.kind == "order":
        return expression
    if expression.type is None or expression.type.name not in _ORDERABLE_TYPES:
        raise TypeError(f"{call} order_by requires an orderable scalar expression")
    return expression.asc()


def _field_key(value: object) -> Expression:
    expression = literal(value)
    if expression.kind != "field":
        raise TypeError("select_first_qualified(...) keys must be declared field references")
    return expression


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
        if operation.kind
        in {
            "join",
            "aggregate",
            "selected_rows",
            "explode_struct",
            "explode_outer_struct",
            "inline_struct",
            "inline_outer_struct",
            "posexplode_struct",
            "posexplode_outer_struct",
            "explode_array",
            "explode_outer_array",
            "posexplode_array",
            "posexplode_outer_array",
            "explode_map",
            "explode_outer_map",
            "posexplode_map",
            "posexplode_outer_map",
            "select_first_qualified",
            "hierarchy_closure",
            "hierarchy_fallbacks",
        }
    ]
    if blocked:
        raise TypeError(f"{function}(relation) must be called before shape-changing operation(s): {', '.join(blocked)}")


def _validate_ordered_state(operations, *, function: str) -> None:
    latest_order = _last_index(operations, {"order_by"})
    latest_break = _last_index(
        operations,
        {
            "aggregate",
            "except_all",
            "explode_struct",
            "explode_outer_struct",
            "inline_struct",
            "inline_outer_struct",
            "intersect",
            "intersect_all",
            "join",
            "posexplode_struct",
            "posexplode_outer_struct",
            "explode_array",
            "explode_outer_array",
            "posexplode_array",
            "posexplode_outer_array",
            "sample",
            "selected_rows",
            "select_first_qualified",
            "hierarchy_closure",
            "hierarchy_fallbacks",
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
        f"{function}(relation) requires identical declared schemas; " f"got {left.__name__} and {right.__name__}"
    )


def _validate_missing_column_union(
    left: type[Schema],
    right: type[Schema],
    *,
    defaults: Mapping[str, object],
    function: str,
) -> tuple[tuple[str, Expression], ...]:
    left_fields = left._structure_fields
    right_fields = right._structure_fields
    missing: dict[str, tuple[Any, str]] = {}
    mismatched: list[str] = []
    for name in left_fields.keys() | right_fields.keys():
        left_field = left_fields.get(name)
        right_field = right_fields.get(name)
        if left_field is None:
            missing[name] = (right_field, "left")
        elif right_field is None:
            missing[name] = (left_field, "right")
        elif left_field.column != right_field.column:
            mismatched.append(name)
        elif not _collect_nested_schema_evolution(
            left_field.type,
            right_field.type,
            path=name,
            missing=missing,
        ):
            mismatched.append(name)
    if mismatched:
        names = ", ".join(sorted(mismatched))
        raise TypeError(f"{function}(allow_missing_columns=True) requires matching common field types: {names}")
    normalized: list[tuple[str, Expression]] = []
    for path, value in defaults.items():
        if not isinstance(path, str) or not path or any(not part for part in path.split(".")):
            raise TypeError(f"{function}(defaults=...) requires canonical Structure field paths; got {path!r}")
        if path not in missing:
            raise TypeError(f"{function}(defaults=...) names unknown field path: {path}")
        field = missing[path][0]
        if isinstance(field.type, (ArrayType, MapType)):
            raise TypeError(f"{function}(defaults=...) cannot partially evolve collection or struct field: {path}")
        expression = literal(value)
        type_matches = (expression.type is None and field.nullable) or _same_structure_type(expression.type, field.type)
        if not type_matches or expression.nullable and not field.nullable:
            raise TypeError(
                f"{function}(defaults=...) value for {path} must be a typed literal compatible with {field.type.name}"
            )
        normalized.append((path, expression))
    defaulted = {path for path, _ in normalized}
    required = [path for path, (field, _) in missing.items() if not field.nullable and path not in defaulted]
    if required:
        names = ", ".join(sorted(required))
        raise TypeError(
            f"{function}(allow_missing_columns=True) requires defaults for non-null missing field(s): {names}"
        )
    return tuple(sorted(normalized))


def _collect_nested_schema_evolution(
    left,
    right,
    *,
    path: str,
    missing: dict[str, tuple[Any, str]],
) -> bool:
    if isinstance(left, StructType) and isinstance(right, StructType):
        left_fields = left.schema._structure_fields
        right_fields = right.schema._structure_fields
        for name in left_fields.keys() | right_fields.keys():
            nested_path = f"{path}.{name}"
            left_field = left_fields.get(name)
            right_field = right_fields.get(name)
            if left_field is None:
                missing[nested_path] = (right_field, "left")
            elif right_field is None:
                missing[nested_path] = (left_field, "right")
            elif left_field.column != right_field.column or not _collect_nested_schema_evolution(
                left_field.type,
                right_field.type,
                path=nested_path,
                missing=missing,
            ):
                return False
        return True
    return _same_structure_type(left, right)


def _same_structure_type(left, right) -> bool:
    if left == right or left is right:
        return True
    if left is None or right is None or left.name != right.name:
        return False
    if isinstance(left, DecimalType) and isinstance(right, DecimalType):
        return left.precision == right.precision and left.scale == right.scale
    if isinstance(left, ArrayType) and isinstance(right, ArrayType):
        return left.contains_null == right.contains_null and _same_structure_type(left.element, right.element)
    if isinstance(left, MapType) and isinstance(right, MapType):
        return (
            left.value_contains_null == right.value_contains_null
            and _same_structure_type(left.key, right.key)
            and _same_structure_type(left.value, right.value)
        )
    if isinstance(left, StructType) and isinstance(right, StructType):
        left_fields, right_fields = left.schema._structure_fields, right.schema._structure_fields
        return tuple(left_fields) == tuple(right_fields) and all(
            left_fields[name].column == right_fields[name].column
            and left_fields[name].nullable == right_fields[name].nullable
            and _same_structure_type(left_fields[name].type, right_fields[name].type)
            for name in left_fields
        )
    return True


def _field_signature(field) -> tuple[str, str, object, bool]:
    return field.name, field.column, field.type, field.nullable


def _validate_closure_schema(
    schema: type[Schema],
    *,
    id_expression: Expression,
    parent_expression: Expression,
    node: str,
    ancestor: str,
    depth: str,
) -> None:
    for name, option in ((node, "node"), (ancestor, "ancestor"), (depth, "depth")):
        if not isinstance(name, str) or not name:
            raise TypeError(f"hierarchy_closure({option}=...) requires a non-empty field name")
        if name not in schema._structure_fields:
            raise TypeError(f"hierarchy_closure(as_=...) schema must declare {option} field {name!r}")
    node_type = schema._structure_fields[node].type
    ancestor_type = schema._structure_fields[ancestor].type
    depth_type = schema._structure_fields[depth].type
    if id_expression.nullable:
        raise TypeError("hierarchy_closure(id) requires a non-null declared field")
    if parent_expression.type != id_expression.type:
        raise TypeError("hierarchy_closure(parent=...) must have the same type as id")
    if node_type != id_expression.type:
        raise TypeError(f"hierarchy_closure(node={node!r}) field must match the id field type")
    if ancestor_type != id_expression.type:
        raise TypeError(f"hierarchy_closure(ancestor={ancestor!r}) field must match the id field type")
    if not isinstance(depth_type, LongType):
        raise TypeError(f"hierarchy_closure(depth={depth!r}) field must be long()")


def _default_scope(schema: type[Schema]) -> str:
    name = schema.__name__
    return name[:1].lower() + name[1:]


def _validate_fallback_schema(
    schema: type[Schema],
    *,
    source_expression: Expression,
    path_expression: Expression,
    parent_id_expression: Expression,
    parent_expression: Expression,
    source: str,
    fallback: str,
    ordinal: str,
) -> None:
    for name, option in ((source, "source"), (fallback, "fallback"), (ordinal, "ordinal")):
        if not isinstance(name, str) or not name:
            raise TypeError(f"hierarchy_fallbacks({option}=...) requires a non-empty field name")
        if name not in schema._structure_fields:
            raise TypeError(f"hierarchy_fallbacks(as_=...) schema must declare {option} field {name!r}")
    if source_expression.nullable:
        raise TypeError("hierarchy_fallbacks(source_id) requires a non-null declared field")
    if path_expression.nullable:
        raise TypeError("hierarchy_fallbacks(path) requires a non-null array expression")
    if parent_id_expression.nullable:
        raise TypeError("hierarchy_fallbacks(parent_id=...) requires a non-null declared field")
    if parent_expression.type != parent_id_expression.type:
        raise TypeError("hierarchy_fallbacks(parent=...) must have the same type as parent_id")
    if not isinstance(path_expression.type, ArrayType) or path_expression.type.contains_null:
        raise TypeError("hierarchy_fallbacks(path) requires an array with contains_null=False")
    if path_expression.type.element != parent_id_expression.type:
        raise TypeError("hierarchy_fallbacks(path) element type must match parent_id")
    fields = schema._structure_fields
    if fields[source].type != source_expression.type or fields[source].nullable:
        raise TypeError(f"hierarchy_fallbacks(source={source!r}) field must match the non-null source_id type")
    if not isinstance(fields[fallback].type, StringType) or not fields[fallback].nullable:
        raise TypeError(f"hierarchy_fallbacks(fallback={fallback!r}) field must be nullable string()")
    if not isinstance(fields[ordinal].type, LongType) or fields[ordinal].nullable:
        raise TypeError(f"hierarchy_fallbacks(ordinal={ordinal!r}) field must be non-null long()")
