"""Immutable typed query over one unjoined PySpark relation input."""

from __future__ import annotations

from dataclasses import dataclass, replace
from functools import cached_property
from typing import Iterable, cast

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression, is_orderable_type
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import BooleanType


@dataclass(frozen=True)
class RelationQuery:
    """An immutable sequence of typed operations over a single relation."""

    relation: object
    operations: tuple[tuple[str, object], ...] = ()
    ancestors: tuple[RelationQuery, ...] = ()

    def __post_init__(self) -> None:
        context = current_symbolic_context()
        if context is not None and hasattr(context, "register_relation_query"):
            context.register_relation_query(self)

    @property
    def source_name(self) -> str:
        return str(getattr(self.relation, "_structure_input_name"))

    @property
    def source_schema(self) -> type[Schema]:
        return getattr(self.relation, "_structure_input_schema")

    @property
    def filters(self) -> tuple[Expression, ...]:
        return tuple(cast(Expression, value) for kind, value in self.operations if kind == "where")

    @property
    def projection(self) -> type[Schema] | tuple[str, ...] | None:
        return next(
            (cast(type[Schema] | tuple[str, ...], value) for kind, value in reversed(self.operations) if kind == "project"),
            None,
        )

    @property
    def orderings(self) -> tuple[Expression, ...]:
        return tuple(
            cast(Expression, order)
            for kind, values in self.operations
            if kind == "order_by"
            for order in cast(tuple[Expression, ...], values)
        )

    @property
    def bound(self) -> int | None:
        return next((cast(int, value) for kind, value in reversed(self.operations) if kind == "limit"), None)

    @cached_property
    def output_schema(self) -> type[Schema]:
        schema = self.source_schema
        for kind, target in self.operations:
            if kind == "project":
                schema = self._projected_schema(schema, target)
        return schema

    @property
    def output_fields(self):
        return tuple(self.output_schema._structure_fields.values())

    @property
    def output_scope(self) -> str:
        return f"{self.source_name}__query"

    def where(self, *predicates: object) -> RelationQuery:
        """Append predicates to this query without changing its source scope."""
        if not predicates:
            raise TypeError("query.where(...) requires at least one boolean Structure expression")
        expressions = tuple(literal(predicate) for predicate in predicates)
        if any(not isinstance(expression.type, BooleanType) for expression in expressions):
            raise TypeError("query.where(...) requires boolean Structure expressions")
        query = self
        for expression in expressions:
            query = query._append("where", expression)
        return query

    def filter(self, *predicates: object) -> RelationQuery:
        """Alias for :meth:`where`."""
        return self.where(*predicates)

    def project(self, target: type[Schema] | Iterable[str]) -> RelationQuery:
        """Set the query's declared schema or selected source fields."""
        if isinstance(target, type):
            if not issubclass(target, Schema):
                raise TypeError("query.project(target) requires a Schema class or field-name iterable")
            _validate_schema_projection(self.output_schema, target)
            return self._append("project", target)
        if isinstance(target, str):
            raise TypeError("query.project(fields) requires an iterable of field names, not one string")
        names = tuple(target)
        source_schema = self.output_schema
        if not names or any(not isinstance(name, str) for name in names):
            raise TypeError("query.project(fields) requires one or more field names")
        unknown = [name for name in names if name not in source_schema._structure_fields]
        if unknown:
            raise TypeError(f"query.project(...) references unknown source fields: {', '.join(unknown)}")
        if len(names) != len(set(names)):
            raise TypeError("query.project(fields) does not allow duplicate field names")
        return self._append("project", names)

    def order_by(self, *orderings: object) -> RelationQuery:
        """Append order expressions for a later query limit."""
        if not orderings:
            raise TypeError("query.order_by(...) requires at least one order expression")
        expressions = tuple(literal(ordering) for ordering in orderings)
        for expression in expressions:
            base = expression.args[0] if expression.kind == "order" else expression
            if not is_orderable_type(base.type):
                raise TypeError("query.order_by(...) requires orderable Structure expressions")
        return self._append("order_by", expressions)

    def limit(self, count: int) -> RelationQuery:
        """Set an ordered, non-negative row bound."""
        if not self.orderings:
            raise TypeError("query.limit(...) requires a preceding query.order_by(...)")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise TypeError("query.limit(count) requires a non-negative integer literal, excluding booleans")
        return self._append("limit", count)

    def exists(self) -> Expression:
        """Create a native-shaped Boolean EXISTS expression."""
        self._consume()
        return Expression(kind="query_exists", type=BooleanType(), nullable=False, data={"query": self})

    def __getattr__(self, name: str) -> Expression:
        schema = self.output_schema
        field = next((item for item in self.output_fields if item.name == name or item.column == name), None)
        if field is None:
            raise AttributeError(name)
        return RowScope(name=self.output_scope, schema=schema)._field(field.name, field)

    def _derive(self, **changes) -> RelationQuery:
        return replace(self, ancestors=(self, *self.ancestors), **changes)

    def _append(self, kind: str, value: object) -> RelationQuery:
        return self._derive(operations=(*self.operations, (kind, value)))

    @staticmethod
    def _projected_schema(source: type[Schema], target: object) -> type[Schema]:
        if isinstance(target, type) and issubclass(target, Schema):
            return target
        declarations = {name: getattr(source, name) for name in cast(tuple[str, ...], target)}
        return type(f"{source.__name__}QueryProjection", (Schema,), declarations)

    def _consume(self, *, correlation_allowed: bool = True) -> None:
        context = current_symbolic_context()
        if not correlation_allowed and any(
            expression.kind == "outer"
            for _, value in self.operations
            if isinstance(value, Expression)
            for expression in _walk_expression(value)
        ):
            raise TypeError("isin(query) supports uncorrelated queries only; remove .outer() references")
        if context is not None:
            from structure.plugin.pyspark.dsl.InputScope import InputScope, _validate_query_scopes

            source = cast(InputScope, self.relation)
            _validate_query_scopes(self, literal(True), context, source)
        if context is not None and hasattr(context, "consume_relation_query"):
            context.consume_relation_query(self)


def _walk_expression(expression: Expression):
    yield expression
    for argument in expression.args:
        yield from _walk_expression(argument)


def relation_query(relation: object) -> RelationQuery:
    """Create a query descriptor from a named unjoined input relation."""
    if not hasattr(relation, "_structure_input_name"):
        raise TypeError("relation queries require an unjoined Structure input relation")
    if getattr(relation, "_structure_joined_scope", None) is not None:
        raise TypeError("relation queries must start from an unjoined input; use its existing joined scope directly")
    return RelationQuery(relation=relation)


def _validate_schema_projection(source: type[Schema], target: type[Schema]) -> None:
    source_fields = source._structure_fields
    for target_field in target._structure_fields.values():
        source_field = source_fields.get(target_field.name)
        if source_field is None:
            source_field = next((field for field in source_fields.values() if field.column == target_field.column), None)
        if source_field is None:
            raise TypeError(
                f"query.project({target.__name__}) requires field {target_field.name!r} from "
                f"{source.__name__}; use a supported source field or field subset"
            )
        if target_field.type != source_field.type:
            raise TypeError(
                f"query.project({target.__name__}) field {target_field.name!r} has type "
                f"{target_field.type.name}, but source field {source_field.name!r} has type {source_field.type.name}"
            )
        if source_field.nullable and not target_field.nullable:
            raise TypeError(
                f"query.project({target.__name__}) cannot narrow nullable source field "
                f"{source_field.name!r}; declare the target field nullable or filter nulls first"
            )
