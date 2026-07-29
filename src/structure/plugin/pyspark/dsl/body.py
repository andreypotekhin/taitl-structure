"""Step-body helpers for filtering, watermarking, and projection.

These helpers are intentionally small because they sit in user-authored
transform bodies.  Each call records symbolic operations in the active compile
context; outside compilation they fail early with guidance instead of producing
partially useful objects.
"""

from __future__ import annotations

from re import fullmatch
from typing import Any, Iterable, TypeVar, cast, overload

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.joins import JoinPlan
from structure.plugin.pyspark.dsl.model.Projection import Projection
from structure.plugin.pyspark.dsl.operations import OperationPlan, WatermarkPlan
from structure.plugin.pyspark.dsl.types import BooleanType, TimestampType

Projected = TypeVar("Projected", bound=Schema)


def where(*predicates: object) -> WhereChain:
    """Filter the current step with one or more boolean predicates.

    Args:
        predicates: Boolean Structure expressions. Existence-join expressions
            are accepted and converted into join operations.

    Returns:
        A chain object that supports ``.where(...)`` and ``.project(...)``.

    Raises:
        RuntimeError: The helper was called outside symbolic compilation.
        TypeError: Any predicate is missing or not boolean.

    Example:
        def publish(self, order):
            return where(order.total > 0).project(order, PublishedOrder)
    """
    context = _context("where")
    if not predicates:
        raise TypeError("where(...) requires at least one boolean Structure expression")
    for position, predicate in enumerate(predicates, start=1):
        expression = literal(predicate)
        if not isinstance(expression.type, BooleanType):
            raise TypeError(f"where(...) requires a boolean Structure expression for predicate {position}")
        if expression.kind == "existence_join" and expression.data is not None:
            join = cast(JoinPlan, expression.data["join"])
            context.joins.append(join)
            context.operations.append(OperationPlan.join_operation(join))
            continue
        context.filters.append(expression)
        context.operations.append(OperationPlan.filter_operation(expression))
    return WhereChain()


def watermark(field: object, *, delay: str = "10 minutes") -> None:
    """Declare Spark event-time watermarking for a timestamp field.

    Args:
        field: Timestamp Structure field expression.
        delay: Non-negative fixed Spark interval text such as ``"10 minutes"``.

    Example:
        def publish(self, event):
            watermark(event.created_at, delay="30 minutes")
            return project(event, PublishedEvent)
    """
    context = _context("watermark")
    expression = literal(field)
    if expression.kind != "field":
        raise TypeError("watermark(...) requires a Structure field expression")
    if not isinstance(expression.type, TimestampType):
        raise TypeError("watermark(...) requires a Timestamp Structure field expression")
    if not isinstance(delay, str) or not fullmatch(
        r"\s*\d+(?:\.\d+)?\s+(?:microseconds?|milliseconds?|seconds?|minutes?|hours?|days?|weeks?)\s*", delay
    ):
        raise TypeError(
            "watermark(delay=...) requires a non-negative fixed Spark interval string, such as '10 minutes'"
        )
    context.operations.append(
        OperationPlan.watermark_operation(WatermarkPlan(expression=expression, delay=delay.strip()))
    )


@overload
def project(source: object, target: type[Projected]) -> Projected:
    """Project a source row into a target Structure schema."""
    ...


@overload
def project(source: object, target: Iterable[str]) -> Any:
    """Project a source row to an explicit field-name subset."""
    ...


@overload
def project(source: object) -> Any:
    """Project the current default row source."""
    ...


def project(source: object | None = None, target: type[Schema] | Iterable[str] | None = None) -> object:
    """Project a source row into a schema or explicit field subset.

    Args:
        source: Row-like object to project. When omitted in a compiled context,
            Structure uses the current default row scope.
        target: Output schema class or iterable of field names.

    Returns:
        A symbolic projection consumed by the compiler.

    Example:
        def publish(self, order):
            return project(order, PublishedOrder)
    """
    context = current_symbolic_context()
    if target is None:
        if context is not None and isinstance(source, type) and issubclass(source, Schema):
            return Projection(sources=(_default_project_source(context),), target=source)
        if context is not None and source is not None:
            return Projection(sources=(_default_project_source(context),), fields=_project_fields(source))
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    if isinstance(source, type) and issubclass(source, Schema):
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    if source is None:
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    if isinstance(target, type):
        if not issubclass(target, Schema):
            raise TypeError("project(source, target) requires a Schema class target")
        return Projection(sources=(source,), target=target)
    return Projection(sources=(source,), fields=_project_fields(target))


class WhereChain:
    """Fluent continuation returned by ``where(...)``."""

    def where(self, *predicates: object) -> WhereChain:
        """Append more filters to the current step body."""
        return where(*predicates)

    def project(
        self,
        source: object | None = None,
        target: type[Schema] | Iterable[str] | None = None,
    ) -> object:
        """Finish a filtered chain with ``project(...)``."""
        return project(source) if target is None else project(source, target)


def _context(function: str):
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError(f"{function}(...) can only be used inside a compiled Structure step method")
    return context


def _default_project_source(context) -> object:
    if context.default_project_source is None:
        raise TypeError("project(...) requires a source row first, such as project(order, OrderPublished)")
    return context.default_project_source


def _project_fields(value: object) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)):
        raise TypeError("project(source, fields) requires a non-empty field sequence")
    try:
        fields = tuple(cast(Iterable[object], value))
    except TypeError as error:
        raise TypeError("project(source, fields) requires a non-empty field sequence") from error
    if not fields:
        raise TypeError("project(source, fields) requires at least one field")
    if not all(isinstance(field, str) for field in fields):
        raise TypeError("project(source, fields) requires field names as strings")
    if len(set(fields)) != len(fields):
        raise TypeError("project(source, fields) cannot repeat field names")
    return cast(tuple[str, ...], fields)
