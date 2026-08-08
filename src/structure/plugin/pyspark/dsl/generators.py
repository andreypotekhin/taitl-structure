"""PySpark generator helpers that expose collection rows as Structure scopes."""

from __future__ import annotations

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.logic import CapturePySparkGenerator
from structure.plugin.pyspark.dsl.RowScope import RowScope

_generators = CapturePySparkGenerator()


def explode_struct(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    """Explode an array of structs and expose each element as ``as_``.

    Args:
        value: Array-of-struct expression to explode.
        as_: Schema describing the generated element row.
        scope: Optional field-access scope name for the generated row.

    Returns:
        A row scope for the generated element.

    Example:
        ``item = explode_struct(order.items, as_=OrderItem)`` exposes
        ``item.sku`` and other ``OrderItem`` fields to later expressions.
    """
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("explode_struct(...) can only be used inside a compiled Structure step method")
    return _generators.explode_struct(context, value, as_=as_, scope=scope)


def explode_outer_struct(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    """Explode an array of structs while preserving rows with null or empty arrays.

    Args:
        value: Array-of-struct expression to explode.
        as_: Schema describing the generated element row.
        scope: Optional field-access scope name for the generated row.

    Returns:
        A row scope for the generated element. Generated fields are nullable
        because Spark ``explode_outer`` can produce an empty row.

    Example:
        ``item = explode_outer_struct(order.items, as_=OrderItem)`` keeps the
        order row even when ``items`` is null or empty.
    """
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("explode_outer_struct(...) can only be used inside a compiled Structure step method")
    return _generators.explode_outer_struct(context, value, as_=as_, scope=scope)


def inline_struct(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    """Inline an array of structs into generated columns described by ``as_``.

    Args:
        value: Array-of-struct expression to inline.
        as_: Schema describing the generated columns.
        scope: Optional field-access scope name for the generated row.

    Returns:
        A row scope exposing the inlined struct fields.

    Example:
        ``line = inline_struct(order.lines, as_=OrderLine)`` mirrors Spark
        ``inline`` while preserving Structure field names.
    """
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("inline_struct(...) can only be used inside a compiled Structure step method")
    return _generators.inline_struct(context, value, as_=as_, scope=scope)


def inline_outer_struct(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    """Inline an array of structs while preserving rows with null or empty arrays.

    Args:
        value: Array-of-struct expression to inline.
        as_: Schema describing the generated columns.
        scope: Optional field-access scope name for the generated row.

    Returns:
        A nullable row scope for the inlined fields.

    Example:
        ``line = inline_outer_struct(order.lines, as_=OrderLine)`` mirrors Spark
        ``inline_outer`` for nullable or empty arrays.
    """
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("inline_outer_struct(...) can only be used inside a compiled Structure step method")
    return _generators.inline_outer_struct(context, value, as_=as_, scope=scope)


def posexplode_struct(
    value: object,
    *,
    as_: type[Schema],
    ordinal: str = "ordinal",
    scope: str | None = None,
) -> RowScope:
    """Explode an array of structs and include a non-null ordinal field.

    Args:
        value: Array-of-struct expression to explode.
        as_: Schema describing the generated element row.
        ordinal: Field in ``as_`` that receives Spark's zero-based position.
        scope: Optional field-access scope name for the generated row.

    Returns:
        A row scope for the generated element and ordinal.

    Example:
        ``item = posexplode_struct(order.items, as_=PositionedItem)`` mirrors
        Spark ``posexplode`` with typed access to ``item.ordinal``.
    """
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("posexplode_struct(...) can only be used inside a compiled Structure step method")
    return _generators.posexplode_struct(context, value, as_=as_, ordinal=ordinal, scope=scope)


def posexplode_outer_struct(
    value: object,
    *,
    as_: type[Schema],
    ordinal: str = "ordinal",
    scope: str | None = None,
) -> RowScope:
    """Outer variant of :func:`posexplode_struct` for null or empty arrays.

    Args:
        value: Array-of-struct expression to explode.
        as_: Schema describing the generated element row.
        ordinal: Field in ``as_`` that receives Spark's zero-based position.
        scope: Optional field-access scope name for the generated row.

    Returns:
        A nullable row scope for the generated element and ordinal.

    Example:
        ``item = posexplode_outer_struct(order.items, as_=PositionedItem)``
        mirrors Spark ``posexplode_outer``.
    """
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("posexplode_outer_struct(...) can only be used inside a compiled Structure step method")
    return _generators.posexplode_outer_struct(context, value, as_=as_, ordinal=ordinal, scope=scope)


def explode_array(
    value: object,
    *,
    as_: type[Schema],
    value_field: str,
    scope: str | None = None,
) -> RowScope:
    """Explode an array of primitive values into a typed scalar row."""
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("explode_array(...) can only be used inside a compiled Structure step method")
    return _generators.explode_array(context, value, as_=as_, value_field=value_field, scope=scope)


def explode_outer_array(
    value: object,
    *,
    as_: type[Schema],
    value_field: str,
    scope: str | None = None,
) -> RowScope:
    """Explode a primitive array while preserving a row for null or empty input."""
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("explode_outer_array(...) can only be used inside a compiled Structure step method")
    return _generators.explode_outer_array(context, value, as_=as_, value_field=value_field, scope=scope)


def posexplode_array(
    value: object,
    *,
    as_: type[Schema],
    value_field: str,
    ordinal: str = "ordinal",
    scope: str | None = None,
) -> RowScope:
    """Explode a primitive array and expose its zero-based ordinal."""
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("posexplode_array(...) can only be used inside a compiled Structure step method")
    return _generators.posexplode_array(context, value, as_=as_, value_field=value_field, ordinal=ordinal, scope=scope)


def posexplode_outer_array(
    value: object,
    *,
    as_: type[Schema],
    value_field: str,
    ordinal: str = "ordinal",
    scope: str | None = None,
) -> RowScope:
    """Outer variant of :func:`posexplode_array`."""
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("posexplode_outer_array(...) can only be used inside a compiled Structure step method")
    return _generators.posexplode_outer_array(
        context, value, as_=as_, value_field=value_field, ordinal=ordinal, scope=scope
    )


def variant_explode(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    """Expand a Variant object or array with Spark's ``variant_explode`` TVF."""
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("variant_explode(...) can only be used inside a compiled Structure step method")
    return _generators.variant_explode(context, value, as_=as_, scope=scope)


def variant_explode_outer(
    value: object,
    *,
    as_: type[Schema],
    scope: str | None = None,
) -> RowScope:
    """Expand a Variant while preserving one null row for null or empty input."""
    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("variant_explode_outer(...) can only be used inside a compiled Structure step method")
    return _generators.variant_explode_outer(context, value, as_=as_, scope=scope)
