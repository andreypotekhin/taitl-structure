"""Alias validation for transform declarations and invocation outputs."""

from __future__ import annotations

import keyword
from dataclasses import replace
from typing import Any, TypeVar, cast

Declaration = TypeVar("Declaration")


def alias(declaration: Declaration, names: tuple[str, ...]) -> Declaration:
    """Return a declaration with additional public aliases.

    Args:
        declaration: Input, lane, or output declaration.
        names: Public Python identifiers accepted as alternate names.

    Returns:
        A copy of the declaration with aliases appended.

    Example:
        customers = input(Customer).alias("customer_rows")
    """
    if not names:
        raise TypeError("alias(...) requires at least one name")
    existing = tuple(getattr(declaration, "aliases", ()))
    aliases = (*existing, *tuple(_alias(name) for name in names))
    if len(set(aliases)) != len(aliases):
        raise TypeError("alias(...) cannot repeat a name")
    return cast(Declaration, replace(cast(Any, declaration), aliases=aliases))


def require_alias(name: str) -> str:
    """Validate and return one public alias name."""
    return _alias(name)


def _alias(name: str) -> str:
    if not isinstance(name, str) or not name:
        raise TypeError("alias(...) names must be non-empty strings")
    if name.startswith("_") or not name.isidentifier() or keyword.iskeyword(name):
        raise TypeError(f"alias(...) name {name!r} must be a public Python identifier")
    return name
