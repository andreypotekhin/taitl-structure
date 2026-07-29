"""Pipe bindings used by ``@step(inout=...)`` and ``@raw(inout=...)``."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InOutBinding:
    """A paired input/output binding created with declaration pipes.

    Example:
        @step(inout=orders | enriched_orders)
        def enrich(self, order):
            return EnrichedOrder.project(order)
    """

    inputs: tuple[object, ...]
    outputs: tuple[object, ...]


def bind_inout(inputs: object, outputs: object) -> InOutBinding:
    """Bind one or more input-side declarations to output-side declarations."""
    return InOutBinding(_side(inputs), _side(outputs))


def _side(value: object) -> tuple[object, ...]:
    if isinstance(value, (str, bytes)):
        raise TypeError("inout pipe sides require declarations, not strings")
    if isinstance(value, (list, tuple)):
        if not value:
            raise TypeError("inout pipe sides require at least one declaration")
        return tuple(value)
    return (value,)
