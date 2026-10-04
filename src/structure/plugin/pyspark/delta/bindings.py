"""Caller-bound Delta table declarations for transforms."""

from structure.dsl import InputDeclaration, OutputDeclaration, Schema


def delta_input(schema: type[Schema]) -> InputDeclaration:
    """Declare a read-only Delta table supplied when a transform is invoked."""
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError("delta_input(...) requires a Structure Schema class")
    return InputDeclaration(schema=schema, binding="delta")


def delta_output(schema: type[Schema]) -> OutputDeclaration:
    """Declare a mutable Delta table supplied by the caller and returned by identity."""
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError("delta_output(...) requires a Structure Schema class")
    return OutputDeclaration(schema=schema, binding="delta")
