"""Caller-bound Iceberg table declarations."""

from structure.dsl import InputDeclaration, OutputDeclaration, Schema


def iceberg_input(schema: type[Schema]) -> InputDeclaration:
    """Declare a read-only Iceberg table supplied by catalog name at invocation time."""
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError("iceberg_input(...) requires a Structure Schema class")
    return InputDeclaration(schema=schema, binding="iceberg")


def iceberg_output(schema: type[Schema]) -> OutputDeclaration:
    """Declare the output shape of an explicitly evolving Iceberg append."""
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError("iceberg_output(...) requires a Structure Schema class")
    return OutputDeclaration(schema=schema, binding="iceberg")


def iceberg_table(schema: type[Schema]) -> OutputDeclaration:
    """Declare a caller-bound Iceberg table that may be read and mutated in place."""
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError("iceberg_table(...) requires a Structure Schema class")
    return OutputDeclaration(schema=schema, binding="iceberg_table")
