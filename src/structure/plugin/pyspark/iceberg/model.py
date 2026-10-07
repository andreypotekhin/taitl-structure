"""Immutable capture payloads for Iceberg table operations."""

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class IcebergClause:
    action: str
    condition: Expression | None = None
    assignments: tuple[tuple[str, Expression], ...] = ()


@dataclass(frozen=True)
class IcebergMutation:
    kind: str
    target: str
    target_scope: str
    predicate: Expression | None = None
    assignments: tuple[tuple[str, Expression], ...] = ()
    source: str | None = None
    source_scope: str | None = None
    clauses: tuple[IcebergClause, ...] = ()
    schema_evolution: bool = False
    output: str | None = None
    output_schema: type[Schema] | None = None
    selector: Expression | None = None
    action: str | None = None
    procedure_args: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True)
class IcebergMutationResult:
    mutation: IcebergMutation
