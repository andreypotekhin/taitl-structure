"""Immutable symbolic payloads for Delta data mutations."""

from dataclasses import dataclass

from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class DeltaClause:
    action: str
    condition: Expression | None = None
    assignments: tuple[tuple[str, Expression], ...] = ()


@dataclass(frozen=True)
class DeltaMutation:
    kind: str
    target: str
    target_scope: str
    predicate: Expression
    assignments: tuple[tuple[str, Expression], ...] = ()
    source: str | None = None
    source_scope: str | None = None
    clauses: tuple[DeltaClause, ...] = ()
