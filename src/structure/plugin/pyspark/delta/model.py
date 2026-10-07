"""Immutable symbolic payloads for Delta data mutations."""

from dataclasses import dataclass

from structure.dsl import Schema
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
    predicate: Expression | None = None
    assignments: tuple[tuple[str, Expression], ...] = ()
    source: str | None = None
    source_scope: str | None = None
    clauses: tuple[DeltaClause, ...] = ()
    schema_evolution: bool = False
    output: str | None = None
    output_schema: type[Schema] | None = None
    selector: Expression | None = None
    end_selector: Expression | None = None
    selector_type: str | None = None
    action: str | None = None
    columns: tuple[str, ...] = ()
    allow_short_retention: bool = False
    procedure_args: tuple[tuple[str, object], ...] = ()


@dataclass(frozen=True)
class DeltaMutationResult:
    """Relation-like symbolic result marking an evolving table output."""

    mutation: DeltaMutation
