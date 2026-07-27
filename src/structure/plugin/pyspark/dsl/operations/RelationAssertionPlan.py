from __future__ import annotations

from dataclasses import dataclass

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression


@dataclass(frozen=True)
class RelationAssertionPlan:
    operation: str
    keys: tuple[Expression, ...] = ()
    predicate: Expression | None = None
    value: Expression | None = None
    reference_input: str | None = None
    reference_source: str | None = None
    reference_schema: type[Schema] | None = None
    reference_key: Expression | None = None
    nulls: str = "allow"
