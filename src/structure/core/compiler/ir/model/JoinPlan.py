from __future__ import annotations

from dataclasses import dataclass

from structure.core.compiler.ir.model.JoinMethod import JoinMethod
from structure.core.dsl.model.expr.Expression import Expression
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.Join import Join
from structure.core.dsl.model.transforms.JoinAsOf import JoinAsOf
from structure.core.dsl.model.transforms.JoinDedupe import JoinDedupe
from structure.core.dsl.model.transforms.JoinHint import JoinHint
from structure.core.dsl.model.transforms.JoinStrategy import JoinStrategy
from structure.core.dsl.model.transforms.JoinTemporal import JoinTemporal


@dataclass(frozen=True)
class JoinPlan:
    input_name: str
    source: str
    input_schema: type[Schema]
    predicate: Expression
    how: Join
    hint: JoinHint | None = None
    strategy: JoinStrategy | None = None
    method: JoinMethod = JoinMethod.LOOKUP
    dedupe: JoinDedupe | None = None
    temporal: JoinTemporal | None = None
    as_of: JoinAsOf | None = None
