"""Typed SQL relation operations for compiled PySpark transforms."""

from __future__ import annotations

import re
from collections.abc import Mapping
from itertools import count
from typing import Any

from structure.dsl import Schema
from structure.plugin.api.v1.model import current_symbolic_context
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan
from structure.plugin.pyspark.dsl.operations.SqlPlan import SqlPlan
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.SqlResult import SqlCommandResult, SqlResult

_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SCOPES = count(1)


def sql(
    statement: str,
    *,
    relations: Mapping[str, object] | None = None,
    args: Mapping[str, object] | list[object] | None = None,
    as_: type[Schema],
    label: str | None = None,
) -> RowScope:
    """Record typed Spark SQL inside a compiled Transform method.

    String relation values are inserted into the SQL text as caller-owned
    relation expressions. Row scopes become Spark DataFrame bindings.
    """
    if not isinstance(statement, str) or not statement.strip():
        raise TypeError("sql(statement, ...) requires non-empty SQL text")
    if not isinstance(as_, type) or not issubclass(as_, Schema):
        raise TypeError("sql(..., as_=...) requires a Structure Schema class")
    if as_ is SqlResult:
        raise TypeError("sql(..., as_=SqlResult) is abstract; choose a concrete result Schema")
    if issubclass(as_, SqlResult) and not issubclass(as_, SqlCommandResult):
        raise TypeError(f"sql(..., as_={as_.__name__}) requires a provider SQL result adapter")
    is_command = issubclass(as_, SqlCommandResult)
    if label is not None and (not isinstance(label, str) or not label.strip()):
        raise TypeError("sql(..., label=...) requires a non-empty string when supplied")
    if not is_command and label is not None:
        raise TypeError("sql(..., label=...) is only valid with SqlCommandResult")

    context = current_symbolic_context()
    if context is None:
        raise RuntimeError("sql(...) can only be used while compiling a Structure Transform method")
    if relations is not None and not isinstance(relations, Mapping):
        raise TypeError("sql(..., relations=...) must be a mapping")
    bindings = dict(relations or {})
    placeholders = set(_PLACEHOLDER.findall(statement))
    if placeholders != set(bindings):
        missing = sorted(placeholders - set(bindings))
        unused = sorted(set(bindings) - placeholders)
        details = []
        if missing:
            details.append(f"unbound placeholder(s): {', '.join(missing)}")
        if unused:
            details.append(f"unused relation key(s): {', '.join(unused)}")
        raise ValueError("sql(...) relation placeholders must match relations=: " + "; ".join(details))

    sql_relations: list[tuple[str, str]] = []
    for name, relation in bindings.items():
        if not isinstance(name, str) or not name.isidentifier():
            raise TypeError("sql(..., relations=...) keys must be Python identifiers")
        if isinstance(relation, str):
            statement = statement.replace("{" + name + "}", relation)
            continue
        if not isinstance(relation, RowScope):
            raise TypeError(f"sql(..., relations[{name!r}]) requires a Structure row scope or table-name string")
        scope_name = relation._structure_scope_name
        sql_relations.append((name, scope_name))

    if args is not None and not isinstance(args, (Mapping, list)):
        raise TypeError("sql(..., args=...) must be a mapping or list")
    values = args.values() if isinstance(args, Mapping) else (() if args is None else args)
    if any(isinstance(value, Expression) for value in values):
        raise TypeError("sql(..., args=...) accepts literal parameter values, not row expressions")
    scope = f"_sql_result_{next(_SCOPES)}"
    context.operations.append(
        OperationPlan.sql_operation(
            SqlPlan(statement, tuple(sql_relations), args, as_, label if is_command else None, scope)
        )
    )
    context.register_current_scope(scope)
    return RowScope(name=scope, schema=as_)
