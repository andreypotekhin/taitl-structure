"""Compiler-visible, typed Delta data mutations."""

from __future__ import annotations

from structure.dsl import Schema
from structure.plugin.pyspark.delta.model import DeltaClause, DeltaMutation
from structure.plugin.pyspark.dsl.Expression import Expression, _same_type
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import BooleanType
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import current_pyspark_context


class DeltaScope(InputScope):
    def __init__(self, *, name: str, schema: type[Schema], source: str, mutable: bool) -> None:
        super().__init__(name=name, schema=schema, source=source)
        self._structure_delta_mutable = mutable


def _context():
    context = current_pyspark_context()
    if context is None:
        raise RuntimeError("Delta mutation helpers can only be used inside a compiled Structure step")
    return context


def _target(target: object) -> DeltaScope:
    if not isinstance(target, DeltaScope) or not target._structure_delta_mutable:
        raise TypeError("Delta mutation target must be a delta_output(...) relation parameter")
    return target


def _predicate(name: str, value: object) -> Expression:
    expression = literal(value)
    if not isinstance(expression.type, BooleanType):
        raise TypeError(f"{name} requires a Boolean Structure expression; use True for all rows")
    return expression


def _scopes(expression: Expression) -> set[str]:
    scopes = set().union(*(_scopes(argument) for argument in expression.args))
    if expression.kind == "field":
        scopes.add(str((expression.data or {})["scope"]))
    return scopes


def _visible(expression: Expression, *, allowed: set[str], action: str) -> None:
    extra = _scopes(expression) - allowed
    if extra:
        raise TypeError(f"{action} references unavailable relation scope(s): {', '.join(sorted(extra))}")


def _assignments(target: DeltaScope, values: object, *, insert: bool = False) -> tuple[tuple[str, Expression], ...]:
    schema = target._structure_input_schema
    if not isinstance(values, schema):
        raise TypeError(f"Delta assignments require {schema.__name__}(...) values")
    supplied = values._structure_values
    if not supplied:
        raise TypeError("Delta assignments must contain at least one target field")
    if insert:
        missing = [
            field.name
            for field in schema._structure_fields.values()
            if not field.nullable and field.name not in supplied
        ]
        if missing:
            raise TypeError(f"Delta insert is missing non-nullable target fields: {', '.join(missing)}")
    assignments = []
    for name, value in supplied.items():
        field = schema._structure_fields[name]
        expression = literal(value)
        if expression.type is not None and not _same_type(expression.type, field.type):
            raise TypeError(f"Delta assignment {name} expects {field.type.name}, got {expression.type.name}")
        if expression.nullable and not field.nullable:
            raise TypeError(f"Delta assignment {name} may be null, but the target field is non-nullable")
        assignments.append((field.column, expression))
    return tuple(assignments)


def delta_delete(target: DeltaScope, *, where: object) -> None:
    target = _target(target)
    predicate = _predicate("delta_delete(where=...)", where)
    _visible(predicate, allowed={target._structure_scope_name}, action="delta_delete")
    _context().delta_mutations.append(
        DeltaMutation("delete", target._structure_source, target._structure_scope_name, predicate)
    )


def delta_update(target: DeltaScope, *, where: object, set: Schema) -> None:
    target = _target(target)
    predicate = _predicate("delta_update(where=...)", where)
    assignments = _assignments(target, set)
    _visible(predicate, allowed={target._structure_scope_name}, action="delta_update")
    for _, expression in assignments:
        _visible(expression, allowed={target._structure_scope_name}, action="delta_update")
    _context().delta_mutations.append(
        DeltaMutation(
            "update",
            target._structure_source,
            target._structure_scope_name,
            predicate,
            assignments,
        )
    )


class DeltaMerge:
    def __init__(self, target: DeltaScope, source: RowScope, source_name: str, predicate: Expression) -> None:
        self.target = target
        self.source = source
        self.source_name = source_name
        self.predicate = predicate
        self.clauses: list[DeltaClause] = []
        self.executed = False

    def _add(self, action: str, condition: object | None = None, values: Schema | None = None) -> DeltaMerge:
        if self.executed:
            raise TypeError("A Delta merge builder can be executed only once")
        phase = action.split("_")[0]
        phases = {"matched": 0, "unmatched": 1, "source": 2}
        if self.clauses and phases[phase] < phases[self.clauses[-1].action.split("_")[0]]:
            raise TypeError("Delta merge clauses must be ordered: matched, unmatched, unmatched by source")
        assignments = () if values is None else _assignments(self.target, values, insert=action == "unmatched_insert")
        predicate = None if condition is None else _predicate("Delta merge condition", condition)
        allowed = {self.target._structure_scope_name, self.source._structure_scope_name}
        if phase == "unmatched":
            allowed = {self.source._structure_scope_name}
        elif phase == "source":
            allowed = {self.target._structure_scope_name}
        if predicate is not None:
            _visible(predicate, allowed=allowed, action=action)
        for _, expression in assignments:
            _visible(expression, allowed=allowed, action=action)
        self.clauses.append(DeltaClause(action, predicate, assignments))
        return self

    def when_matched_update(self, *, set: Schema, condition: object | None = None) -> DeltaMerge:
        return self._add("matched_update", condition, set)

    def when_matched_delete(self, *, condition: object | None = None) -> DeltaMerge:
        return self._add("matched_delete", condition)

    def when_matched_update_all(self, *, condition: object | None = None) -> DeltaMerge:
        return self._add("matched_update_all", condition)

    def when_not_matched_insert(self, *, values: Schema, condition: object | None = None) -> DeltaMerge:
        return self._add("unmatched_insert", condition, values)

    def when_not_matched_insert_all(self, *, condition: object | None = None) -> DeltaMerge:
        return self._add("unmatched_insert_all", condition)

    def when_not_matched_by_source_update(self, *, set: Schema, condition: object | None = None) -> DeltaMerge:
        return self._add("source_update", condition, set)

    def when_not_matched_by_source_delete(self, *, condition: object | None = None) -> DeltaMerge:
        return self._add("source_delete", condition)

    def execute(self) -> None:
        if self.executed or not self.clauses:
            raise TypeError("Delta merge requires clauses and can be executed only once")
        self.executed = True
        for phase in ("matched", "unmatched", "source"):
            clauses = [item for item in self.clauses if item.action.startswith(phase)]
            if any(item.condition is None for item in clauses[:-1]):
                raise TypeError(f"Only the final {phase} Delta merge clause may omit condition=")
        _context().delta_mutations.append(
            DeltaMutation(
                "merge",
                self.target._structure_source,
                self.target._structure_scope_name,
                self.predicate,
                source=self.source_name,
                source_scope=self.source._structure_scope_name,
                clauses=tuple(self.clauses),
            )
        )


def delta_merge(target: DeltaScope, source: RowScope, *, on: object) -> DeltaMerge:
    target = _target(target)
    if not isinstance(source, RowScope) or source is target:
        raise TypeError("delta_merge(source=...) requires a distinct relation parameter")
    source_name = source._structure_source if isinstance(source, InputScope) else _context().default_project_frame
    if not isinstance(source_name, str):
        raise TypeError("delta_merge(source=...) cannot resolve the source relation")
    predicate = _predicate("delta_merge(on=...)", on)
    actual = _scopes(predicate)
    expected = {target._structure_scope_name, source._structure_scope_name}
    if actual != expected:
        raise TypeError("delta_merge(on=...) must compare the target and source relation parameters")
    return DeltaMerge(target, source, source_name, predicate)
