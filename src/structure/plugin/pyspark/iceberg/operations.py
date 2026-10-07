"""Typed, compiler-visible Iceberg table operations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.Expression import Expression, _same_type
from structure.plugin.pyspark.dsl.expressions import literal
from structure.plugin.pyspark.dsl.InputScope import InputScope
from structure.plugin.pyspark.dsl.RowScope import RowScope
from structure.plugin.pyspark.dsl.types import BooleanType
from structure.plugin.pyspark.iceberg.model import IcebergClause, IcebergMutation, IcebergMutationResult
from structure.plugin.pyspark.symbolic_execution.model.PySparkSymbolicContext import current_pyspark_context


class IcebergScope(InputScope):
    def __init__(self, *, name: str, schema: type[Schema], source: str, binding: str) -> None:
        super().__init__(name=name, schema=schema, source=source)
        self._structure_iceberg_binding = binding


def _context():
    context = current_pyspark_context()
    if context is None:
        raise RuntimeError("Iceberg helpers can only be used while compiling a Structure Transform method")
    return context


def _target(value: object, *, mutation: bool = True) -> IcebergScope:
    if not isinstance(value, IcebergScope):
        raise TypeError("Iceberg operations require a relation declared with iceberg_input(...) or iceberg_table(...)")
    if value._structure_iceberg_binding == "iceberg_output":
        raise TypeError("iceberg_output(...) is a schema transition result, not a caller-bound table")
    if mutation and value._structure_iceberg_binding == "iceberg_input":
        raise TypeError("iceberg_input(...) is read-only; use iceberg_table(...) for same-schema mutations")
    return value


def _predicate(action: str, value: object, allowed: set[str]) -> Expression:
    expression = literal(value)
    if not isinstance(expression.type, BooleanType):
        raise TypeError(f"{action}(where=...) requires a Boolean expression; use where=True for all rows")
    _visible(expression, allowed=allowed, action=action)
    return expression


def _visible(expression: Expression, *, allowed: set[str], action: str) -> None:
    scopes = set()
    pending = [expression]
    while pending:
        current = pending.pop()
        pending.extend(current.args)
        if current.kind == "field":
            scopes.add(str((current.data or {})["scope"]))
    extra = scopes - allowed
    if extra:
        raise TypeError(f"{action} references unavailable relation scope(s): {', '.join(sorted(extra))}")


def _assignments(target: IcebergScope, values: object, *, insert: bool = False):
    schema = target._structure_input_schema
    if not isinstance(values, schema):
        raise TypeError(f"Iceberg assignments require {schema.__name__}(...) values")
    if not values._structure_values:
        raise TypeError("Iceberg assignments must contain at least one target field")
    assignments = []
    for name, value in values._structure_values.items():
        field = schema._structure_fields[name]
        expression = literal(value)
        if expression.type is not None and not _same_type(expression.type, field.type):
            raise TypeError(f"Iceberg assignment {name} expects {field.type.name}, got {expression.type.name}")
        if expression.nullable and not field.nullable:
            raise TypeError(f"Iceberg assignment {name} may be null, but the target field is non-nullable")
        assignments.append((field.column, expression))
    if insert:
        missing = [
            field.column
            for field in schema._structure_fields.values()
            if not field.nullable and field.column not in {name for name, _ in assignments}
        ]
        if missing:
            raise TypeError(f"Iceberg insert is missing non-nullable field(s): {', '.join(missing)}")
    return tuple(assignments)


def _source_name(source: RowScope) -> str:
    if not isinstance(source, RowScope):
        raise TypeError("Iceberg source must be a Structure relation")
    name = source._structure_source if isinstance(source, InputScope) else _context().default_project_frame
    if not isinstance(name, str):
        raise TypeError("Iceberg source relation cannot be resolved")
    return name


def _append_mutation(mutation: IcebergMutation) -> IcebergMutationResult:
    _context().delta_mutations.append(mutation)
    return IcebergMutationResult(mutation)


def iceberg_delete(target: IcebergScope, *, where: object) -> None:
    target = _target(target)
    predicate = _predicate("iceberg_delete", where, {target._structure_scope_name})
    _context().delta_mutations.append(
        IcebergMutation("iceberg_delete", target._structure_source, target._structure_scope_name, predicate)
    )


def iceberg_update(target: IcebergScope, *, where: object, set: Schema) -> None:
    target = _target(target)
    predicate = _predicate("iceberg_update", where, {target._structure_scope_name})
    assignments = _assignments(target, set)
    for _, expression in assignments:
        _visible(expression, allowed={target._structure_scope_name}, action="iceberg_update")
    _context().delta_mutations.append(
        IcebergMutation("iceberg_update", target._structure_source, target._structure_scope_name, predicate, assignments)
    )


class IcebergAppend:
    def __init__(self, target: IcebergScope, source: RowScope) -> None:
        self.target = target
        self.source = source
        self.schema: type[Schema] | None = None
        self.executed = False

    def with_schema_evolution(self, *, to: type[Schema]) -> IcebergAppend:
        if self.executed or self.schema is not None:
            raise TypeError("Iceberg append schema evolution can be selected once before execute()")
        if not isinstance(to, type) or not issubclass(to, Schema):
            raise TypeError("with_schema_evolution(to=...) requires a Structure Schema class")
        if to is self.target._structure_input_schema:
            raise TypeError("with_schema_evolution(to=...) requires a different output Schema")
        self.schema = to
        return self

    def execute(self) -> IcebergMutationResult | None:
        if self.executed:
            raise TypeError("An Iceberg append operation can execute only once")
        self.executed = True
        schema = self.schema
        if schema is None and self.target._structure_iceberg_binding == "iceberg_input":
            raise TypeError("iceberg_input(...) can be written only by an explicit schema-evolving append")
        if schema is not None:
            _validate_append_schema(self.target, self.source, schema)
        result = _append_mutation(
            IcebergMutation(
                "iceberg_append",
                self.target._structure_source,
                self.target._structure_scope_name,
                source=_source_name(self.source),
                source_scope=self.source._structure_scope_name,
                schema_evolution=schema is not None,
                output_schema=schema,
            )
        )
        return result if schema is not None else None


def _validate_append_schema(target: IcebergScope, source: RowScope, output: type[Schema]) -> None:
    old = {field.column: field for field in target._structure_input_schema._structure_fields.values()}
    incoming = {field.column: field for field in source._structure_scope_schema._structure_fields.values()}
    result = {field.column: field for field in output._structure_fields.values()}
    if not old.keys() <= result.keys():
        raise TypeError("Iceberg append schema evolution cannot drop existing columns")
    for column, field in old.items():
        actual = result[column]
        if not _same_type(field.type, actual.type) or field.nullable != actual.nullable:
            raise TypeError(f"Iceberg evolution must preserve the existing type and nullability for {field.name}")
    if not incoming.keys() <= result.keys():
        raise TypeError("Iceberg evolving append result schema must include every source column")
    for column, field in incoming.items():
        actual = result[column]
        if not _same_type(field.type, actual.type) or field.nullable != actual.nullable:
            raise TypeError(f"Iceberg evolution field {actual.name} is incompatible with the append source")
    added = result.keys() - old.keys()
    required = [result[column].name for column in added if not result[column].nullable]
    if required:
        raise TypeError(f"Iceberg append schema evolution requires new fields to be nullable: {', '.join(required)}")


def iceberg_append(target: IcebergScope, source: RowScope) -> IcebergAppend:
    return IcebergAppend(_target(target, mutation=False), source)


class IcebergMerge:
    def __init__(self, target: IcebergScope, source: RowScope, predicate: Expression) -> None:
        self.target = target
        self.source = source
        self.predicate = predicate
        self.clauses: list[IcebergClause] = []
        self.executed = False

    def _add(self, action: str, *, condition: object | None = None, values: Schema | None = None) -> IcebergMerge:
        if self.executed:
            raise TypeError("Iceberg merge clauses cannot be added after execute()")
        allowed = {self.target._structure_scope_name, self.source._structure_scope_name}
        predicate = None if condition is None else _predicate("iceberg_merge", condition, allowed)
        assignments = () if values is None else _assignments(self.target, values, insert=action.endswith("insert"))
        for _, expression in assignments:
            _visible(expression, allowed=allowed, action="iceberg_merge")
        self.clauses.append(IcebergClause(action, predicate, assignments))
        return self

    def when_matched_update(self, *, set: Schema, condition: object | None = None) -> IcebergMerge:
        return self._add("matched_update", condition=condition, values=set)

    def when_matched_delete(self, *, condition: object | None = None) -> IcebergMerge:
        return self._add("matched_delete", condition=condition)

    def when_matched_update_all(self, *, condition: object | None = None) -> IcebergMerge:
        return self._add("matched_update_all", condition=condition)

    def when_not_matched_insert(self, *, values: Schema, condition: object | None = None) -> IcebergMerge:
        return self._add("unmatched_insert", condition=condition, values=values)

    def when_not_matched_insert_all(self, *, condition: object | None = None) -> IcebergMerge:
        return self._add("unmatched_insert_all", condition=condition)

    def when_not_matched_by_source_update(self, *, set: Schema, condition: object | None = None) -> IcebergMerge:
        return self._add("unmatched_source_update", condition=condition, values=set)

    def when_not_matched_by_source_delete(self, *, condition: object | None = None) -> IcebergMerge:
        return self._add("unmatched_source_delete", condition=condition)

    def execute(self) -> IcebergMutationResult:
        if self.executed:
            raise TypeError("An Iceberg merge operation can execute only once")
        if not self.clauses:
            raise TypeError("Iceberg merge requires at least one clause")
        self.executed = True
        return _append_mutation(
            IcebergMutation(
                "iceberg_merge",
                self.target._structure_source,
                self.target._structure_scope_name,
                self.predicate,
                source=_source_name(self.source),
                source_scope=self.source._structure_scope_name,
                clauses=tuple(self.clauses),
            )
        )


def iceberg_merge(target: IcebergScope, source: RowScope, *, on: object) -> IcebergMerge:
    target = _target(target)
    if not isinstance(source, RowScope):
        raise TypeError("iceberg_merge(source=...) requires a Structure relation")
    predicate = _predicate("iceberg_merge", on, {target._structure_scope_name, source._structure_scope_name})
    return IcebergMerge(target, source, predicate)


def _read_result(target: IcebergScope, *, kind: str, selector=None, action: str | None = None):
    context = _context()
    schema = getattr(context, "step_output_schema", None)
    if not isinstance(schema, type) or not issubclass(schema, Schema):
        raise TypeError(f"{kind} must be the direct result of a single-output @step")
    selected = None if selector is None else literal(selector)
    if selected is not None and selected.kind not in {"literal", "variable"}:
        raise TypeError(f"{kind} selector must be a literal or runtime variable")
    if selected is not None and selected.kind == "literal" and (selected.data or {}).get("value") is None:
        raise TypeError(f"{kind} selector cannot be None")
    context.delta_mutations.append(
        IcebergMutation(
            kind, target._structure_source, target._structure_scope_name,
            selector=selected, action=action, output_schema=schema,
        )
    )
    return RowScope(name=target._structure_scope_name, schema=schema)


def iceberg_snapshot(target: IcebergScope, *, snapshot_id=None, timestamp=None):
    target = _target(target, mutation=False)
    if (snapshot_id is None) == (timestamp is None):
        raise TypeError("iceberg_snapshot requires exactly one of snapshot_id= or timestamp=")
    selected = snapshot_id if snapshot_id is not None else timestamp
    expression = literal(selected)
    expected = {"long", "integer"} if snapshot_id is not None else {"timestamp"}
    if expression.type is None or expression.type.name not in expected:
        raise TypeError("iceberg_snapshot requires an integer snapshot ID or timestamp selector")
    if timestamp is not None:
        _validate_timestamp_literal(expression, "iceberg_snapshot")
    return _read_result(target, kind="iceberg_snapshot", selector=selected)


def iceberg_history(target: IcebergScope, *, limit=None):
    _validate_limit("iceberg_history", limit)
    return _read_result(_target(target, mutation=False), kind="iceberg_history", selector=limit)


def iceberg_snapshots(target: IcebergScope, *, limit=None):
    _validate_limit("iceberg_snapshots", limit)
    return _read_result(_target(target, mutation=False), kind="iceberg_snapshots", selector=limit)


def iceberg_metadata(target: IcebergScope, *, kind: str, to: type[Schema]):
    target = _target(target, mutation=False)
    if kind not in {"history", "snapshots", "files", "manifests", "partitions", "refs"}:
        raise ValueError("Iceberg metadata kind must be history, snapshots, files, manifests, partitions, or refs")
    if not isinstance(to, type) or not issubclass(to, Schema):
        raise TypeError("iceberg_metadata(to=...) requires a Structure Schema class")
    if getattr(_context(), "step_output_schema", None) is not to:
        raise TypeError("iceberg_metadata(to=...) must match the direct step return Schema")
    return _read_result(target, kind="iceberg_metadata", action=kind)


def _validate_limit(action: str, limit) -> None:
    if limit is None:
        return
    selected = literal(limit)
    if selected.kind not in {"literal", "variable"} or selected.type is None or selected.type.name not in {"integer", "long"}:
        raise TypeError(f"{action}(limit=...) requires a positive integer literal or variable")
    if selected.kind == "literal":
        value = (selected.data or {}).get("value")
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{action}(limit=...) must be a positive integer")


class IcebergMaintenance:
    def __init__(self, target: IcebergScope, procedure: str, args: tuple[tuple[str, object], ...]):
        self.target = target
        self.procedure = procedure
        self.args = args
        self.executed = False

    def execute(self) -> None:
        if self.executed:
            raise TypeError("An Iceberg maintenance operation can execute only once")
        self.executed = True
        _context().delta_mutations.append(
            IcebergMutation(
                "iceberg_maintenance", self.target._structure_source,
                self.target._structure_scope_name, action=self.procedure,
                procedure_args=self.args,
            )
        )


def _maintenance(target, procedure: str, **arguments) -> IcebergMaintenance:
    target = _target(target)
    if target._structure_iceberg_binding != "iceberg_table":
        raise TypeError(f"iceberg_{procedure}(...) requires an iceberg_table(...) target")
    allowed = {
        "rollback_to_snapshot", "rollback_to_timestamp", "rewrite_data_files",
        "rewrite_manifests", "expire_snapshots", "remove_orphan_files",
    }
    if procedure not in allowed:
        raise ValueError(f"Unknown Iceberg maintenance procedure {procedure!r}")
    return IcebergMaintenance(target, procedure, tuple((key, value) for key, value in arguments.items() if value is not None))


def iceberg_rollback(target: IcebergScope, *, snapshot_id=None, timestamp=None) -> IcebergMaintenance:
    if (snapshot_id is None) == (timestamp is None):
        raise TypeError("iceberg_rollback requires exactly one of snapshot_id= or timestamp=")
    selector = snapshot_id if snapshot_id is not None else timestamp
    expression = literal(selector)
    if expression.kind not in {"literal", "variable"} or expression.type is None:
        raise TypeError("iceberg_rollback selectors must be integer IDs or timestamps")
    if snapshot_id is not None and expression.type.name not in {"long", "integer"}:
        raise TypeError("iceberg_rollback(snapshot_id=...) requires an integer")
    if timestamp is not None and expression.type.name != "timestamp":
        raise TypeError("iceberg_rollback(timestamp=...) requires a timestamp")
    if timestamp is not None:
        _validate_timestamp_literal(expression, "iceberg_rollback")
    proc = "rollback_to_snapshot" if snapshot_id is not None else "rollback_to_timestamp"
    key = "snapshot_id" if snapshot_id is not None else "timestamp"
    return _maintenance(target, proc, **{key: selector})


def iceberg_rewrite_data_files(target: IcebergScope, *, strategy=None, sort_order=None, where=None, options=None):
    for name, value in (("strategy", strategy), ("sort_order", sort_order)):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise TypeError(f"{name} must be a non-empty string")
    if options is not None:
        if not isinstance(options, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in options.items()
        ):
            raise TypeError("Iceberg rewrite options must be a mapping of strings to strings")
        options = dict(options)
    arguments = {"strategy": strategy, "sort_order": sort_order, "options": options}
    if where is not None:
        predicate = _predicate("iceberg_rewrite_data_files(where=...)", where, {target._structure_scope_name})
        arguments["where"] = predicate
    return _maintenance(target, "rewrite_data_files", **arguments)


def iceberg_rewrite_manifests(target: IcebergScope, *, use_caching=None, spec_id=None):
    if use_caching is not None and not isinstance(use_caching, bool):
        raise TypeError("use_caching must be a Boolean")
    if spec_id is not None and (isinstance(spec_id, bool) or not isinstance(spec_id, int) or spec_id < 0):
        raise TypeError("spec_id must be a nonnegative integer")
    return _maintenance(target, "rewrite_manifests", use_caching=use_caching, spec_id=spec_id)


def iceberg_expire_snapshots(target: IcebergScope, *, older_than=None, retain_last=None):
    if older_than is not None:
        expression = literal(older_than)
        if expression.type is None or expression.type.name != "timestamp" or expression.kind not in {"literal", "variable"}:
            raise TypeError("older_than must be a timestamp literal or runtime variable")
        _validate_timestamp_literal(expression, "iceberg_expire_snapshots")
    if retain_last is not None and (
        isinstance(retain_last, bool) or not isinstance(retain_last, int) or retain_last <= 0
    ):
        raise TypeError("retain_last must be a positive integer")
    return _maintenance(target, "expire_snapshots", older_than=older_than, retain_last=retain_last)


def iceberg_remove_orphan_files(target: IcebergScope, *, older_than=None, dry_run=False):
    if not isinstance(dry_run, bool):
        raise TypeError("dry_run must be a Boolean")
    if older_than is not None:
        expression = literal(older_than)
        if expression.type is None or expression.type.name != "timestamp" or expression.kind not in {"literal", "variable"}:
            raise TypeError("older_than must be a timestamp literal or runtime variable")
        _validate_timestamp_literal(expression, "iceberg_remove_orphan_files")
    return _maintenance(target, "remove_orphan_files", older_than=older_than, dry_run=dry_run)


def _validate_timestamp_literal(expression: Expression, action: str) -> None:
    if expression.kind != "literal":
        return
    value = (expression.data or {}).get("value")
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise TypeError(f"{action} timestamp values must be timezone-aware datetime objects")
