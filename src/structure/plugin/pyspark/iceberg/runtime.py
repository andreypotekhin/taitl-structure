"""Public Spark SQL and DataFrame operations for caller-owned Iceberg tables."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from decimal import Decimal


def quote_table_name(name: str) -> str:
    """Validate and quote a multipart Spark catalog identifier."""
    parts = _identifier_parts(name)
    return ".".join("`" + item.replace("`", "``") + "`" for item in parts)


def quote_iceberg_column(name: str) -> str:
    """Quote one Iceberg column name for generated Spark SQL."""
    if not isinstance(name, str) or not name:
        raise TypeError("Iceberg column names must be non-empty strings")
    return "`" + name.replace("`", "``") + "`"


def _identifier_parts(name: str) -> list[str]:
    if not isinstance(name, str):
        raise TypeError("Iceberg bindings require a catalog table name string")
    parts, part, quoted, index = [], "", False, 0
    while index < len(name):
        char = name[index]
        if char == "`":
            if quoted and index + 1 < len(name) and name[index + 1] == "`":
                part += "`"
                index += 1
            else:
                quoted = not quoted
        elif char == "." and not quoted:
            if not part:
                raise ValueError(f"Invalid Iceberg table identifier {name!r}")
            parts.append(part)
            part = ""
        else:
            part += char
        index += 1
    if quoted or not part:
        raise ValueError(f"Invalid Iceberg table identifier {name!r}")
    parts.append(part)
    if len(parts) < 2 or any(not item for item in parts):
        raise ValueError("Iceberg table names must include an explicit catalog and table component")
    return parts


def validate_iceberg_table(spark, name: str, schema: type, *, validation="on"):
    quoted = quote_table_name(name)
    try:
        provider_rows = spark.sql(f"DESCRIBE TABLE EXTENDED {quoted}").collect()
    except Exception as error:
        error.add_note(f"Cannot resolve Iceberg table {name!r}; check the configured catalog and table name")
        raise
    properties = {str(row[0]).strip().casefold(): str(row[1]).strip() for row in provider_rows if len(row) > 1}
    provider = properties.get("provider", "").casefold()
    if provider != "iceberg":
        raise TypeError(f"Table {name!r} uses provider {provider or '<unknown>'!r}; expected Iceberg")
    table_properties = spark.sql(f"SHOW TBLPROPERTIES {quoted}").collect()
    property_map = {str(row[0]).strip().casefold(): str(row[1]).strip() for row in table_properties if len(row) > 1}
    format_version = property_map.get("format-version", "2")
    if format_version != "2":
        raise ValueError(
            f"Table {name!r} uses Iceberg format version {format_version!r}; typed Iceberg helpers require "
            "format-version=2. Recreate or migrate the table with format-version=2 before using the helpers."
        )
    frame = spark.table(quoted)
    if validation != "off":
        _validate_schema(frame, schema)
    return frame


def validate_evolved_iceberg_table(spark, name: str, schema: type):
    try:
        return validate_iceberg_table(spark, name, schema)
    except Exception as error:
        error.add_note("The Iceberg append may already have committed before output-schema validation failed.")
        raise


def _validate_schema(frame, schema):
    from pyspark.sql import types as T

    from structure.plugin.pyspark.api.PySpark import PySpark

    expected = PySpark.schema.materialize()(schema, types=T)
    actual = {field.name: field for field in frame.schema}
    if {field.name for field in expected} != set(actual):
        raise ValueError(f"Iceberg table for {schema.__name__} has different columns")
    validator = PySpark.execution.validator()
    for field in expected:
        found = actual[field.name]
        if not validator._same_data_type(found.dataType, field.dataType):
            raise ValueError(f"Iceberg table for {schema.__name__}.{field.name} has incompatible type")


def _literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return "TIMESTAMP '" + value.isoformat(sep=" ") + "'"
    if isinstance(value, date):
        return "DATE '" + value.isoformat() + "'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"Unsupported Iceberg SQL value {type(value).__name__}")


def render_iceberg_predicate_template(expression, aliases=None):
    if expression.kind == "field":
        data = expression.data or {}
        field = ".".join("`" + str(part).replace("`", "``") + "`" for part in data["path"])
        scope = data.get("scope")
        return f"{aliases[scope]}.{field}" if aliases and scope in aliases else field
    if expression.kind == "variable":
        return "{{" + str(expression.data["name"]) + "}}"
    if expression.kind == "literal":
        return _literal((expression.data or {}).get("value"))
    unary = {"not": "NOT", "neg": "-"}
    if expression.kind in unary:
        return f"{unary[expression.kind]} ({render_iceberg_predicate_template(expression.args[0], aliases)})"
    binary = {
        "and": "AND", "or": "OR", "eq": "=", "ne": "<>", "gt": ">", "lt": "<",
        "ge": ">=", "le": "<=", "add": "+", "sub": "-", "mul": "*", "div": "/", "mod": "%",
    }
    if expression.kind in binary:
        left, right = (render_iceberg_predicate_template(item, aliases) for item in expression.args)
        return f"({left} {binary[expression.kind]} {right})"
    if expression.kind in {"is_null", "is_not_null"}:
        operator = "IS NULL" if expression.kind == "is_null" else "IS NOT NULL"
        return f"({render_iceberg_predicate_template(expression.args[0], aliases)} {operator})"
    raise TypeError(f"Iceberg SQL helpers do not support {expression.kind!r} expressions; use sql(...)")


def bind_iceberg_predicate_variables(template: str, variables) -> str:
    if isinstance(template, tuple):
        return tuple(bind_iceberg_predicate_variables(value, variables) for value in template)
    if isinstance(template, list):
        return [bind_iceberg_predicate_variables(value, variables) for value in template]
    for name, value in variables.items():
        template = template.replace("{{" + name + "}}", _literal(value))
    if "{{" in template or "}}" in template:
        raise ValueError("Iceberg SQL expression has an unbound runtime variable")
    return template


def _expr(expression, aliases=None):
    from structure.plugin.pyspark.dsl.RuntimeVariables import runtime_variable

    template = render_iceberg_predicate_template(expression, aliases)
    names = set(re.findall(r"\{\{([A-Za-z_][A-Za-z0-9_]*)\}\}", template))
    variables = {name: runtime_variable(name) for name in names}
    return bind_iceberg_predicate_variables(template, variables)


def execute_iceberg_mutation(mutation, *, tables, frames, spark):
    target = quote_table_name(tables[mutation.target])
    kind = mutation.kind
    if kind == "iceberg_delete":
        spark.sql(f"DELETE FROM {target} WHERE {_expr(mutation.predicate)}")
    elif kind == "iceberg_update":
        assignments = ", ".join(f"`{name.replace('`', '``')}` = {_expr(value)}" for name, value in mutation.assignments)
        spark.sql(f"UPDATE {target} SET {assignments} WHERE {_expr(mutation.predicate)}")
    elif kind == "iceberg_append":
        if mutation.source is None:
            raise ValueError("Iceberg append is missing its source relation")
        source = frames[mutation.source]
        append_iceberg_table(source, tables[mutation.target], schema_evolution=mutation.schema_evolution)
    elif kind == "iceberg_merge":
        if mutation.source is None or mutation.source_scope is None:
            raise ValueError("Iceberg merge is missing its source relation")
        view = f"_structure_iceberg_{id(mutation):x}"
        frames[mutation.source].createOrReplaceTempView(view)
        try:
            clauses = []
            aliases = {mutation.target_scope: "target", mutation.source_scope: "source"}
            for clause in mutation.clauses:
                condition = "" if clause.condition is None else f" AND {_expr(clause.condition, aliases)}"
                if clause.action == "matched_update":
                    values = ", ".join(f"`{name}` = {_expr(expr, aliases)}" for name, expr in clause.assignments)
                    clauses.append(f"WHEN MATCHED{condition} THEN UPDATE SET {values}")
                elif clause.action == "matched_delete":
                    clauses.append(f"WHEN MATCHED{condition} THEN DELETE")
                elif clause.action == "matched_update_all":
                    clauses.append(f"WHEN MATCHED{condition} THEN UPDATE SET *")
                elif clause.action in {"unmatched_insert", "unmatched_insert_all"}:
                    if clause.action.endswith("all"):
                        clauses.append(f"WHEN NOT MATCHED{condition} THEN INSERT *")
                    else:
                        columns = ", ".join(f"`{name}`" for name, _ in clause.assignments)
                        values = ", ".join(_expr(expr, aliases) for _, expr in clause.assignments)
                        clauses.append(f"WHEN NOT MATCHED{condition} THEN INSERT ({columns}) VALUES ({values})")
                elif clause.action == "unmatched_source_update":
                    values = ", ".join(f"`{name}` = {_expr(expr, aliases)}" for name, expr in clause.assignments)
                    clauses.append(f"WHEN NOT MATCHED BY SOURCE{condition} THEN UPDATE SET {values}")
                elif clause.action == "unmatched_source_delete":
                    clauses.append(f"WHEN NOT MATCHED BY SOURCE{condition} THEN DELETE")
                else:
                    raise ValueError(f"Unknown Iceberg merge clause {clause.action!r}")
            spark.sql(
                f"MERGE INTO {target} AS target USING `{view}` AS source "
                f"ON {_expr(mutation.predicate, aliases)} {' '.join(clauses)}"
            )
        finally:
            spark.catalog.dropTempView(view)
    elif kind == "iceberg_maintenance":
        execute_iceberg_procedure(
            spark, tables[mutation.target], mutation.action, mutation.procedure_args
        )
    else:
        raise ValueError(f"Unknown Iceberg mutation {kind!r}")


def _procedure_value(value):
    if isinstance(value, dict):
        entries: list[str] = []
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, str):
                raise TypeError("Iceberg procedure options must map strings to strings")
            entries.extend((_literal(key), _literal(item)))
        return "map(" + ", ".join(entries) + ")"
    return _literal(value)


def execute_iceberg_procedure(spark, table: str, procedure: str, arguments, variables=None):
    parts = _identifier_parts(table)
    catalog, table_name = parts[0], ".".join(parts[1:])
    args = [f"table => {_literal(table_name)}"]
    for key, value in arguments:
        if isinstance(value, str) and ("{{" in value or "}}" in value):
            rendered = bind_iceberg_predicate_variables(value, variables or {})
            if key == "where":
                rendered = _literal(rendered)
        elif hasattr(value, "kind"):
            rendered = _expr(value)
            if key == "where":
                rendered = _literal(rendered)
        else:
            rendered = _procedure_value(value)
        args.append(f"{key} => {rendered}")
    spark.sql(f"CALL `{catalog}`.system.{procedure}({', '.join(args)})")


def append_iceberg_table(source, table: str, *, schema_evolution: bool = False) -> None:
    try:
        writer = source.writeTo(table)
        if schema_evolution:
            writer = writer.option("mergeSchema", "true")
        writer.append()
    except Exception as error:
        if schema_evolution:
            error.add_note(
                "Iceberg evolving append requires table property write.spark.accept-any-schema=true; "
                "set it on the destination table before retrying."
            )
        raise


def read_iceberg_relation(mutation, *, tables, spark):
    """Read a current or historical relation and a typed metadata relation."""
    if isinstance(mutation, dict):
        from types import SimpleNamespace

        selector = mutation.get("selector")
        if selector is not None:
            selector = SimpleNamespace(
                kind=selector["kind"],
                type=SimpleNamespace(name=selector["type"]) if selector.get("type") else None,
                data=selector.get("data", {}),
            )
        mutation = SimpleNamespace(
            kind=mutation["kind"], target=mutation["target"],
            selector=selector, action=mutation.get("action"), output_schema=None,
        )
    name = quote_table_name(tables[mutation.target])
    kind = mutation.kind
    if kind == "iceberg_snapshot":
        selector = mutation.selector
        value = selector.data["value"] if selector.kind == "literal" else _variable(selector)
        if selector.type.name == "timestamp":
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise TypeError("Iceberg timestamp selectors must be timezone-aware datetime values")
            value = value.astimezone(timezone.utc)
            frame = spark.sql(f"SELECT * FROM {name} TIMESTAMP AS OF {_literal(value)}")
        else:
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError("Iceberg snapshot IDs must be integers")
            frame = spark.sql(f"SELECT * FROM {name} VERSION AS OF {value}")
    elif kind in {"iceberg_history", "iceberg_snapshots", "iceberg_metadata"}:
        metadata = mutation.action if kind == "iceberg_metadata" else kind.removeprefix("iceberg_")
        frame = spark.table(f"{name}.{metadata}")
        if mutation.selector is not None:
            value = mutation.selector.data["value"] if mutation.selector.kind == "literal" else _variable(mutation.selector)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{kind} limit must be a positive integer")
            sort = "made_current_at" if metadata == "history" else "committed_at" if metadata == "snapshots" else None
            if sort:
                frame = frame.orderBy(frame[sort].desc(), frame["snapshot_id"].desc()).limit(value)
            else:
                frame = frame.limit(value)
    else:
        raise ValueError(f"Unknown Iceberg read {kind!r}")
    schema = mutation.output_schema
    if schema is not None:
        expected = [field.column for field in schema._structure_fields.values()]
        missing = [name for name in expected if name not in frame.columns]
        if missing:
            raise ValueError(f"Iceberg {kind} metadata is missing requested column(s): {', '.join(missing)}")
        from pyspark.sql import types as T

        from structure.plugin.pyspark.api.PySpark import PySpark

        expected_schema = PySpark.schema.materialize()(schema, types=T)
        actual_fields = {field.name: field for field in frame.schema}
        validator = PySpark.execution.validator()
        for expected_field in expected_schema:
            actual_field = actual_fields[expected_field.name]
            if not validator._same_data_type(actual_field.dataType, expected_field.dataType):
                raise TypeError(
                    f"Iceberg {kind} metadata column {expected_field.name!r} has type "
                    f"{actual_field.dataType.simpleString()}, expected {expected_field.dataType.simpleString()}"
                )
        frame = frame.select(*expected)
    return frame


def _variable(expression):
    from structure.plugin.pyspark.dsl.RuntimeVariables import runtime_variable

    return runtime_variable(str(expression.data["name"]))
