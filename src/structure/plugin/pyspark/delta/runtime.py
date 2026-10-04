"""Late-bound Delta validation and mutation execution."""

from __future__ import annotations

import re

from structure.plugin.pyspark.delta.checks import bind_checks


def validate_delta_table(table, schema, *, mode: str = "expression"):
    try:
        from delta.tables import DeltaTable  # type: ignore[import-not-found]
        from pyspark.sql import types as T
    except ImportError as error:
        raise RuntimeError("Delta table bindings require the optional delta-spark and pyspark packages") from error
    if not isinstance(table, DeltaTable):
        raise TypeError(f"{schema.__name__} Delta binding requires delta.tables.DeltaTable")
    from structure.plugin.pyspark.api.PySpark import PySpark

    expected = PySpark.schema.materialize()(schema, types=T)
    actual = fresh_delta_frame(table).schema
    expected_fields = {field.name: field for field in expected}
    actual_fields = {field.name: field for field in actual}
    if expected_fields.keys() != actual_fields.keys():
        raise ValueError(
            f"Delta table for {schema.__name__} has different columns: expected {sorted(expected_fields)}, got {sorted(actual_fields)}"
        )
    validator = PySpark.execution.validator()
    for name, field in expected_fields.items():
        found = actual_fields[name]
        if not validator._same_data_type(found.dataType, field.dataType) or found.nullable != field.nullable:
            raise ValueError(f"Delta table for {schema.__name__}.{name} has incompatible type or nullability")
    if mode == "off":
        return table
    checks = bind_checks(schema)
    if not checks:
        return table
    properties = table.detail().select("properties").first()["properties"] or {}
    native = {
        key[len("delta.constraints.") :].casefold(): value
        for key, value in properties.items()
        if key.casefold().startswith("delta.constraints.")
    }
    case_sensitive = table.toDF().sparkSession.conf.get("spark.sql.caseSensitive", "false").lower() == "true"
    for check in checks:
        stored = native.get(check.name.casefold())
        if stored is None:
            raise ValueError(
                f"Delta table for {schema.__name__} is missing CHECK {check.name}; provision it before running the transform"
            )
        if mode == "expression" and _check_tree(check.predicate, case_sensitive) != _sql_tree(stored, case_sensitive):
            raise ValueError(f"Delta CHECK {check.name} differs from {schema.__name__}.constraints")
    return table


def fresh_delta_frame(table):
    """Read the latest committed snapshot without replacing the caller's handle."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    frame = table.toDF()
    location = table.detail().select("location").first()["location"]
    return DeltaTable.forPath(frame.sparkSession, location).toDF()


def execute_delta_mutation(mutation, *, tables, frames, functions):
    table = tables[mutation.target]
    from structure.plugin.pyspark.api.PySpark import PySpark

    evaluator = PySpark.execution.expression()
    aliases = {mutation.target_scope: ""}

    def column(expression, mapping=aliases):
        return evaluator.evaluate(expression, functions=functions, aliases=mapping)

    if mutation.kind == "delete":
        table.delete(column(mutation.predicate))
        return
    if mutation.kind == "update":
        table.update(
            condition=column(mutation.predicate), set={name: column(value) for name, value in mutation.assignments}
        )
        return
    if mutation.kind != "merge" or mutation.source is None or mutation.source_scope is None:
        raise ValueError(f"Unknown Delta mutation {mutation.kind!r}")
    source = frames[mutation.source]
    mapping = {mutation.target_scope: "target", mutation.source_scope: "source"}

    def merged(expression):
        return column(expression, mapping)

    builder = table.alias("target").merge(source.alias("source"), merged(mutation.predicate))
    for clause in mutation.clauses:
        condition = None if clause.condition is None else merged(clause.condition)
        values = {name: merged(value) for name, value in clause.assignments}
        if clause.action == "matched_update":
            builder = builder.whenMatchedUpdate(condition=condition, set=values)
        elif clause.action == "matched_delete":
            builder = builder.whenMatchedDelete(condition=condition)
        elif clause.action == "matched_update_all":
            builder = builder.whenMatchedUpdateAll(condition=condition)
        elif clause.action == "unmatched_insert":
            builder = builder.whenNotMatchedInsert(condition=condition, values=values)
        elif clause.action == "unmatched_insert_all":
            builder = builder.whenNotMatchedInsertAll(condition=condition)
        elif clause.action == "source_update":
            builder = builder.whenNotMatchedBySourceUpdate(condition=condition, set=values)
        elif clause.action == "source_delete":
            builder = builder.whenNotMatchedBySourceDelete(condition=condition)
        else:
            raise ValueError(f"Unknown Delta merge action {clause.action!r}")
    builder.execute()


_TOKEN = re.compile(
    r"\s*(?:('(?:''|[^'])*')|(`(?:``|[^`])*`)|([A-Za-z_][A-Za-z_0-9]*)|(\d+(?:\.\d+)?)|(<=|>=|<>|!=|==|[()+*/%<>=,-]))"
)


def _sql_tree(source: str, case_sensitive: bool):
    source = source.strip()
    tokens = []
    offset = 0
    while offset < len(source):
        match = _TOKEN.match(source, offset)
        if match is None:
            raise ValueError(
                f"Unsupported Delta CHECK expression near {source[offset:offset + 20]!r}; use a supported expression or delta_check_match='name'"
            )
        tokens.append(next(group for group in match.groups() if group is not None))
        offset = match.end()
    position = 0

    def peek():
        return tokens[position] if position < len(tokens) else None

    def take():
        nonlocal position
        value = peek()
        position += 1
        return value

    def expression(minimum=0):
        token = take()
        if token is None:
            raise ValueError("Empty Delta CHECK expression")
        upper = token.upper()
        if token == "(":
            left = expression()
            if take() != ")":
                raise ValueError("Unbalanced Delta CHECK expression")
        elif upper == "NOT":
            left = ("not", expression(7))
        elif token == "-":
            left = ("neg", expression(7))
        elif token.startswith("'"):
            left = ("literal", token[1:-1].replace("''", "'"))
        elif token[0].isdigit():
            left = ("number", token.lstrip("0") or "0")
        elif upper in {"TRUE", "FALSE", "NULL"}:
            left = ("literal", {"TRUE": True, "FALSE": False, "NULL": None}[upper])
        elif peek() == "(":
            take()
            argument = expression()
            if take() != ")":
                raise ValueError("Unsupported Delta CHECK function")
            left = (upper.casefold(), argument)
        else:
            identifier = token[1:-1].replace("``", "`") if token.startswith("`") else token
            left = ("field", identifier if case_sensitive else identifier.casefold())
        precedence = {
            "OR": 1,
            "AND": 2,
            "=": 3,
            "==": 3,
            "!=": 3,
            "<>": 3,
            ">": 3,
            "<": 3,
            ">=": 3,
            "<=": 3,
            "+": 4,
            "-": 4,
            "*": 5,
            "/": 5,
            "%": 5,
        }
        names = {
            "OR": "or",
            "AND": "and",
            "=": "eq",
            "==": "eq",
            "!=": "ne",
            "<>": "ne",
            ">": "gt",
            "<": "lt",
            ">=": "ge",
            "<=": "le",
            "+": "add",
            "-": "sub",
            "*": "mul",
            "/": "div",
            "%": "mod",
        }
        while peek() is not None:
            operator = peek().upper()
            if operator == "IS":
                if 3 < minimum:
                    break
                take()
                negate = peek() is not None and peek().upper() == "NOT"
                if negate:
                    take()
                if (take() or "").upper() != "NULL":
                    raise ValueError("Unsupported Delta CHECK IS expression")
                left = ("is_not_null" if negate else "is_null", left)
                continue
            priority = precedence.get(operator, -1)
            if priority < minimum:
                break
            take()
            left = (names[operator], left, expression(priority + 1))
        return left

    result = expression()
    if position != len(tokens):
        raise ValueError("Unsupported Delta CHECK syntax")
    return result


def _check_tree(expression, case_sensitive: bool):
    if expression.kind == "field":
        name = expression.data["field"]
        return ("field", name if case_sensitive else name.casefold())
    if expression.kind == "literal":
        value = expression.data["value"]
        return (
            ("number", str(value))
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else ("literal", value)
        )
    if expression.kind in {
        "eq",
        "ne",
        "gt",
        "lt",
        "ge",
        "le",
        "and",
        "or",
        "add",
        "sub",
        "mul",
        "div",
        "mod",
        "not",
        "neg",
        "is_null",
        "is_not_null",
        "is_nan",
    }:
        return (expression.kind, *(_check_tree(argument, case_sensitive) for argument in expression.args))
    raise ValueError(
        f"Unsupported Structure CHECK expression {expression.kind}; use delta_check_match='name' if appropriate"
    )
