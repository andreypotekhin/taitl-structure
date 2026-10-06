"""Late-bound Delta validation and mutation execution."""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime
from decimal import Decimal
from importlib.metadata import PackageNotFoundError, version
from zoneinfo import ZoneInfo

from structure.plugin.pyspark.delta.checks import bind_checks
from structure.plugin.pyspark.delta.schema import resolve_delta_columns


def validate_delta_table(table, schema, *, mode: str = "expression"):
    validated_delta_frame(table, schema, modes=(mode,))
    return table


def validated_delta_frame(table, schema, *, modes=("expression",)):
    """Validate a caller-owned table and return its single refreshed binding frame."""
    try:
        from delta.tables import DeltaTable  # type: ignore[import-not-found]
        from pyspark.sql import types as T
    except ImportError as error:
        raise RuntimeError("Delta table bindings require the optional delta-spark and pyspark packages") from error
    connect_table = False
    if not isinstance(table, DeltaTable):
        try:
            from delta.connect.tables import DeltaTable as ConnectDeltaTable  # type: ignore[import-not-found]
            from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
        except ImportError:
            raise TypeError(f"{schema.__name__} Delta binding requires a native DeltaTable") from None
        if not isinstance(table, ConnectDeltaTable):
            raise TypeError(f"{schema.__name__} Delta binding requires a native DeltaTable")
        connect_table = True
        if not isinstance(table.toDF().sparkSession, ConnectSparkSession):
            raise TypeError(f"{schema.__name__} Delta Connect binding requires a Connect session")
    require_compatible_delta_runtime(table)
    from structure.plugin.pyspark.api.PySpark import PySpark

    expected = PySpark.schema.materialize()(schema, types=T)
    frame = fresh_delta_frame(table)
    actual = frame.schema
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
    if not connect_table:
        _validate_delta_column_metadata(table, schema, actual)
    if set(modes) == {"off"}:
        return frame
    checks = bind_checks(schema)
    if not checks:
        return frame
    properties = table.detail().select("properties").first()["properties"] or {}
    native = {
        key[len("delta.constraints.") :].casefold(): value
        for key, value in properties.items()
        if key.casefold().startswith("delta.constraints.")
    }
    case_sensitive = frame.sparkSession.conf.get("spark.sql.caseSensitive", "false").lower() == "true"
    for check in checks:
        stored = native.get(check.name.casefold())
        if stored is None:
            raise ValueError(
                f"Delta table for {schema.__name__} is missing CHECK {check.name}; provision it before running the transform"
            )
        if "expression" in modes and _check_tree(check.predicate, case_sensitive) != _sql_tree(stored, case_sensitive):
            raise ValueError(f"Delta CHECK {check.name} differs from {schema.__name__}.constraints")
    return frame


def require_compatible_delta_runtime(table) -> None:
    """Reject Spark and Delta package pairs outside the project's evidenced matrix."""
    try:
        delta_version = version("delta-spark")
    except PackageNotFoundError as error:
        raise RuntimeError(
            "Delta table bindings require delta-spark; install the Delta version supported by this PySpark profile"
        ) from error
    spark_version = table.toDF().sparkSession.version
    try:
        spark_parts = tuple(int(part) for part in spark_version.split(".")[:3])
        delta_line = ".".join(delta_version.split(".")[:2])
    except (AttributeError, TypeError, ValueError):
        raise RuntimeError(
            f"Cannot identify the active Spark/Delta versions ({spark_version!r}, {delta_version!r})"
        ) from None
    compatible = (
        (spark_parts[:2] == (3, 5) and spark_parts >= (3, 5, 3) and delta_line == "3.3")
        or (spark_parts[:2] == (4, 0) and delta_line == "4.0")
        or (spark_parts[:2] == (4, 1) and delta_line == "4.1")
    )
    if not compatible:
        supported = (
            "PySpark 3.5.3+ with Delta 3.3.x, PySpark 4.0.x with Delta 4.0.x, "
            "or PySpark 4.1.x with Delta 4.1.x"
        )
        raise RuntimeError(
            f"Unsupported Spark/Delta runtime pair: PySpark {spark_version} with delta-spark {delta_version}. "
            f"Supported pairs are {supported}."
        )


def _validate_delta_column_metadata(table, schema, spark_schema) -> None:
    declarations = resolve_delta_columns(schema)
    if not declarations:
        return
    delta_metadata = (
        _delta_log_field_metadata(table)
        if any(declaration.kind in {"generated", "identity"} for declaration in declarations.values())
        else {}
    )
    spark_fields = {field.name: field for field in spark_schema}
    structure_fields = schema._structure_fields
    for name, declaration in declarations.items():
        field = structure_fields[name]
        native = delta_metadata.get(field.column, {})
        if declaration.kind == "generated":
            actual = native.get("delta.generationExpression")
            expected = str(declaration.value)
            if not isinstance(actual, str) or _compact_sql(actual) != _compact_sql(expected):
                raise ValueError(
                    f"Delta table for {schema.__name__}.{name} does not have the declared generated expression "
                    f"{expected!r}"
                )
        elif declaration.kind == "identity":
            expected_mode = declaration.mode == "by_default"
            if (
                native.get("delta.identity.start") != declaration.start
                or native.get("delta.identity.step") != declaration.step
                or native.get("delta.identity.allowExplicitInsert", False) is not expected_mode
            ):
                raise ValueError(
                    f"Delta table for {schema.__name__}.{name} does not match the declared identity mode, start, "
                    "and step"
                )
        elif declaration.kind == "default":
            actual = spark_fields[field.column].metadata.get("CURRENT_DEFAULT")
            expected = _sql_literal(declaration.value)
            if actual != expected:
                raise ValueError(
                    f"Delta table for {schema.__name__}.{name} does not have the declared default {expected!r}"
                )


def _delta_log_field_metadata(table) -> dict[str, dict[str, object]]:
    frame = table.toDF()
    spark = frame.sparkSession
    location = table.detail().select("location").first()["location"]
    log = spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession,
        spark._jvm.org.apache.hadoop.fs.Path(location),
    )
    schema = json.loads(log.unsafeVolatileSnapshot().metadata().schemaString())
    return {field["name"]: field.get("metadata", {}) for field in schema.get("fields", ())}


def _compact_sql(source: str) -> str:
    compact: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(source):
        character = source[index]
        if quote is not None:
            compact.append(character)
            if character == quote:
                if index + 1 < len(source) and source[index + 1] == quote:
                    compact.append(source[index + 1])
                    index += 1
                else:
                    quote = None
        elif character in {"'", '"', "`"}:
            quote = character
            compact.append(character)
        elif not character.isspace():
            compact.append(character)
        index += 1
    return "".join(compact)


def fresh_delta_frame(table):
    """Read the latest committed snapshot without replacing the caller's handle."""
    from delta.tables import DeltaTable  # type: ignore[import-not-found]

    frame = table.toDF()
    location = table.detail().select("location").first()["location"]
    if not isinstance(table, DeltaTable):
        from delta.connect.tables import DeltaTable as ConnectDeltaTable  # type: ignore[import-not-found]

        if isinstance(table, ConnectDeltaTable):
            return ConnectDeltaTable.forPath(frame.sparkSession, location).toDF()
    return DeltaTable.forPath(frame.sparkSession, location).toDF()


def execute_delta_mutation(mutation, *, tables, frames, functions):
    table = tables[mutation.target]
    from structure.plugin.pyspark.api.PySpark import PySpark

    evaluator = PySpark.execution.expression()
    aliases = {mutation.target_scope: ""}

    def column(expression, mapping=aliases):
        return evaluator.evaluate(expression, functions=functions, aliases=mapping)

    if mutation.kind == "delete":
        assert mutation.predicate is not None
        table.delete(column(mutation.predicate))
        return
    if mutation.kind == "update":
        assert mutation.predicate is not None
        table.update(
            condition=column(mutation.predicate), set={name: column(value) for name, value in mutation.assignments}
        )
        return
    if mutation.kind == "replace_where":
        if mutation.source is None or mutation.predicate is None:
            raise ValueError("Delta replaceWhere mutation is missing its source or predicate")
        location = table.detail().select("location").first()["location"]
        predicate = delta_predicate_sql(mutation.predicate)
        frames[mutation.source].write.format("delta").mode("overwrite").option(
            "replaceWhere", predicate
        ).save(location)
        return
    if mutation.kind == "restore":
        execute_delta_restore(table, _selector_value(mutation.selector), mutation.selector_type)
        return
    if mutation.kind == "optimize":
        optimize_predicate = None if mutation.predicate is None else delta_predicate_sql(mutation.predicate)
        execute_delta_optimize(
            table,
            optimize_predicate,
            mutation.action,
            mutation.columns,
            tuple(_expression_fields(mutation.predicate)),
        )
        return
    if mutation.kind == "vacuum":
        retention = _selector_value(mutation.selector)
        execute_delta_vacuum(table, retention, mutation.allow_short_retention)
        return
    if mutation.kind not in {"merge", "append"} or mutation.source is None:
        raise ValueError(f"Unknown Delta mutation {mutation.kind!r}")
    source = frames[mutation.source]
    if mutation.kind == "append":
        location = table.detail().select("location").first()["location"]
        writer = source.write.format("delta").mode("append")
        if mutation.schema_evolution:
            writer = writer.option("mergeSchema", "true")
        writer.save(location)
        return
    if mutation.source_scope is None or mutation.predicate is None:
        raise ValueError("Delta merge mutation is missing its source scope or match predicate")
    mapping = {mutation.target_scope: "target", mutation.source_scope: "source"}

    def merged(expression):
        return column(expression, mapping)

    builder = table.alias("target").merge(source.alias("source"), merged(mutation.predicate))
    if mutation.schema_evolution:
        builder = builder.withSchemaEvolution()
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


def read_delta_relation(
    mutation, *, tables, spark, evaluator, functions, check_cdf_configuration=True
):
    """Open a snapshot or bounded CDF range from a caller-owned Delta handle."""
    table = tables[mutation.target]
    selector = _selector_value(mutation.selector)
    end = _selector_value(mutation.end_selector) if mutation.end_selector is not None else None
    selector_type = (
        mutation.selector_type
        or (mutation.selector.type.name if mutation.selector is not None and mutation.selector.type else "")
    )
    return open_delta_relation(
        table,
        mutation.kind,
        selector,
        end,
        selector_type,
        output_schema=mutation.output_schema,
        check_cdf_configuration=check_cdf_configuration,
        spark=spark,
    )


def _validate_delta_cdf_configuration(properties, spark) -> None:
    if properties.get("delta.enablechangedatafeed") != "true":
        raise ValueError(
            "Delta change feed is not enabled; set the table property delta.enableChangeDataFeed=true before writing changes"
        )
    extensions = spark.conf.get("spark.sql.extensions", "")
    catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")
    if "delta" not in extensions.casefold() or "delta" not in catalog.casefold():
        raise RuntimeError(
            "Delta CDF requires a Spark session with Delta SQL extension and catalog configuration; "
            "set delta_cdf_checks=False only when your environment validates these requirements another way"
        )


def validate_delta_relation(frame, schema):
    """Validate the declared result fields against a resolved Delta read schema."""
    from pyspark.sql import types as T

    from structure.plugin.pyspark.api.PySpark import PySpark

    expected = PySpark.schema.materialize()(schema, types=T)
    actual_fields = {field.name: field for field in frame.schema}
    validator = PySpark.execution.validator()
    for field in expected:
        actual = actual_fields.get(field.name)
        if actual is None:
            raise ValueError(
                f"Delta read declared as {schema.__name__} is missing column {field.name!r}; "
                "check the result Schema and physical aliases"
            )
        if not validator._same_data_type(actual.dataType, field.dataType):
            raise ValueError(
                f"Delta read column {field.name!r} has a type incompatible with {schema.__name__}.{field.name}"
            )
        if actual.nullable and not field.nullable:
            raise ValueError(
                f"Delta read column {field.name!r} may be null, but {schema.__name__}.{field.name} is non-nullable"
            )
    return frame


def open_delta_relation(
    table, kind, selector, end, selector_type, *, output_schema=None, check_cdf_configuration=True, spark
):
    if kind in {"delta_history", "delta_detail"}:
        frame = table.history() if kind == "delta_history" and selector is None else None
        if kind == "delta_history" and selector is not None:
            if isinstance(selector, bool) or not isinstance(selector, int) or selector <= 0:
                raise ValueError("delta_history(limit=...) must be a positive integer")
            frame = table.history(selector)
        elif kind == "delta_detail":
            frame = table.detail()
        return validate_delta_relation(frame, output_schema) if output_schema is not None else frame
    details = table.detail().first().asDict(recursive=True)
    properties = {str(key).casefold(): str(value).casefold() for key, value in (details.get("properties") or {}).items()}
    if kind == "delta_changes" and check_cdf_configuration:
        _validate_delta_cdf_configuration(properties, spark)
    if selector_type == "timestamp":
        if isinstance(selector, str):
            selector = datetime.fromisoformat(selector)
        if not isinstance(selector, datetime) or selector.tzinfo is None or selector.utcoffset() is None:
            raise TypeError("Delta timestamp selectors must be timezone-aware datetime values")
        selector = selector.astimezone(ZoneInfo(spark.conf.get("spark.sql.session.timeZone", "UTC"))).replace(
            tzinfo=None
        ).isoformat(sep=" ")
        if isinstance(end, datetime):
            if end.tzinfo is None or end.utcoffset() is None:
                raise TypeError("Delta timestamp selectors must be timezone-aware datetime values")
            end = end.astimezone(ZoneInfo(spark.conf.get("spark.sql.session.timeZone", "UTC"))).replace(
                tzinfo=None
            ).isoformat(sep=" ")
        elif isinstance(end, str):
            parsed_end = datetime.fromisoformat(end)
            if parsed_end.tzinfo is None or parsed_end.utcoffset() is None:
                raise TypeError("Delta timestamp selectors must be timezone-aware datetime values")
            end = parsed_end.astimezone(ZoneInfo(spark.conf.get("spark.sql.session.timeZone", "UTC"))).replace(
                tzinfo=None
            ).isoformat(sep=" ")
    elif isinstance(selector, bool) or not isinstance(selector, int):
        raise TypeError("Delta version selectors must be integers")
    elif end is not None and (isinstance(end, bool) or not isinstance(end, int)):
        raise TypeError("Delta version selectors must be integers")
    location = details["location"]
    reader = spark.read.format("delta")
    if kind == "delta_snapshot":
        option = "timestampAsOf" if selector_type == "timestamp" else "versionAsOf"
        frame = reader.option(option, selector).load(location)
        return validate_delta_relation(frame, output_schema) if output_schema is not None else frame
    option = "startingTimestamp" if selector_type == "timestamp" else "startingVersion"
    reader = reader.option("readChangeFeed", "true").option(option, selector)
    if end is not None:
        end_option = "endingTimestamp" if selector_type == "timestamp" else "endingVersion"
        reader = reader.option(end_option, end)
    frame = reader.load(location)
    return validate_delta_relation(frame, output_schema) if output_schema is not None else frame


def _selector_value(expression):
    if expression is None:
        return None
    if expression.kind == "literal":
        return expression.data["value"]
    if expression.kind == "variable":
        from structure.plugin.pyspark.dsl.RuntimeVariables import runtime_variable

        return runtime_variable(str(expression.data["name"]))
    raise TypeError("Delta snapshot and CDF selectors must be literals or runtime variable references")


def _expression_fields(expression):
    if expression is None:
        return set()
    fields = set()
    if expression.kind == "field":
        data = expression.data or {}
        fields.add(str(data.get("field", "")))
    for argument in expression.args:
        fields.update(_expression_fields(argument))
    return fields


def execute_delta_restore(table, selector, selector_type):
    if selector_type == "version":
        if isinstance(selector, bool) or not isinstance(selector, int) or selector < 0:
            raise ValueError("Delta restore version must be a nonnegative integer")
        return table.restoreToVersion(selector)
    if selector_type == "timestamp":
        if isinstance(selector, str):
            selector = datetime.fromisoformat(selector)
        if not isinstance(selector, datetime):
            raise TypeError("Delta restore timestamp must be a datetime value")
        return table.restoreToTimestamp(selector.isoformat(sep=" "))
    raise ValueError("Delta restore is missing its version or timestamp selector")


def execute_delta_optimize(table, predicate, action, columns, predicate_columns=()):
    partition_columns = set(table.detail().first().asDict(recursive=True).get("partitionColumns") or ())
    invalid = set(predicate_columns) - partition_columns
    if invalid:
        raise ValueError(
            f"Delta optimize where=... may reference only partition columns; invalid: {', '.join(sorted(invalid))}"
        )
    builder = table.optimize()
    if predicate is not None:
        builder = builder.where(predicate)
    if action == "compaction":
        return builder.executeCompaction()
    if action == "zorder":
        return builder.executeZOrderBy(list(columns))
    raise ValueError(f"Unknown Delta optimize action {action!r}")


def execute_delta_vacuum(table, retention, allow_short_retention=False):
    if isinstance(retention, bool) or not isinstance(retention, (int, float, Decimal)):
        raise TypeError("Delta vacuum retention_hours must be numeric")
    if not math.isfinite(retention) or retention < 0:
        raise ValueError("Delta vacuum retention_hours must be finite and nonnegative")
    if retention < 168 and not allow_short_retention:
        raise ValueError("retention below 168 hours requires allow_short_retention=True")
    return table.vacuum(float(retention))


def delta_predicate_sql(expression, *, variables=None) -> str:
    """Render the deliberately small safe SQL subset accepted by Delta replaceWhere."""
    variables = variables or {}
    if expression.kind == "field":
        return ".".join(f"`{str(part).replace('`', '``')}`" for part in expression.data["path"])
    if expression.kind == "literal":
        return _sql_literal(expression.data["value"])
    if expression.kind == "variable":
        name = str(expression.data["name"])
        if name not in variables:
            from structure.plugin.pyspark.dsl.RuntimeVariables import runtime_variable

            value = runtime_variable(name)
        else:
            value = variables[name]
        return _sql_literal(value)
    unary = {"not": "NOT", "neg": "-"}
    if expression.kind in unary:
        return f"{unary[expression.kind]} ({delta_predicate_sql(expression.args[0], variables=variables)})"
    binary = {
        "and": "AND", "or": "OR", "eq": "=", "ne": "<>", "gt": ">", "lt": "<",
        "ge": ">=", "le": "<=", "add": "+", "sub": "-", "mul": "*", "div": "/", "mod": "%",
    }
    if expression.kind in binary:
        left, right = (delta_predicate_sql(item, variables=variables) for item in expression.args)
        return f"({left} {binary[expression.kind]} {right})"
    if expression.kind in {"is_null", "is_not_null"}:
        operator = "IS NULL" if expression.kind == "is_null" else "IS NOT NULL"
        return f"({delta_predicate_sql(expression.args[0], variables=variables)} {operator})"
    raise TypeError(f"delta_replace_where(where=...) does not support {expression.kind!r} expressions")


def render_delta_predicate_template(expression) -> str:
    """Render SQL containing named markers for runtime variable values."""
    if expression.kind == "variable":
        return "{{" + str(expression.data["name"]) + "}}"
    if expression.kind == "field":
        return ".".join(f"`{str(part).replace('`', '``')}`" for part in expression.data["path"])
    if expression.kind == "literal":
        return _sql_literal(expression.data["value"])
    unary = {"not": "NOT", "neg": "-"}
    if expression.kind in unary:
        return f"{unary[expression.kind]} ({render_delta_predicate_template(expression.args[0])})"
    binary = {
        "and": "AND", "or": "OR", "eq": "=", "ne": "<>", "gt": ">", "lt": "<",
        "ge": ">=", "le": "<=", "add": "+", "sub": "-", "mul": "*", "div": "/", "mod": "%",
    }
    if expression.kind in binary:
        left, right = (render_delta_predicate_template(item) for item in expression.args)
        return f"({left} {binary[expression.kind]} {right})"
    if expression.kind in {"is_null", "is_not_null"}:
        operator = "IS NULL" if expression.kind == "is_null" else "IS NOT NULL"
        return f"({render_delta_predicate_template(expression.args[0])} {operator})"
    raise TypeError(f"delta_replace_where(where=...) does not support {expression.kind!r} expressions")


def bind_delta_predicate_variables(template: str, variables) -> str:
    for name, value in variables.items():
        template = template.replace("{{" + name + "}}", _sql_literal(value))
    if "{{" in template or "}}" in template:
        raise ValueError("Delta replaceWhere predicate has an unbound runtime variable")
    return template


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return "TIMESTAMP '" + value.isoformat(sep=" ") + "'" if isinstance(value, datetime) else "DATE '" + value.isoformat() + "'"
    if isinstance(value, (str, bytes)):
        text = value.decode("utf-8") if isinstance(value, bytes) else value
        return "'" + text.replace("'", "''") + "'"
    raise TypeError(f"Unsupported Delta replaceWhere literal {type(value).__name__}")


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
