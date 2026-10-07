from __future__ import annotations


def execute_pyspark_sql(
    spark, statement: str, *, args, relations, step: str, label: str | None, command_result: bool = False
):
    """Call Spark SQL and annotate immediate backend failures without changing their type."""
    try:
        result = spark.sql(statement, **relations) if args is None else spark.sql(statement, args=args, **relations)
        if command_result:
            # Command receipts must not retain an effectful Spark plan: reading the
            # receipt again must never submit the native command a second time.
            rows = result.collect()
            schema = result.schema
            if not schema.fields:
                return spark.range(0).selectExpr("CAST(NULL AS INT) AS __structure_empty_command_result")
            return spark.createDataFrame(rows, schema=schema)
        return result
    except Exception as error:
        command_label = f", label={label!r}" if label is not None else ""
        error.add_note(
            f"Structure SQL failed in Transform step {step!r}{command_label}. "
            "See docs/dev/design/TypedSqlExecution.design.md."
        )
        raise
