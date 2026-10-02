from __future__ import annotations


def execute_pyspark_sql(spark, statement: str, *, args, relations, step: str, label: str | None):
    """Call Spark SQL and annotate immediate backend failures without changing their type."""
    try:
        return spark.sql(statement, **relations) if args is None else spark.sql(statement, args=args, **relations)
    except Exception as error:
        command_label = f", label={label!r}" if label is not None else ""
        error.add_note(
            f"Structure SQL failed in Transform step {step!r}{command_label}. "
            "See docs/dev/design/TypedSqlExecution.design.md."
        )
        raise
