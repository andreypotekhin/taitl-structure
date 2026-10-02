from __future__ import annotations


def normalize_sql_command_result(result, *, label: str | None, schema, functions, types):
    """Return one command-result row, filling an absent label and metrics with nulls."""
    fields = tuple(schema.fields)
    label_field = next(field for field in fields if field.name == "label")
    metric_fields = tuple(field for field in fields if field.name != "label")
    key = "__structure_sql_command_result_key"

    defaults = result.sparkSession.range(1).select(
        functions.lit(1).alias(key),
        functions.lit(label).cast(label_field.dataType).alias(label_field.name),
        *(functions.lit(None).cast(field.dataType).alias(field.name) for field in metric_fields),
    )
    available = set(result.columns)
    metrics = result.limit(1).select(
        functions.lit(1).alias(key),
        *(
            (
                functions.col(field.name).cast(field.dataType)
                if field.name in available
                else functions.lit(None).cast(field.dataType)
            ).alias(field.name)
            for field in metric_fields
        ),
    )
    return defaults.join(metrics, on=key, how="left").select(*(field.name for field in fields))
