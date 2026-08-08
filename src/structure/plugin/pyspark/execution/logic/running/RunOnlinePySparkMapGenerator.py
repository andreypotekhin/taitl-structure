from __future__ import annotations


class RunOnlinePySparkMapGenerator:
    """Apply typed primitive-map generators to online PySpark DataFrames."""

    def __call__(self, frame, generator, *, functions, types, value):
        prefix = f"__structure_{generator.scope}"
        position, key, item = f"{prefix}_pos", f"{prefix}_key", f"{prefix}_value"
        expanded = frame.select(
            "*",
            getattr(functions, generator.function)(value).alias(
                *((position, key, item) if generator.ordinal is not None else (key, item))
            ),
        )
        for temporary, field_name in ((key, generator.key_field), (item, generator.value_field)):
            expanded = expanded.withColumn(
                generator.schema._structure_fields[field_name].column,
                functions.col(temporary),
            )
        if generator.ordinal is not None:
            expanded = expanded.withColumn(
                generator.schema._structure_fields[generator.ordinal].column,
                functions.col(position).cast(types.LongType()),
            )
        return expanded.drop(*((position, key, item) if generator.ordinal is not None else (key, item)))
