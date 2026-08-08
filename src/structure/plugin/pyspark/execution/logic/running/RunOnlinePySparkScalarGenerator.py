from __future__ import annotations


class RunOnlinePySparkScalarGenerator:
    """Apply typed primitive-array generators to online PySpark DataFrames."""

    def __call__(self, frame, generator, *, functions, types, value):
        prefix = f"__structure_{generator.scope}"
        position = f"{prefix}_pos"
        item = f"{prefix}_item"
        expand = getattr(functions, generator.function)
        expanded = frame.select(
            "*",
            expand(value).alias(*((position, item) if generator.ordinal is not None else (item,))),
        )
        value_column = generator.schema._structure_fields[generator.value_field].column
        expanded = expanded.withColumn(value_column, functions.col(item))
        if generator.ordinal is not None:
            ordinal_column = generator.schema._structure_fields[generator.ordinal].column
            expanded = expanded.withColumn(ordinal_column, functions.col(position).cast(types.LongType()))
        return expanded.drop(position, item)
