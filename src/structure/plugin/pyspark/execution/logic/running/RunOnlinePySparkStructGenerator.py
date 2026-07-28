from __future__ import annotations


class RunOnlinePySparkStructGenerator:
    """Apply typed array-of-struct generators to online PySpark DataFrames."""

    def __call__(self, frame, generator, *, functions, types, value):
        prefix = f"__structure_{generator.scope}"
        position = f"{prefix}_pos"
        item = f"{prefix}_item"
        expanded = frame.select("*", self._generator(functions, generator, value, position, item))
        if generator.ordinal is not None:
            expanded = expanded.withColumn(
                generator.schema._structure_fields[generator.ordinal].column,
                functions.col(position).cast(types.LongType()),
            )
        for name, field in generator.schema._structure_fields.items():
            if name == generator.ordinal:
                continue
            expanded = expanded.withColumn(field.column, functions.col(f"{item}.{field.column}"))
        return expanded.drop(*self._drop_columns(generator, position, item))

    def _generator(self, functions, generator, value, position, item):
        if generator.function == "explode":
            return functions.explode(value).alias(item)
        return functions.posexplode(value).alias(position, item)

    def _drop_columns(self, generator, position, item):
        if generator.ordinal is None:
            return (item,)
        return (position, item)
