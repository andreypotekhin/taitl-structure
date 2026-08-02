from __future__ import annotations


class RunOnlinePySparkStructGenerator:
    """Apply typed array-of-struct generators to online PySpark DataFrames."""

    def __call__(self, frame, generator, *, functions, types, value):
        if generator.tvf:
            return self._variant_tvf(frame, generator, value=value, functions=functions)
        prefix = f"__structure_{generator.scope}"
        position = f"{prefix}_pos"
        item = f"{prefix}_item"
        expanded = frame.select("*", self._generator(functions, generator, value, position, item))
        if self._inline(generator):
            return self._inline_fields(expanded, generator, functions=functions, prefix=position)
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

    def _variant_tvf(self, frame, generator, *, value, functions):
        from pyspark.sql import types

        function = generator.function
        tvf = getattr(frame.sparkSession.tvf, function)(value.outer()).select(
            functions.col("pos").alias("__structure_variant_pos"),
            functions.col("key").alias("__structure_variant_key"),
            functions.col("value").alias("__structure_variant_value"),
        )
        expanded = frame.lateralJoin(tvf, how="left" if generator.outer else "cross")
        columns = {
            "pos": "__structure_variant_pos",
            "key": "__structure_variant_key",
            "value": "__structure_variant_value",
        }
        for name, field in generator.schema._structure_fields.items():
            column = functions.col(columns[name])
            if getattr(field.type, "name", None) == "long":
                column = column.cast(types.LongType())
            expanded = expanded.withColumn(field.column, column)
        return expanded.drop(*columns.values())

    def _generator(self, functions, generator, value, position, item):
        if generator.function == "explode_outer":
            return functions.explode_outer(value).alias(item)
        if generator.function == "explode":
            return functions.explode(value).alias(item)
        if generator.function == "posexplode_outer":
            return functions.posexplode_outer(value).alias(position, item)
        if generator.function == "inline_outer":
            return functions.inline_outer(value).alias(*self._inline_aliases(generator, prefix=position))
        if generator.function == "inline":
            return functions.inline(value).alias(*self._inline_aliases(generator, prefix=position))
        return functions.posexplode(value).alias(position, item)

    def _drop_columns(self, generator, position, item):
        if generator.ordinal is None:
            return (item,)
        return (position, item)

    def _inline(self, generator):
        return generator.function in {"inline", "inline_outer"}

    def _inline_fields(self, frame, generator, *, functions, prefix):
        temporaries = self._inline_aliases(generator, prefix=prefix)
        expanded = frame
        for temporary, field in zip(temporaries, generator.schema._structure_fields.values(), strict=True):
            expanded = expanded.withColumn(field.column, functions.col(temporary))
        return expanded.drop(*temporaries)

    def _inline_aliases(self, generator, *, prefix):
        return tuple(f"{prefix}_{field.column}" for field in generator.schema._structure_fields.values())
