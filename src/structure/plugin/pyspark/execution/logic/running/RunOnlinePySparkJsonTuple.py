from __future__ import annotations


class RunOnlinePySparkJsonTuple:
    """Apply a typed row-preserving JSON tuple operation to a live DataFrame."""

    def __call__(self, frame, generator, *, functions, value):
        json_fields = tuple(json_name for _, json_name in generator.fields)
        output_fields = tuple(generator.schema._structure_fields[name].column for name, _ in generator.fields)
        return frame.select("*", functions.json_tuple(value, *json_fields).alias(*output_fields))
