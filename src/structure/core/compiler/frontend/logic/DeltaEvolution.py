"""Find Delta output declarations produced from differently shaped Delta inputs."""

from __future__ import annotations

import inspect
from typing import get_type_hints

from structure.core.dsl.model.transforms.Transform import Transform


def evolving_delta_outputs(transform_class: type[Transform]) -> frozenset[str]:
    outputs = {
        declaration.schema: name
        for name, declaration in transform_class._structure_outputs.items()
        if declaration.binding == "delta"
    }
    if not outputs:
        return frozenset()
    input_schemas = {
        declaration.schema
        for name, declaration in transform_class._structure_inputs.items()
        if declaration.binding == "delta" and name not in transform_class._structure_outputs
    }
    result: set[str] = set()
    for _, member in inspect.getmembers(transform_class, inspect.isfunction):
        if member.__name__.startswith("_"):
            continue
        try:
            hints = get_type_hints(member)
        except (NameError, TypeError):
            continue
        output_schema = hints.get("return")
        if output_schema not in outputs:
            continue
        if any(hints.get(name) in input_schemas and hints.get(name) is not output_schema for name in inspect.signature(member).parameters if name != "self"):
            result.add(outputs[output_schema])
    return frozenset(result)
