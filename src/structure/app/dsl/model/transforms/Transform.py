from __future__ import annotations

from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline


class Transform:

    _structure_inputs: dict[str, InputDeclaration] = {}
    _structure_lanes: dict[str, LaneDeclaration] = {}
    _structure_outputs: dict[str, OutputDeclaration] = {}
    _structure_pipeline: TransformPipeline | None = None

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        inputs: dict[str, InputDeclaration] = {}
        lanes: dict[str, LaneDeclaration] = {}
        outputs: dict[str, OutputDeclaration] = {}
        for base in cls.__bases__:
            inputs.update(getattr(base, "_structure_inputs", {}))
            lanes.update(getattr(base, "_structure_lanes", {}))
            outputs.update(getattr(base, "_structure_outputs", {}))

        for value in cls.__dict__.values():
            if isinstance(value, InputDeclaration):
                inputs[value.name] = value
            if isinstance(value, LaneDeclaration):
                lanes[value.name] = value
            if isinstance(value, OutputDeclaration):
                outputs[value.name] = value

        cls._structure_inputs = inputs
        cls._structure_lanes = lanes
        cls._structure_outputs = outputs
        pipelines = [value for value in cls.__dict__.values() if isinstance(value, TransformPipeline)]
        if len(pipelines) > 1:
            raise TypeError(f"{cls.__name__} declares more than one transform pipeline field")
        cls._structure_pipeline = pipelines[0] if pipelines else None

    def __init__(self, **inputs: object) -> None:
        unknown = set(inputs) - set(self._structure_inputs)
        if unknown:
            allowed = ", ".join(self._structure_inputs)
            raise TypeError(
                f"{type(self).__name__} got unknown input(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_bound_inputs = dict(inputs)

    def run(self, session):
        return session.run(self)

    def to(self, *stages: "Transform") -> TransformPipeline:
        return TransformPipeline((self, *stages))
