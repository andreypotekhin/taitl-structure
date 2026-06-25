from __future__ import annotations

from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration


class Transform:

    _structure_inputs: dict[str, InputDeclaration] = {}
    _structure_lanes: dict[str, LaneDeclaration] = {}
    _structure_outputs: dict[str, OutputDeclaration] = {}

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

    def __init__(self, **inputs: object) -> None:
        unknown = set(inputs) - set(self._structure_inputs)
        if unknown:
            allowed = ", ".join(self._structure_inputs)
            raise TypeError(
                f"{type(self).__name__} got unknown input(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_bound_inputs = dict(inputs)
        self.schemas: object | None = None

    def run(self, session):
        return session.run(self)
