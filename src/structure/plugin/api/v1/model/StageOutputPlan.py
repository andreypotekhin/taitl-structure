from dataclasses import dataclass

from structure.plugin.api.v1.model.OutputPlan import OutputPlan


@dataclass(frozen=True)
class StageOutputPlan:
    """A public output of a composed stage, addressed by its stage path."""

    path: tuple[str, ...]
    output: OutputPlan
