from dataclasses import dataclass

from structure.plugin.api.v1.model.StepSinkCapture import StepSinkCapture


@dataclass(frozen=True)
class StepAuthoringCapture:
    """Opaque plugin body and diagnostics captured from one Core-owned step invocation."""

    body: object
    diagnostics: tuple[object, ...] = ()
    sinks: tuple[StepSinkCapture, ...] = ()
    effect: bool = False
