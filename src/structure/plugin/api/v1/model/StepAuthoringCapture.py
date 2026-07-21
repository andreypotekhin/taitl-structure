from dataclasses import dataclass


@dataclass(frozen=True)
class StepAuthoringCapture:
    """Opaque plugin body and diagnostics captured from one Core-owned step invocation."""

    body: object
    diagnostics: tuple[object, ...] = ()
