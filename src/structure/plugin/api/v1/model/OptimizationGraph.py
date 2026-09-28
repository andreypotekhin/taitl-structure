"""Plugin-neutral facts for removing whole executable steps."""

from dataclasses import dataclass


@dataclass(frozen=True)
class OptimizationStep:
    """Dependencies refer to earlier step identities, not mutable frame names."""

    name: str
    dependencies: tuple[str, ...] = ()
    removable: bool = False
    reason: str = "the plugin has not certified removal safety"


@dataclass(frozen=True)
class OptimizationGraph:
    steps: tuple[OptimizationStep, ...]
    output_roots: tuple[str, ...] = ()
    stage_roots: tuple[str, ...] = ()
    allow_stage_outputs: bool = True
