import hashlib
import json
from dataclasses import asdict, dataclass

from structure.plugin.api.v1 import CompilerAPI as CompilerAPIV1
from structure.plugin.api.v1 import CompileRequest, PluginCompilation, TransformPlan

from ..authoring.Authoring import IterableStepBody


@dataclass(frozen=True)
class IterableStep:
    name: str
    inputs: tuple[str, ...]
    results: tuple[str, ...]
    body: IterableStepBody


@dataclass(frozen=True)
class IterableRecipe:
    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    steps: tuple[IterableStep, ...]


class Compiler(CompilerAPIV1):
    """Lowers Core's structural plan and Iterable-owned symbolic bodies to a finite recipe."""

    def compile(self, request: CompileRequest) -> PluginCompilation:
        plan = request.analysis
        if not isinstance(plan, TransformPlan):
            raise ValueError("PLUGIN-E2708: Iterable compilation requires a Core TransformPlan analysis.")
        steps = tuple(self._step(step) for step in plan.steps)
        recipe = IterableRecipe(
            name=plan.name,
            inputs=tuple(input.name for input in plan.inputs),
            outputs=tuple(output.name for output in plan.outputs),
            steps=steps,
        )
        encoded = json.dumps(asdict(recipe), default=self._json, sort_keys=True, separators=(",", ":"))
        return PluginCompilation(lowered=recipe, fingerprint=f"iterable:{hashlib.sha256(encoded.encode()).hexdigest()}")

    def _step(self, step) -> IterableStep:
        if not isinstance(step.plugin_body, IterableStepBody):
            raise ValueError(f"PLUGIN-E2708: Iterable step {step.name!r} has no Iterable-authored body.")
        return IterableStep(
            name=step.name,
            inputs=tuple(item.lane for item in step.inputs),
            results=tuple(item.lane for item in step.results),
            body=step.plugin_body,
        )

    @staticmethod
    def _json(value: object) -> object:
        if isinstance(value, type):
            return f"{value.__module__}.{value.__qualname__}"
        raise TypeError(f"Cannot serialize Iterable recipe value {value!r}.")
