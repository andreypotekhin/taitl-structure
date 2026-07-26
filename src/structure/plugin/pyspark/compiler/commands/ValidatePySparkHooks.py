import inspect

from structure import StructureCompileError
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model import HookPlan, TransformPlan


class ValidatePySparkHooks:
    """Validate PySpark-only hook targeting and runtime parameters."""

    def __call__(self, plan: TransformPlan) -> None:
        for step in plan.steps:
            self._all((*step.before_hooks, *step.after_hooks))
            for result in step.results:
                self._all(result.after_hooks)

    def _all(self, hooks: tuple[HookPlan, ...]) -> None:
        for hook in hooks:
            self._hook(hook)

    def _hook(self, hook: HookPlan) -> None:
        origin = hook.origin
        owner = getattr(origin, "owner", None)
        if not isinstance(owner, type):
            return
        if "all" not in hook.targets and "pyspark" not in hook.targets:
            targets = ", ".join(hook.targets)
            self._error(
                hook,
                f"{owner.__name__}.{hook.name} targets {targets}, but v1 active hook execution is PySpark only.",
                'Use target="pyspark" for v1, or keep non-PySpark hook declarations for a future plugin.',
                target=targets,
            )
        parameters = list(inspect.signature(getattr(owner, hook.name)).parameters.values())
        if not parameters or parameters[0].name != "self":
            self._signature_error(hook, owner, f"{owner.__name__}.{hook.name} must declare self.")
        runtime = parameters[1:]
        if any(parameter.kind is not inspect.Parameter.KEYWORD_ONLY for parameter in runtime):
            self._signature_error(hook, owner, f"{owner.__name__}.{hook.name} hook parameters must be keyword-only.")
        expected = [lane.name for lane in hook.lanes] + ["spark", "ctx"]
        names = [parameter.name for parameter in runtime]
        if names != expected:
            self._signature_error(
                hook,
                owner,
                f"{owner.__name__}.{hook.name} must declare keyword-only parameters {', '.join(expected)}; got {', '.join(names) or 'none'}.",
            )

    def _signature_error(self, hook: HookPlan, owner: type, problem: str) -> None:
        lanes = ", ".join(lane.name for lane in hook.lanes)
        self._error(hook, problem, f"Use def {hook.name}(self, *, {lanes}, spark, ctx): ...", lane=lanes)

    def _error(self, hook: HookPlan, problem: str, use: str, **context: str) -> None:
        origin = hook.origin
        owner = getattr(origin, "owner", None)
        source = f"{getattr(owner, '__module__', '')}.{getattr(owner, '__name__', '')}.{hook.name}".strip(".")
        raise StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get("DSL-E0402"),
                problem=problem,
                use=use,
                context={"hook": hook.name, **context},
                source=source,
            )
        )
