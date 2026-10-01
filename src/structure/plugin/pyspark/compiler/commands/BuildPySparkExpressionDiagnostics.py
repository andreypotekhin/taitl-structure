from __future__ import annotations

from collections.abc import Iterable, Mapping

from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.TransformPlan import TransformPlan


class BuildPySparkExpressionDiagnostics:
    """Turn expression-level boundary warnings into compiler diagnostics."""

    def __call__(self, plan: TransformPlan) -> tuple[Diagnostic, ...]:
        diagnostics: list[Diagnostic] = []
        seen: set[tuple[str, str]] = set()
        for step in plan.steps:
            for expression in self._expressions(getattr(step, "plugin_body", None), set()):
                data = getattr(expression, "data", None)
                if not isinstance(data, Mapping):
                    continue
                for code in tuple(data.get("warnings", ())):
                    key = (str(code), str(getattr(step.origin, "owner", plan.name)))
                    if key in seen:
                        continue
                    seen.add(key)
                    entry = diagnostic_registry.get(str(code))
                    diagnostics.append(
                        Diagnostic(
                            entry=entry,
                            source=str(getattr(step.origin, "owner", plan.name)),
                        )
                    )
        return tuple(diagnostics)

    def _expressions(self, value: object, visited: set[int]) -> Iterable[object]:
        if value is None or id(value) in visited:
            return
        visited.add(id(value))
        if hasattr(value, "kind") and hasattr(value, "data"):
            yield value
            for argument in getattr(value, "args", ()):
                yield from self._expressions(argument, visited)
            return
        if isinstance(value, (str, bytes, int, float, bool)):
            return
        if isinstance(value, dict):
            for item in value.values():
                yield from self._expressions(item, visited)
            return
        if isinstance(value, (tuple, list, set, frozenset)):
            for item in value:
                yield from self._expressions(item, visited)
            return
        for name in getattr(value, "__dataclass_fields__", {}):
            yield from self._expressions(getattr(value, name), visited)
