from collections.abc import Iterable
from types import SimpleNamespace

from structure.plugin.api.v1.model import OpaqueBoundary


class FindPythonUdfBoundaries:
    """Find each Python UDF boundary represented by compiled expressions."""

    def __call__(self, *, step: str, schema: str, expressions: Iterable[object]) -> tuple[OpaqueBoundary, ...]:
        boundaries: list[OpaqueBoundary] = []
        seen: set[str] = set()
        for expression in expressions:
            for udf in self._expressions(expression):
                data = getattr(udf, "data", None) or {}
                name = str(data.get("function_name", "python_udf"))
                key = f"{step}:{name}"
                if key in seen:
                    continue
                seen.add(key)
                boundaries.append(
                    OpaqueBoundary(
                        step=step,
                        hook=name,
                        phase="expression",
                        target="arrow_udf" if getattr(udf, "kind", None) in {"arrow_udf", "arrow_udf_window"} else "python_udf",
                        schema=schema,
                        reason="python UDF body",
                    )
                )
        return tuple(boundaries)

    def _expressions(self, expression: object) -> tuple[object, ...]:
        arguments = tuple(getattr(expression, "args", ()))
        kind = getattr(expression, "kind", None)
        data = getattr(expression, "data", None) or {}
        if kind == "aggregate" and data.get("function") == "arrow_udf_grouped_agg":
            found: list[object] = [
                SimpleNamespace(
                    kind="arrow_udf",
                    data={"function_name": data.get("arrow_function_name", "arrow_udf")},
                )
            ]
        else:
            found = [expression] if kind in {"python_udf", "arrow_udf", "arrow_udf_window"} else []
        for argument in arguments:
            found.extend(self._expressions(argument))
        return tuple(found)
