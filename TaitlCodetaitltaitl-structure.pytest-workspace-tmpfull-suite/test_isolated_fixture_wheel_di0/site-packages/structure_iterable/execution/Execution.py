from collections.abc import Iterable, Mapping
from typing import cast

from structure.plugin.api.v1 import ExecutionAPI as ExecutionAPIV1
from structure.plugin.api.v1 import ExecutionRequest, TransformResult

from ..authoring.Authoring import BinaryStateExpression, Field, LiteralStateExpression, StateExpression, StateValue
from ..compiler.Compiler import IterableRecipe
from .IterableRelation import IterableRelation


class Execution(ExecutionAPIV1):
    """Evaluates the finite recipe shared by online and generated Iterable execution."""

    def execute(self, request: ExecutionRequest) -> object:
        recipe = request.payload
        if not isinstance(recipe, IterableRecipe):
            raise TypeError("Iterable execution requires an IterableRecipe payload.")
        sources = self._inputs(recipe, request.runtime)
        for step in recipe.steps:
            produced: dict[str, list[dict[str, object]]] = {name: [] for name in step.results}
            driver = step.inputs[0]
            state = step.body.scan.initial if step.body.scan is not None else ()
            for ordinal, row in enumerate(sources[driver].rows):
                if step.body.scan is not None and row.get("index") != ordinal:
                    raise ValueError("Iterable scans require contiguous 'index' values starting at 0.")
                contexts = [{driver: row}]
                for join in step.body.joins:
                    contexts = self._join(contexts, join, sources)
                for context in contexts:
                    for lane, projection in zip(step.results, step.body.projections, strict=True):
                        produced[lane].append({
                            self._column(projection.schema, name): self._value(value, context, state=state)
                            for name, value in projection.values.items()
                        })
                if step.body.scan is not None:
                    state = tuple(self._state_value(value, state) for value in step.body.scan.next)
            sources.update({name: IterableRelation.from_rows(rows) for name, rows in produced.items()})
        outputs = {name: sources[name] for name in recipe.outputs}
        if len(outputs) == 1:
            return next(iter(outputs.values()))
        return TransformResult(outputs, single=False)

    def _inputs(self, recipe: IterableRecipe, runtime: object) -> dict[str, IterableRelation]:
        values = runtime if isinstance(runtime, Mapping) else {recipe.inputs[0]: runtime}
        missing = set(recipe.inputs) - set(values)
        if missing:
            raise ValueError(f"Iterable runtime is missing input(s): {', '.join(sorted(missing))}.")
        return {name: IterableRelation.from_rows(self._rows(values[name])) for name in recipe.inputs}

    @staticmethod
    def _rows(value: object) -> Iterable[Mapping[str, object]]:
        if not isinstance(value, Iterable):
            raise TypeError("The Iterable runtime must contain finite iterables of row mappings.")
        return cast(Iterable[Mapping[str, object]], value)

    def _join(self, contexts, join, sources):
        index: dict[object, list[dict[str, object]]] = {}
        for row in sources[join.relation].rows:
            index.setdefault(row.get(join.right.name), []).append(row)
        joined: list[dict[str, dict[str, object]]] = []
        fields = {field for row in sources[join.relation].rows for field in row}
        for context in contexts:
            matches = index.get(cast(dict[str, object], context[join.left.row]).get(join.left.name), ())
            if matches:
                joined.extend({**context, join.relation: match} for match in matches)
            elif join.kind == "left":
                joined.append({**context, join.relation: dict.fromkeys(fields)})
        return joined

    @staticmethod
    def _column(schema: object, name: str) -> str:
        return getattr(schema, "_structure_fields")[name].column

    @staticmethod
    def _value(value: object, context: Mapping[str, Mapping[str, object]], *, state: tuple[int | float, ...]) -> object:
        if isinstance(value, Field):
            return context[value.row].get(value.name)
        if isinstance(value, StateExpression):
            return Execution._state_value(value, state)
        return value

    @staticmethod
    def _state_value(value: StateExpression, state: tuple[int | float, ...]) -> int | float:
        if isinstance(value, StateValue):
            return state[value.ordinal]
        if isinstance(value, LiteralStateExpression):
            return value.value
        if isinstance(value, BinaryStateExpression) and value.operation == "add":
            return Execution._state_value(value.left, state) + Execution._state_value(value.right, state)
        raise TypeError("Invalid Iterable scan state expression.")
