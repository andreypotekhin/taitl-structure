from collections.abc import Iterable, Mapping
from typing import cast

from structure.plugin.api.v1 import ExecutionRequest

from .IterableRelation import IterableRelation


class Execution:
    """Evaluates the deliberately small finite iterable payload vocabulary."""

    def execute(self, request: ExecutionRequest) -> IterableRelation:
        return self._operation(self._payload(request.payload), self._inputs(request.runtime))

    def _payload(self, payload: object) -> Mapping[str, object]:
        if not isinstance(payload, Mapping) or not isinstance(payload.get("operation"), str):
            raise TypeError("Iterable payload must be a mapping with an operation.")
        return payload

    def _inputs(self, runtime: object) -> Mapping[str, IterableRelation]:
        if isinstance(runtime, Mapping):
            return {str(name): IterableRelation.from_rows(self._rows(rows)) for name, rows in runtime.items()}
        return {"input": IterableRelation.from_rows(self._rows(runtime))}

    def _rows(self, value: object) -> Iterable[Mapping[str, object]]:
        if not isinstance(value, Iterable):
            raise TypeError("The iterable fixture runtime must contain finite iterables of row mappings.")
        return cast(Iterable[Mapping[str, object]], value)

    def _operation(self, payload: Mapping[str, object], inputs: Mapping[str, IterableRelation]) -> IterableRelation:
        operation = payload["operation"]
        if operation == "identity":
            return self._input(payload, inputs)
        if operation == "project":
            return self._project(payload, inputs)
        if operation in {"inner_join", "left_join"}:
            return self._join(payload, inputs, left=(operation == "left_join"))
        if operation == "aggregate":
            return self._aggregate(payload, inputs)
        if operation == "recurrence":
            return self._recurrence(payload, inputs)
        raise ValueError(f"Unsupported iterable operation {operation!r}.")

    def _input(self, payload: Mapping[str, object], inputs: Mapping[str, IterableRelation]) -> IterableRelation:
        name = payload.get("input", "input")
        if not isinstance(name, str) or name not in inputs:
            raise ValueError(f"Iterable input {name!r} is unavailable.")
        return inputs[name]

    def _project(self, payload: Mapping[str, object], inputs: Mapping[str, IterableRelation]) -> IterableRelation:
        fields = payload.get("fields")
        if not isinstance(fields, Mapping) or not all(isinstance(name, str) and isinstance(source, str) for name, source in fields.items()):
            raise TypeError("Iterable projection fields must map output names to source field names.")
        rows = ({name: row.get(source) for name, source in fields.items()} for row in self._input(payload, inputs).rows)
        return IterableRelation.from_rows(rows)

    def _join(self, payload: Mapping[str, object], inputs: Mapping[str, IterableRelation], *, left: bool) -> IterableRelation:
        left_name, right_name, left_key, right_key = self._join_fields(payload)
        left_rows = self._named_input(left_name, inputs)
        right_rows = self._named_input(right_name, inputs)
        index: dict[object, list[dict[str, object]]] = {}
        for row in right_rows.rows:
            index.setdefault(row.get(right_key), []).append(row)
        right_fields = tuple({field for row in right_rows.rows for field in row})
        rows: list[dict[str, object]] = []
        for row in left_rows.rows:
            matches = index.get(row.get(left_key), ())
            if matches:
                rows.extend(self._merge(row, match) for match in matches)
            elif left:
                rows.append({**row, **{field: None for field in right_fields if field not in row}})
        return IterableRelation.from_rows(rows)

    def _join_fields(self, payload: Mapping[str, object]) -> tuple[str, str, str, str]:
        values = tuple(payload.get(name) for name in ("left", "right", "left_on", "right_on"))
        if not all(isinstance(value, str) for value in values):
            raise TypeError("Iterable joins require left, right, left_on, and right_on string fields.")
        return cast(tuple[str, str, str, str], values)

    def _named_input(self, name: str, inputs: Mapping[str, IterableRelation]) -> IterableRelation:
        if name not in inputs:
            raise ValueError(f"Iterable input {name!r} is unavailable.")
        return inputs[name]

    def _merge(self, left: Mapping[str, object], right: Mapping[str, object]) -> dict[str, object]:
        overlap = {field for field in set(left) & set(right) if left[field] != right[field]}
        if overlap:
            fields = ", ".join(sorted(overlap))
            raise ValueError(f"Iterable join has colliding fields: {fields}. Project one input before joining.")
        return {**right, **left}

    def _aggregate(self, payload: Mapping[str, object], inputs: Mapping[str, IterableRelation]) -> IterableRelation:
        group_by = payload.get("group_by", ())
        aggregates = payload.get("aggregates")
        if not isinstance(group_by, tuple | list) or not all(isinstance(field, str) for field in group_by):
            raise TypeError("Iterable aggregation group_by must be a sequence of field names.")
        if not isinstance(aggregates, Mapping):
            raise TypeError("Iterable aggregation aggregates must be a mapping.")
        groups: dict[tuple[object, ...], list[dict[str, object]]] = {}
        for row in self._input(payload, inputs).rows:
            groups.setdefault(tuple(row.get(field) for field in group_by), []).append(row)
        rows: list[dict[str, object]] = []
        for key, group in groups.items():
            row = dict(zip(group_by, key, strict=True))
            row.update({name: self._aggregate_value(specification, group) for name, specification in aggregates.items()})
            rows.append(row)
        return IterableRelation.from_rows(rows)

    def _aggregate_value(self, specification: object, rows: list[dict[str, object]]) -> object:
        if not isinstance(specification, Mapping) or len(specification) != 1:
            raise TypeError("Iterable aggregate definitions must contain exactly one operation.")
        operation, field = next(iter(specification.items()))
        if operation == "count":
            return len(rows) if field is None else sum(row.get(cast(str, field)) is not None for row in rows)
        if operation == "sum" and isinstance(field, str):
            values = [row.get(field) for row in rows if row.get(field) is not None]
            if not all(isinstance(value, int | float) for value in values):
                raise TypeError(f"Iterable sum field {field!r} must contain numbers.")
            return sum(cast(list[int | float], values))
        raise TypeError("Iterable aggregates support count and sum only.")

    def _recurrence(self, payload: Mapping[str, object], inputs: Mapping[str, IterableRelation]) -> IterableRelation:
        index = payload.get("index")
        value = payload.get("value")
        initial = payload.get("initial")
        output = payload.get("output")
        next_state = payload.get("next")
        if not isinstance(index, str) or not isinstance(value, str):
            raise TypeError("Iterable recurrences require string index and value field names.")
        if not isinstance(initial, list) or not initial or not all(isinstance(item, int | float) for item in initial):
            raise TypeError("Iterable recurrences require a non-empty numeric initial state.")
        if not isinstance(next_state, list) or len(next_state) != len(initial):
            raise TypeError("Iterable recurrences require one next-state expression per initial value.")
        state = tuple(cast(int | float, item) for item in initial)
        rows: list[dict[str, object]] = []
        for expected, row in enumerate(self._input(payload, inputs).rows):
            if row.get(index) != expected:
                raise ValueError(f"Iterable recurrences require contiguous {index!r} values starting at 0.")
            if value in row:
                raise ValueError(f"Iterable recurrence output field {value!r} already exists.")
            rows.append({**row, value: self._evaluate(output, state)})
            state = tuple(self._evaluate(expression, state) for expression in next_state)
        return IterableRelation.from_rows(rows)

    def _evaluate(self, expression: object, state: tuple[int | float, ...]) -> int | float:
        if not isinstance(expression, Mapping) or len(expression) != 1:
            raise TypeError("Iterable recurrence expressions must contain exactly one operation.")
        operation, arguments = next(iter(expression.items()))
        if operation == "state" and isinstance(arguments, int) and 0 <= arguments < len(state):
            return state[arguments]
        if operation == "literal" and isinstance(arguments, int | float):
            return arguments
        if operation in {"add", "subtract", "multiply", "divide"} and isinstance(arguments, list) and len(arguments) == 2:
            left, right = (self._evaluate(argument, state) for argument in arguments)
            return {
                "add": left + right,
                "subtract": left - right,
                "multiply": left * right,
                "divide": left / right,
            }[operation]
        raise TypeError("Invalid Iterable recurrence expression.")
