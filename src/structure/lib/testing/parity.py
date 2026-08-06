from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FrameSnapshot:
    columns: tuple[str, ...]
    rows: tuple[object, ...]
    schema: str | None


def assert_online_generated_parity(
    online: Callable[[], Any],
    generated: Callable[[], Any],
    *,
    outputs: Sequence[str] | None = None,
    compare_schema: bool = True,
    ordered: bool = False,
) -> Mapping[str, Any]:
    """Assert parity and return the online outputs for follow-up assertions.

    Returning the already-built online outputs lets integration tests inspect
    them without constructing the same transform a second time (which would
    otherwise trigger another Spark action).
    """
    online_outputs = _outputs(online())
    generated_outputs = _outputs(generated())
    names = tuple(outputs or online_outputs)
    if tuple(online_outputs) != tuple(generated_outputs) and outputs is None:
        raise AssertionError(
            "Execution/generated-code outputs differ.\n"
            f"execution outputs: {tuple(online_outputs)!r}\n"
            f"generated-code outputs: {tuple(generated_outputs)!r}"
        )
    for name in names:
        if name not in online_outputs:
            raise AssertionError(f"Online result does not contain output {name!r}.")
        if name not in generated_outputs:
            raise AssertionError(f"Generated result does not contain output {name!r}.")
        _assert_output(
            name,
            _frame(online_outputs[name], compare_schema=compare_schema, ordered=ordered),
            _frame(generated_outputs[name], compare_schema=compare_schema, ordered=ordered),
        )
    return online_outputs


def _outputs(result: Any) -> dict[str, Any]:
    if hasattr(result, "as_dict"):
        return dict(result.as_dict())
    if isinstance(result, Mapping):
        return dict(result)
    return {"result": result}


def _frame(value: Any, *, compare_schema: bool, ordered: bool) -> FrameSnapshot:
    columns = tuple(str(column) for column in getattr(value, "columns", ()))
    rows = tuple(_row(row) for row in value.collect()) if hasattr(value, "collect") else (_row(value),)
    if not ordered:
        rows = tuple(sorted(rows, key=repr))
    return FrameSnapshot(columns=columns, rows=rows, schema=_schema(value) if compare_schema else None)


def _row(row: Any) -> object:
    if hasattr(row, "asDict"):
        return _plain(row.asDict(recursive=True))
    return _plain(row)


def _plain(value: Any) -> object:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return tuple(_plain(item) for item in value)
    return value


def _schema(value: Any) -> str | None:
    schema = getattr(value, "schema", None)
    if schema is None:
        return None
    if hasattr(schema, "simpleString"):
        return str(schema.simpleString())
    return str(schema)


def _assert_output(name: str, online: FrameSnapshot, generated: FrameSnapshot) -> None:
    if online != generated:
        raise AssertionError(
            f"Execution/generated-code parity failed for output {name!r}.\n"
            f"execution: {online!r}\n"
            f"generated-code: {generated!r}"
        )
