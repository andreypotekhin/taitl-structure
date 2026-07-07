from __future__ import annotations

from collections.abc import Callable


def rows(frame, *order_by: str, recursive: bool = True) -> list[dict[str, object]]:
    ordered = frame.orderBy(*order_by) if order_by else frame
    return [row.asDict(recursive=recursive) for row in ordered.collect()]


def sorted_rows(frame) -> list[dict[str, object]]:
    return sorted(rows(frame), key=repr)


def single(frame, predicate: Callable[[dict[str, object]], bool]) -> dict[str, object]:
    matches = [row for row in rows(frame) if predicate(row)]
    assert len(matches) == 1
    return matches[0]
