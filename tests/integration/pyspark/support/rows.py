from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any
from weakref import ReferenceType, ref

_RowCacheKey = tuple[int, tuple[str, ...], bool]
_CachedRows = tuple[ReferenceType[Any], list[dict[str, object]]]
_rows_cache: dict[_RowCacheKey, _CachedRows] = {}


def rows(frame, *order_by: str, recursive: bool = True) -> list[dict[str, object]]:
    key = (id(frame), order_by, recursive)
    cached = _rows_cache.get(key)
    if cached is not None and cached[0]() is frame:
        return deepcopy(cached[1])

    ordered = frame.orderBy(*order_by) if order_by else frame
    result = [row.asDict(recursive=recursive) for row in ordered.collect()]
    _rows_cache[key] = (ref(frame), deepcopy(result))
    return result


def sorted_rows(frame) -> list[dict[str, object]]:
    return sorted(rows(frame), key=repr)


def single(frame, predicate: Callable[[dict[str, object]], bool]) -> dict[str, object]:
    matches = [row for row in rows(frame) if predicate(row)]
    assert len(matches) == 1
    return matches[0]


def clear_rows() -> None:
    _rows_cache.clear()
