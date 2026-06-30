from __future__ import annotations

import inspect
import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, cast

import structure

SNAPSHOT = Path("res/testing/snapshots/api/public_structure.v1.json")


def test_public_structure_api_matches_snapshot() -> None:
    assert _snapshot() == json.loads(SNAPSHOT.read_text(encoding="utf-8"))


def _snapshot() -> dict[str, object]:
    exports = {}
    for name in sorted(structure.__all__):
        value = getattr(structure, name)
        exports[name] = {
            "kind": _kind(value),
            "module": getattr(value, "__module__", None),
            "qualname": getattr(value, "__qualname__", None),
            "signature": _signature(value),
            "enum_members": list(getattr(value, "__members__", {})),
            "dataclass_fields": [field.name for field in fields(value)] if is_dataclass(value) else [],
        }
    return {"package": "structure", "exports": exports}


def _kind(value: object) -> str:
    if inspect.isclass(value):
        return "class"
    if inspect.isfunction(value):
        return "function"
    return type(value).__name__


def _signature(value: object) -> str | None:
    try:
        return str(inspect.signature(cast(Any, value)))
    except (TypeError, ValueError):
        return None
