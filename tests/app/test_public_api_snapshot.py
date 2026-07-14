from __future__ import annotations

import inspect
import json
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Any, cast

import pytest

import structure
from structure import *

SNAPSHOT = Path("res/testing/snapshots/api/public_structure.v1.json")


def test_public_structure_api_matches_snapshot() -> None:
    assert _snapshot() == json.loads(SNAPSHOT.read_text(encoding="utf-8"))


def test_public_structure_star_import_exports_only_public_api() -> None:
    namespace: dict[str, object] = {}

    exec("from structure import *", namespace)

    exports = {name: value for name, value in namespace.items() if not name.startswith("__")}
    assert set(exports) == set(structure.__all__)
    assert all(exports[name] is getattr(structure, name) for name in structure.__all__)


def test_public_structure_star_import_compiles_end_user_source() -> None:
    namespace: dict[str, object] = {}

    exec(
        """
from structure import *


class Raw(Schema):
    id = field.string(nullable=False)


class Published(Schema):
    id = field.string(nullable=False)


@transform
class Publish(Transform):
    rows = input(Raw)
    published = output(Published)

    def publish(self, row: Raw) -> Published:
        where(row.id.is_not_null())
        return project(row, Published)
""",
        namespace,
    )

    plan = compile_transform(cast(Any, namespace["Publish"]))

    assert plan.name == "Publish"
    assert [input.name for input in plan.inputs] == ["rows"]
    assert [output.name for output in plan.outputs] == ["published"]
    assert [step.name for step in plan.steps] == ["publish"]


def test_method_level_transform_reports_the_step_migration() -> None:
    with pytest.raises(TypeError, match=r"replace method-level @transform\(\.\.\.\) with @step"):
        transform(lambda: None, output=object())


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
