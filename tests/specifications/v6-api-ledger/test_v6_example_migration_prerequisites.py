import ast
import importlib
import json
from pathlib import Path
from typing import Any

from structure.core.compiler.api import Compiler

ROOT = Path(__file__).resolve().parents[3]
INVENTORY = ROOT / "docs/dev/specifications/V6ExampleRawHookInventory.json"
COMPOSED_TRANSFORMS = {
    "search.rerank-documents.score_candidates": (
        "examples.search.transforms.searching.search_docs.SearchDocuments",
        "SearchDocuments",
    ),
}
RETIRED_TRANSFORMS = {
    "security.vulnerability-posture.retain-reconciled-inventory": (
        "examples.security.transforms.posture",
        "SecurityPosture",
    ),
    "security.vulnerability-quality.reconcile-device-inventory": (
        "examples.security.transforms.quality",
        "SecurityInventoryQuality",
    ),
}


def test_each_v6_example_hook_compiles_to_its_declared_opaque_boundary() -> None:
    for entry in _entries():
        transform, source_transform = _transform(entry)
        traceability = Compiler.traceability.build()(
            Compiler.frontend.compile()(transform, materialize_schemas=False).lowered,
            source_transform=source_transform,
            transform_module="v6_migration_fixture",
        )
        hooks = {boundary.hook for boundary in traceability.opaque_boundaries}
        if entry["status"] == "retired":
            assert entry["method"] not in hooks
        else:
            assert entry["method"] in hooks


def test_security_reconciliation_is_typed_and_has_no_opaque_hook_boundary() -> None:
    posture, posture_traceability = _lowered("examples.security.transforms.posture", "SecurityPosture")
    quality, quality_traceability = _lowered("examples.security.transforms.quality", "SecurityInventoryQuality")

    assert posture_traceability.opaque_boundaries == ()
    assert quality_traceability.opaque_boundaries == ()
    assert {"array_contains", "array_exists"} <= _functions(_step(posture, "expose").filters[0])
    assignments = {assignment.field.name: assignment.expression for assignment in _step(quality, "prepare_inventory_reconciliation").projection}
    assert {"array_exists"} <= _functions(assignments["device_has_software"])
    assert {"array_contains", "array_exists"} <= _functions(assignments["is_reconciled"])


def _entries() -> tuple[dict[str, Any], ...]:
    return tuple(json.loads(INVENTORY.read_text(encoding="utf-8"))["entries"])


def _transform(entry: dict[str, Any]) -> tuple[type, str]:
    declared = COMPOSED_TRANSFORMS.get(entry["id"]) or RETIRED_TRANSFORMS.get(entry["id"])
    if declared is not None:
        module_name, class_name = declared
        return getattr(importlib.import_module(module_name), class_name), f"{module_name}.{class_name}"
    path = Path(entry["path"])
    module_name = ".".join(path.with_suffix("").parts)
    class_name = _owner(path, entry["method"])
    return getattr(importlib.import_module(module_name), class_name), f"{module_name}.{class_name}"


def _owner(path: Path, method: str) -> str:
    for node in ast.parse((ROOT / path).read_text(encoding="utf-8")).body:
        if isinstance(node, ast.ClassDef) and any(
            isinstance(member, ast.FunctionDef) and member.name == method for member in node.body
        ):
            return node.name
    raise AssertionError(f"No owner for {path}:{method}")


def _lowered(module_name: str, class_name: str):
    transform = getattr(importlib.import_module(module_name), class_name)
    lowered = Compiler.frontend.compile()(transform, materialize_schemas=False).lowered
    traceability = Compiler.traceability.build()(
        lowered,
        source_transform=f"{module_name}.{class_name}",
        transform_module="v6_migration_fixture",
    )
    return lowered, traceability


def _step(lowered, name: str):
    return next(step for step in lowered.steps if step.name == name)


def _functions(expression) -> set[str]:
    return {
        str(item.data["function"])
        for item in _walk(expression)
        if item.kind == "transform_expression" and item.data is not None and "function" in item.data
    }


def _walk(expression):
    yield expression
    for argument in expression.args:
        yield from _walk(argument)
