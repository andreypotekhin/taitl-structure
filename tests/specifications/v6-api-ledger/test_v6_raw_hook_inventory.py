import ast
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "examples"
INVENTORY = ROOT / "docs/dev/specifications/ExampleRawHookInventory.json"
PARITY = ROOT / "docs/dev/Parity.md"
VALID_STATUSES = {"scheduled", "retired", "intentional", "deferred"}
PARITY_REGISTER = (
    "Normal, conditional, predicate, and sort",
    "String",
    "Numeric and mathematical",
    "Date and timestamp",
    "Bitwise and binary",
    "Hash",
    "JSON and CSV",
    "Arrays and higher-order functions",
    "Struct and map",
    "Aggregates",
    "Windows",
    "Generators and partition transforms",
    "Variant",
    "XML, URL, provider/runtime",
    "Python UDF/UDTF/custom types",
)


def test_example_raw_hooks_have_one_explicit_v6_disposition() -> None:
    entries = _load()["entries"]
    registered = {
        (entry["path"], entry["method"])
        for entry in entries
        if entry["status"] != "retired"
    }

    assert len({entry["id"] for entry in entries}) == len(entries)
    assert registered == _raw_hooks()


def test_v6_raw_hook_dispositions_name_a_real_boundary_and_owner() -> None:
    for entry in _load()["entries"]:
        assert entry["status"] in VALID_STATUSES
        assert entry["id"]
        assert entry["owner"]
        assert entry["boundary"]
        assert entry["rationale"]
        if entry["status"] == "intentional":
            assert entry["capabilities"] == []
        else:
            assert entry["capabilities"]


def test_sql_function_families_have_a_parity_register_entry() -> None:
    parity = PARITY.read_text(encoding="utf-8")

    assert all(capability in parity for capability in PARITY_REGISTER)


def _raw_hooks() -> set[tuple[str, str]]:
    hooks = set()
    for path in EXAMPLES.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(
                _is_raw(decorator) for decorator in node.decorator_list
            ):
                hooks.add((path.relative_to(ROOT).as_posix(), node.name))
    return hooks


def _is_raw(decorator: ast.expr) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return isinstance(target, ast.Name) and target.id == "raw"


def _load() -> dict[str, Any]:
    return json.loads(INVENTORY.read_text(encoding="utf-8"))
