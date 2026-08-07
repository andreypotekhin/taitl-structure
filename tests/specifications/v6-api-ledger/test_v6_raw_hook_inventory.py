import ast
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
EXAMPLES = ROOT / "examples"
INVENTORY = ROOT / "docs/dev/specifications/ExampleRawHookInventory.json"
GAPS = ROOT / "docs/dev/Gaps.md"
VALID_STATUSES = {"scheduled", "retired", "intentional", "deferred"}
GAPS_REGISTER = (
    "Lambda-bound struct field access",
    "Partitioned `window_max`",
    "Ordered `collect_list`",
    "`exactly_one` validation",
    "Implicit global aggregation",
    "Explicit scalar UDF example",
    "`posexplode` over array of structs",
    "Other generator forms",
    "Exact-schema relation set composition and self-alias",
    "Relation order/limit/offset",
    "Branchable typed union",
    "`require_unique` / `require_all` / `require_reference`",
    "Bounded parent hierarchy and fallbacks",
    "First-qualified priority selection",
    "Sampling",
    "Bounded ordered `scan(...)`",
    "Binary/encoding; JSON/CSV parsing; Deterministic `mode`",
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


def test_v6_postponed_and_scheduled_capabilities_have_a_gaps_register_entry() -> None:
    gaps = GAPS.read_text(encoding="utf-8")

    assert all(capability in gaps for capability in GAPS_REGISTER)


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
