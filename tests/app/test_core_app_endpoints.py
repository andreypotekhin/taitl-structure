import ast
from pathlib import Path

_NESTED_APPS = (
    ("compiler", "artifacts"),
    ("compiler", "compileability", "streaming_compatibility"),
    ("compiler", "diagnostics"),
    ("compiler", "frontend"),
    ("compiler", "symbolic_execution"),
    ("compiler", "traceability"),
    ("runtime", "execution"),
    ("runtime", "schemas"),
    ("runtime", "session"),
    ("target", "capabilities"),
)


def _app(parts: tuple[str, ...]) -> tuple[str, ...]:
    for nested in _NESTED_APPS:
        if parts[: len(nested)] == nested:
            return nested
    return parts[:1]


def test_core_apps_do_not_import_peer_private_commands_or_logic() -> None:
    root = Path("src/structure/core")
    violations: list[str] = []
    for source in root.rglob("*.py"):
        relative = source.relative_to(root)
        owner = _app(relative.parts)
        tree = ast.parse(source.read_text(encoding="utf-8-sig"), filename=str(source))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.module is None:
                continue
            parts = tuple(node.module.split("."))
            prefix = ("structure", "core")
            if parts[:2] != prefix:
                continue
            remainder = parts[2:]
            private_boundary = next((index for index, part in enumerate(remainder) if part in {"commands", "logic"}), None)
            if private_boundary is None:
                continue
            target = _app(remainder[:private_boundary])
            if owner != target:
                violations.append(f"{relative}: {node.module}")
    assert not violations, "Core apps must invoke peers through API endpoints:\n" + "\n".join(violations)
