from __future__ import annotations

import ast
import re
from pathlib import Path


def test_pyspark_compatibility_matrix_matches_docs_and_compose_defaults() -> None:
    docs = Path("docs/Compatibility.md").read_text(encoding="utf-8")
    env = Path("infra/compose/.env_example").read_text(encoding="utf-8")
    script = Path("scripts/run_integration.py").read_text(encoding="utf-8")

    assert "PySpark 3.5.x and 4.0.x" in docs
    assert 'target_pyspark = ">=3.5,<4.1"' in docs
    assert _env_value(env, "PYSPARK35_VERSION") == "3.5.0"
    assert _env_value(env, "PYSPARK40_VERSION") == "4.0.0"
    assert _backends(script) == ("pyspark35", "pyspark40")


def _env_value(text: str, key: str) -> str:
    match = re.search(rf"(?m)^{key}=(.+)$", text)
    assert match is not None
    return match.group(1)


def _backends(script: str) -> tuple[str, ...]:
    tree = ast.parse(script)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "BACKENDS":
                    value = ast.literal_eval(node.value)
                    return tuple(value)
    raise AssertionError("scripts/run_integration.py does not define BACKENDS")
