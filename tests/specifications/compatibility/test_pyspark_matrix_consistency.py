from __future__ import annotations

import ast
import re
from pathlib import Path


def test_pyspark_compatibility_matrix_matches_docs_and_compose_defaults() -> None:
    docs = Path("docs/Compatibility.md").read_text(encoding="utf-8")
    env = Path("infra/compose/.env_example").read_text(encoding="utf-8")
    script = Path("scripts/run_integration.py").read_text(encoding="utf-8")

    assert "PySpark 3.5.x and 4.0.x" in docs
    assert 'target_profile = ">=3.5,<4.1"' in docs
    assert _env_value(env, "PYSPARK35_VERSION") == "3.5.0"
    assert _env_value(env, "PYSPARK40_VERSION") == "4.0.0"
    assert _backends(script) == ("pyspark35", "pyspark40")


def test_public_docs_use_target_variant_and_do_not_claim_v4_only_spark_connect() -> None:
    paths = [
        Path("Readme.md"),
        Path("docs/Overview.md"),
        Path("docs/QuickRef.md"),
        Path("docs/Configuration.md"),
        Path("docs/Compatibility.md"),
        Path("docs/dev/specifications/ConfigSchema.md"),
        Path("docs/dev/specifications/CompatibilityPolicy.md"),
        Path("docs/dev/specifications/BackendCapabilities.md"),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in paths)

    assert 'target_profile = ">=3.5,<4.1"' in text
    assert 'target_variant = "ordinary"' in text
    assert 'target_variant = "spark-connect"' in text
    assert "scheduled for v4" not in text
    assert "planned for v4" not in text
    assert "not part of the initial release, v2, or v3" not in text


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
