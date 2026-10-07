from __future__ import annotations

import ast
import os
import re
import subprocess
from pathlib import Path


def test_pyspark_compatibility_matrix_matches_docs_and_compose_defaults() -> None:
    docs = Path("docs/Compatibility.md").read_text(encoding="utf-8")
    env = Path("infra/compose/.env_example").read_text(encoding="utf-8")
    script = Path("scripts/run_integration.py").read_text(encoding="utf-8")
    compose = Path("infra/compose/docker-compose.yaml").read_text(encoding="utf-8")

    assert "PySpark 3.5.x and 4.0.x" in docs
    assert 'profile = ">=3.5,<4.1"' in docs
    assert _env_value(env, "PYSPARK35_VERSION") == "3.5.3"
    assert _env_value(env, "PYSPARK40_VERSION") == "4.0.0"
    assert _env_value(env, "PYSPARK41_VERSION") == "4.1.0"
    assert _backends(script) == ("pyspark35", "pyspark40", "pyspark41", "spark-connect35", "spark-connect40", "spark-connect41")
    assert '"pyspark41": ("spark41-master", "spark41-worker")' in script
    assert '"spark-connect41": ()' in script
    assert "structure-integration-pyspark41" in compose
    assert "structure-integration-spark-connect35" in compose
    assert "structure-integration-spark-connect40" in compose
    assert "structure-integration-spark-connect41" in compose
    assert "spark35-connect" not in compose
    assert "spark40-connect" not in compose


def test_pyspark_4_1_runner_selects_v11_only(tmp_path) -> None:
    captured = tmp_path / "pytest-arguments"
    timeout = tmp_path / "timeout"
    timeout.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$STRUCTURE_CAPTURED_ARGS"\n', encoding="utf-8")
    timeout.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "STRUCTURE_CAPTURED_ARGS": str(captured),
    }
    runner = Path("infra/compose/images/pyspark/run-integration.sh").resolve()

    subprocess.run(["bash", str(runner), "pyspark41"], check=True, env=environment)
    arguments = captured.read_text(encoding="utf-8").splitlines()

    assert "/workspace/tests/integration/pyspark/backend/test_runtime_versions.py" in arguments
    assert "/workspace/tests/integration/pyspark/v11" in arguments
    assert "/workspace/tests/integration" not in arguments
    assert "/workspace/tests/concepts/live_pyspark" not in arguments


def test_spark_connect_4_1_runner_runs_isolated_providers_then_full_suite(tmp_path) -> None:
    calls = tmp_path / "timeout-calls"
    checkpoint = tmp_path / "connect-checkpoint"
    checkpoint.mkdir()
    wrappers = {
        "timeout": '#!/bin/sh\nprintf "%s\\n" "$*" >> "$STRUCTURE_CAPTURED_CALLS"\nsleep 0.1\n',
        "spark-submit": '#!/bin/sh\nprintf "%s\\n" "$*" > "$STRUCTURE_CAPTURED_SUBMIT"\n',
        "mkdir": "#!/bin/sh\nexit 0\n",
        "mktemp": '#!/bin/sh\nprintf "%s\\n" "$STRUCTURE_FAKE_CHECKPOINT_DIR"\n',
    }
    for name, content in wrappers.items():
        wrapper = tmp_path / name
        wrapper.write_text(content, encoding="utf-8")
        wrapper.chmod(0o755)
    environment = {
        **os.environ,
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "SPARK_HOME": str(tmp_path / "spark"),
        "STRUCTURE_CAPTURED_CALLS": str(calls),
        "STRUCTURE_CAPTURED_SUBMIT": str(tmp_path / "spark-submit-args"),
        "STRUCTURE_FAKE_CHECKPOINT_DIR": str(checkpoint),
        "STRUCTURE_EXPECTED_SPARK": "4.1.0",
        "STRUCTURE_EXPECTED_DELTA": "4.1.0",
    }
    runner = Path("infra/compose/images/pyspark/run-integration.sh").resolve()

    subprocess.run(["bash", str(runner), "spark-connect41"], check=True, env=environment)

    phases = calls.read_text(encoding="utf-8").splitlines()
    assert len(phases) == 3
    assert "/workspace/tests/integration/pyspark/iceberg/test_native_iceberg.py" in phases[0]
    assert "/workspace/tests/integration/pyspark/v11/test_delta_transform_live.py" in phases[1]
    assert "/workspace/tests/integration" in phases[2]
    assert "/workspace/tests/concepts/live_pyspark" in phases[2]
    assert "--ignore=/workspace/tests/integration/pyspark/iceberg" in phases[2]
    assert "--ignore=/workspace/tests/integration/pyspark/v11/test_delta_transform_live.py" in phases[2]
    assert "--integration-backend=spark-connect41" in phases[2]
    assert not checkpoint.exists()
    assert "spark.checkpoint.dir=" in (tmp_path / "spark-submit-args").read_text(encoding="utf-8")


def test_public_docs_use_target_variant_and_do_not_claim_v4_only_spark_connect() -> None:
    paths = [
        Path("Readme.md"),
        Path("docs/Overview.md"),
        Path("docs/QuickRef.md"),
        Path("docs/Configuration.md"),
        Path("docs/Compatibility.md"),
        Path("docs/dev/specifications/ConfigSchema.spec.md"),
        Path("docs/dev/specifications/CompatibilityPolicy.spec.md"),
        Path("docs/dev/specifications/BackendCapabilities.spec.md"),
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in paths)

    assert 'profile = ">=3.5,<4.1"' in text
    assert 'variant = "ordinary"' in text
    assert 'variant = "spark-connect"' in text
    assert "spark-connect35" in text
    assert "spark-connect40" in text
    assert "scheduled for v4" not in text
    assert "planned for v4" not in text
    assert "planned as an experimental end-of-v2" not in text
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
