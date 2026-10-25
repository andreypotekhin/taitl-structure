from __future__ import annotations

import argparse
import ast
import importlib
import os
import re
import subprocess
from pathlib import Path
from shlex import split

import pytest


def test_pyspark_compatibility_matrix_matches_docs_and_compose_defaults() -> None:
    docs = Path("docs/Compatibility.md").read_text(encoding="utf-8")
    env = Path("infra/compose/.env_example").read_text(encoding="utf-8")
    script = Path("scripts/run_integration.py").read_text(encoding="utf-8")
    compose = Path("infra/compose/docker-compose.yaml").read_text(encoding="utf-8")

    assert "PySpark 3.5.x, 4.0.x, and 4.1.x" in docs
    assert 'profile = ">=3.5,<4.2"' in docs
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


@pytest.mark.parametrize("backend", ("pyspark35", "pyspark40", "pyspark41", "spark-connect35", "spark-connect40", "spark-connect41"))
def test_runner_phases_and_cleanup(tmp_path, backend) -> None:
    result, phases, checkpoint, submit, pid = _run_launcher(tmp_path, backend)
    assert result.returncode == 0
    providers = backend in {"pyspark35", "pyspark40", "pyspark41", "spark-connect41"}
    assert len(phases) == (3 if backend == "pyspark41" else 5 if providers else 3)
    if providers:
        assert "/workspace/tests/integration/pyspark/iceberg/test_native_iceberg.py" in phases[0]
        assert "/workspace/tests/integration/pyspark/iceberg/test_iceberg_sql.py" in phases[0]
        assert "/workspace/tests/integration/pyspark/iceberg/test_iceberg_transform.py" in phases[0]
        assert "/workspace/tests/integration/pyspark/v11/test_delta_transform_live.py" in phases[1]
        assert "--ignore=/workspace/tests/integration/pyspark/iceberg" in phases[2]
        assert "--ignore=/workspace/tests/integration/pyspark/v11/test_delta_transform_live.py" in phases[2]
    if backend in {"spark-connect35", "spark-connect40"}:
        assert "--ignore=/workspace/tests/integration/pyspark/iceberg" in phases[0]
        assert "--ignore=/workspace/tests/integration/pyspark/v11/test_delta_transform_live.py" in phases[0]
    if backend == "pyspark41":
        assert "/workspace/tests/integration/pyspark/v11" in phases[-1]
        assert "/workspace/tests/concepts/live_pyspark" not in phases[-1]
    else:
        general = phases[2:] if providers else phases
        assert "/workspace/tests/integration/pyspark/book_contracts " in general[0]
        for directory in ("search", "security", "stocks", "store"):
            assert f"/workspace/tests/integration/pyspark/{directory} " in general[1]
            assert f"--ignore=/workspace/tests/integration/pyspark/{directory}" in general[2]
        assert "--ignore=/workspace/tests/integration/pyspark/book_contracts" in general[2]
        assert "/workspace/tests/integration " in phases[-1]
        assert "/workspace/tests/concepts/live_pyspark" in phases[-1]
    for phase in phases:
        assert f"--integration-backend={backend}" in phase
        assert "-ra" in phase
        assert "--durations=20" in phase
        assert "-v" in phase
    assert "Integration launcher: backend=" in result.stdout
    assert "checksum=" in result.stdout
    assert "=== Integration phase: general" in result.stdout
    assert "Selection:" in result.stdout
    assert "Pytest overrides: <none>" in result.stdout
    assert "elapsed=" in result.stdout
    _assert_cleanup(backend, checkpoint, submit, pid)


@pytest.mark.parametrize("phase", (1, 2, 3, 4, 5))
@pytest.mark.parametrize("status", (1, 124, 137))
def test_runner_stops_and_cleans_up_after_failure(tmp_path, phase, status) -> None:
    result, phases, checkpoint, submit, pid = _run_launcher(tmp_path, "spark-connect41", phase, status)
    assert result.returncode == status
    assert len(phases) == phase
    assert f"exit={status}" in result.stdout
    assert "Spark Connect server output" in result.stderr
    if status in {124, 137}:
        assert "Integration deadline reached" in result.stderr
    _assert_cleanup("spark-connect41", checkpoint, submit, pid)


def test_runner_cleans_checkpoint_after_setup_failure(tmp_path) -> None:
    result, phases, checkpoint, submit, pid = _run_launcher(tmp_path, "spark-connect41", setup_failure=True)
    assert result.returncode != 0
    assert "SPARK_HOME" in result.stderr
    assert phases == []
    assert not checkpoint.exists()
    assert not submit.exists()
    assert not pid.exists()


def test_runner_keeps_filtered_provider_phases_when_no_tests_match(tmp_path) -> None:
    result, phases, checkpoint, submit, pid = _run_launcher(
        tmp_path, "spark-connect41", failing_phase=1, status=5, pytest_args="-k kll_theta"
    )
    assert result.returncode == 0
    assert len(phases) == 5
    assert "No tests matched the explicit filter in phase iceberg" in result.stdout
    assert "No tests matched the explicit filter in phase delta" in result.stdout
    _assert_cleanup("spark-connect41", checkpoint, submit, pid)


@pytest.mark.parametrize("selection,count", (("all", 5), ("iceberg", 1), ("delta", 1), ("general", 3), ("general-contracts", 1), ("general-examples", 1), ("general-runtime", 1)))
def test_runner_can_select_independent_phases(tmp_path, selection, count) -> None:
    result, phases, checkpoint, submit, pid = _run_launcher(
        tmp_path, "spark-connect41", extra_environment={"STRUCTURE_INTEGRATION_PHASE": selection}
    )
    assert result.returncode == 0
    assert len(phases) == count
    if selection != "all":
        assert f"=== Integration phase: {selection}" in result.stdout
    _assert_cleanup("spark-connect41", checkpoint, submit, pid)


@pytest.mark.parametrize("backend,selection", (("pyspark35", "typo"), ("pyspark41", "general-contracts"), ("spark-connect35", "delta"), ("spark-connect40", "iceberg")))
def test_runner_rejects_unavailable_phase_before_startup(tmp_path, backend, selection) -> None:
    result, phases, checkpoint, submit, pid = _run_launcher(
        tmp_path, backend, extra_environment={"STRUCTURE_INTEGRATION_PHASE": selection}
    )
    assert result.returncode == 2
    assert phases == []
    assert not submit.exists()
    assert not pid.exists()


def test_general_passes_partition_current_test_modules(tmp_path) -> None:
    result, phases, *_ = _run_launcher(tmp_path, "pyspark35")
    assert result.returncode == 0
    partitions = []
    for phase in phases[2:]:
        arguments = split(phase)
        paths = [Path(value.removeprefix("/workspace/")) for value in arguments if value.startswith("/workspace/")]
        ignored = [Path(value.removeprefix("--ignore=/workspace/")) for value in arguments if value.startswith("--ignore=")]
        partitions.append({
            source for path in paths for source in path.rglob("test_*.py")
            if not any(source == ignore or source.is_relative_to(ignore) for ignore in ignored)
        })
    expected = {
        source for path in (Path("tests/integration"), Path("tests/concepts/live_pyspark"))
        for source in path.rglob("test_*.py")
        if not source.is_relative_to("tests/integration/pyspark/iceberg")
        and source.name != "test_delta_transform_live.py"
    }
    assert all(partitions)
    assert set.union(*partitions) == expected
    assert sum(map(len, partitions)) == len(expected)


@pytest.mark.parametrize("backend", ("pyspark41", "spark-connect41"))
def test_runner_isolates_sedona_coexistence_phase(tmp_path, backend) -> None:
    environment = {
        "STRUCTURE_SEDONA_ONLY": "1",
        "STRUCTURE_SEDONA_TESTS": "1",
        "STRUCTURE_SPARK_JARS_PACKAGES": (
            "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.0,"
            "org.datasyslab:geotools-wrapper:1.9.0-33.5"
        ),
    }
    result, phases, checkpoint, submit, pid = _run_launcher(
        tmp_path, backend, extra_environment=environment
    )
    assert result.returncode == 0
    assert len(phases) == 1
    assert "/workspace/tests/integration/pyspark/v11/test_geospatial.py" in phases[0]
    assert "/workspace/tests/integration/pyspark/v9/test_geometry.py" in phases[0]
    assert "=== Integration phase: sedona" in result.stdout
    if backend == "spark-connect41":
        command = submit.read_text(encoding="utf-8")
        assert "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.0" in command
        assert (
            "spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ) in command
    _assert_cleanup(backend, checkpoint, submit, pid)


def _run_launcher(
    tmp_path, backend, failing_phase=0, status=0, setup_failure=False, pytest_args="", extra_environment=None
):
    calls = tmp_path / "timeout-calls"
    checkpoint = tmp_path / "connect-checkpoint"
    checkpoint.mkdir()
    submit = tmp_path / "spark-submit-args"
    pid = tmp_path / "gateway-pid"
    wrappers = {
        "timeout": (
            '#!/bin/sh\n'
            'if [ "$STRUCTURE_HAS_GATEWAY" = "1" ]; then\n'
            '  while [ ! -f "$STRUCTURE_CAPTURED_PID" ]; do sleep 0.01; done\nfi\n'
            'printf "%s\\n" "$*" >> "$STRUCTURE_CAPTURED_CALLS"\n'
            'if [ "$(wc -l < "$STRUCTURE_CAPTURED_CALLS")" -eq "$STRUCTURE_FAIL_PHASE" ] || [ "$STRUCTURE_FAIL_STATUS" -eq 5 ]; then\n'
            '  exit "$STRUCTURE_FAIL_STATUS"\nfi\n'
        ),
        "spark-submit": (
            '#!/bin/sh\nprintf "%s\\n" "$*" > "$STRUCTURE_CAPTURED_SUBMIT"\n'
            'printf "%s\\n" "$$" > "$STRUCTURE_CAPTURED_PID"\nexec sleep 60\n'
        ),
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
        "STRUCTURE_CAPTURED_SUBMIT": str(submit),
        "STRUCTURE_CAPTURED_PID": str(pid),
        "STRUCTURE_HAS_GATEWAY": "1" if backend.startswith("spark-connect") else "0",
        "STRUCTURE_FAKE_CHECKPOINT_DIR": str(checkpoint),
        "STRUCTURE_EXPECTED_SPARK": "4.1.0",
        "STRUCTURE_EXPECTED_DELTA": "4.1.0",
        "STRUCTURE_FAIL_PHASE": str(failing_phase),
        "STRUCTURE_FAIL_STATUS": str(status),
        "INTEGRATION_PYTEST_ARGS": pytest_args,
        "STRUCTURE_ICEBERG_ONLY": "0",
        "STRUCTURE_INTEGRATION_PHASE": "all",
        **(extra_environment or {}),
    }
    runner = Path("infra/compose/images/pyspark/run-integration.sh").resolve()
    if setup_failure:
        environment.pop("SPARK_HOME")
    result = subprocess.run(["bash", str(runner), backend], env=environment, capture_output=True, text=True, timeout=5)
    phases = calls.read_text(encoding="utf-8").splitlines() if calls.exists() else []
    return result, phases, checkpoint, submit, pid


def _assert_cleanup(backend, checkpoint, submit, pid):
    if backend in {"spark-connect40", "spark-connect41"}:
        assert not checkpoint.exists()
        assert "spark.checkpoint.dir=" in submit.read_text(encoding="utf-8")
    else:
        assert checkpoint.exists()
    if backend.startswith("spark-connect"):
        with pytest.raises(ProcessLookupError):
            os.kill(int(pid.read_text(encoding="utf-8")), 0)


@pytest.mark.parametrize("backend", ("pyspark35", "pyspark40", "pyspark41", "spark-connect35", "spark-connect40", "spark-connect41"))
def test_compose_uses_mounted_launcher(backend) -> None:
    compose = Path("infra/compose/docker-compose.yaml").read_text(encoding="utf-8")
    section = compose.split(f"  structure-integration-{backend}:\n", 1)[1].split("\n  structure-", 1)[0]
    assert f"command: [bash, /workspace/infra/compose/images/pyspark/run-integration.sh, {backend}]" in section
    assert "target: /workspace\n        read_only: true" in section
    assert "command: run-integration" not in section


@pytest.mark.parametrize("backend", ("all", "pyspark35", "pyspark40", "pyspark41", "spark-connect35", "spark-connect40", "spark-connect41"))
@pytest.mark.parametrize("build", (False, True))
def test_orchestrator_builds_runners_and_starts_selected_services(tmp_path, monkeypatch, backend, build) -> None:
    monkeypatch.syspath_prepend(str(Path("scripts").resolve()))
    runner = importlib.import_module("run_integration")
    for name in runner.RUNNER_OVERRIDES:
        monkeypatch.delenv(name, raising=False)
    calls = []
    monkeypatch.setattr(runner, "parse", lambda: argparse.Namespace(backend=backend, build=build, down=False))
    monkeypatch.setattr(runner, "ensure_compose_env", lambda: None)
    monkeypatch.setattr(runner, "WORKSPACE_TMP", tmp_path / "integration")
    monkeypatch.setattr(runner, "run", lambda *args: calls.append(args))
    runner.main()
    backends = runner.BACKENDS if backend == "all" else (backend,)
    expected = []
    if build:
        expected.append(("build", *(f"structure-integration-{name}" for name in backends)))
    mappings = {
        "pyspark35": ("spark35-master", "spark35-worker"),
        "spark-connect35": ("spark35-master", "spark35-worker"),
        "pyspark40": ("spark40-master", "spark40-worker"),
        "spark-connect40": ("spark40-master", "spark40-worker"),
        "pyspark41": ("spark41-master", "spark41-worker"),
        "spark-connect41": (),
    }
    assert runner.SERVICES == mappings
    services = tuple(dict.fromkeys(service for name in backends for service in mappings[name]))
    if services:
        expected.append(("up", "-d", "--no-recreate", *(("--build",) if build else ()), *services))
    expected.extend(("run", "--rm", f"structure-integration-{name}") for name in backends)
    assert calls == expected
    assert tmp_path.joinpath("integration").is_dir()


def test_orchestrator_forwards_filters_deadlines_and_phase_selection(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(Path("scripts").resolve()))
    runner = importlib.import_module("run_integration")
    calls = []
    monkeypatch.setattr(runner, "parse", lambda: argparse.Namespace(backend="pyspark35", build=False, down=False))
    monkeypatch.setattr(runner, "ensure_compose_env", lambda: None)
    monkeypatch.setattr(runner, "WORKSPACE_TMP", tmp_path / "integration")
    monkeypatch.setattr(runner, "run", lambda *args: calls.append(args))
    monkeypatch.setenv("INTEGRATION_PYTEST_ARGS", "-k runtime_versions -vv")
    monkeypatch.setenv("STRUCTURE_INTEGRATION_TIMEOUT", "600")
    monkeypatch.setenv("STRUCTURE_INTEGRATION_PHASE", "general-runtime")
    runner.main()
    assert calls[-1] == (
        "run", "--rm", "-e", "INTEGRATION_PYTEST_ARGS", "-e", "STRUCTURE_INTEGRATION_TIMEOUT",
        "-e", "STRUCTURE_INTEGRATION_PHASE", "structure-integration-pyspark35",
    )
    assert calls[0] == ("up", "-d", "--no-recreate", "spark35-master", "spark35-worker")


def test_orchestrator_stops_after_failed_backend(tmp_path, monkeypatch) -> None:
    monkeypatch.syspath_prepend(str(Path("scripts").resolve()))
    runner = importlib.import_module("run_integration")
    for name in runner.RUNNER_OVERRIDES:
        monkeypatch.delenv(name, raising=False)
    calls = []
    monkeypatch.setattr(runner, "parse", lambda: argparse.Namespace(backend="all", build=False, down=False))
    monkeypatch.setattr(runner, "ensure_compose_env", lambda: None)
    monkeypatch.setattr(runner, "WORKSPACE_TMP", tmp_path / "integration")

    def fail(*args):
        calls.append(args)
        if args[0] == "run":
            raise SystemExit(1)

    monkeypatch.setattr(runner, "run", fail)
    with pytest.raises(SystemExit):
        runner.main()
    assert len(calls) == 2
    assert calls[-1] == ("run", "--rm", "structure-integration-pyspark35")


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

    assert 'profile = ">=3.5,<4.2"' in text
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
