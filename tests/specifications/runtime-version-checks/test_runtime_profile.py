from __future__ import annotations

import logging
import sys
from types import SimpleNamespace

import pytest

from structure.core.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.plugin.pyspark.execution.logic.ValidatePySparkRuntime import validate_pyspark_runtime


class Runtime:
    def __init__(self, version: str) -> None:
        self.version = version
        self.reads = 0

    @property
    def version(self) -> str:
        self.reads += 1
        return self._version

    @version.setter
    def version(self, value: str) -> None:
        self._version = value


def _session(runtime: Runtime, *, profile: str, policy: str = "raise"):
    return SimpleNamespace(
        runtime=runtime,
        plugin_options={
            "profile": profile,
            "variant": "ordinary",
            "runtime_version_check": policy,
        },
    )


def test_runtime_check_accepts_matching_client_and_engine_and_caches(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace(__version__="4.1.0-vendor"))
    runtime = Runtime("4.1.2-vendor")
    session = _session(runtime, profile=">=4.1,<4.2")

    validate_pyspark_runtime(session)
    validate_pyspark_runtime(session)

    assert runtime.reads == 1


@pytest.mark.parametrize(
    ("client", "engine", "profile", "component"),
    [
        ("4.1.0", "4.0.0", ">=4.1,<4.2", "engine"),
        ("4.0.0", "4.1.0", ">=4.1,<4.2", "client"),
        ("4.2.0", "4.2.0", ">=4.1,<4.2", "client, engine"),
    ],
)
def test_runtime_check_rejects_each_version_outside_profile(monkeypatch, client, engine, profile, component) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace(__version__=client))
    with pytest.raises(StructureRuntimeError) as raised:
        validate_pyspark_runtime(_session(Runtime(engine), profile=profile), transform="example.Clean")

    assert raised.value.diagnostic.code == "PYSPARK-E2710"
    assert component in raised.value.diagnostic.context["failing_component"]
    assert "[tool.structure.plugin.pyspark]" in str(raised.value)
    assert "example.Clean" in str(raised.value)


def test_runtime_log_mode_emits_one_error_and_continues(monkeypatch, caplog) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace(__version__="3.5.0"))
    runtime = Runtime("3.5.0")
    session = _session(runtime, profile=">=4.1,<4.2", policy="log")
    caplog.set_level(logging.ERROR, logger="structure.plugin.pyspark.runtime")

    validate_pyspark_runtime(session)
    validate_pyspark_runtime(session)

    assert runtime.reads == 1
    assert len(caplog.records) == 1
    assert "PYSPARK-E2710" in caplog.text


def test_runtime_off_does_not_import_client_or_read_engine(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", None)
    runtime = Runtime("invalid")

    validate_pyspark_runtime(_session(runtime, profile=">=4.1,<4.2", policy="off"))

    assert runtime.reads == 0


def test_runtime_off_still_rejects_unknown_profile_without_probing() -> None:
    runtime = Runtime("4.1.0")
    with pytest.raises(ValueError, match="Unsupported PySpark profile"):
        validate_pyspark_runtime(_session(runtime, profile=">=4.0,<4.2", policy="off"))
    assert runtime.reads == 0


def test_invalid_runtime_policy_fails_before_probing() -> None:
    runtime = Runtime("4.1.0")
    with pytest.raises(ValueError, match="runtime_version_check"):
        validate_pyspark_runtime(_session(runtime, profile=">=4.1,<4.2", policy="silent"))
    assert runtime.reads == 0


@pytest.mark.parametrize("version", [None, "", "4.x", "v4.1.0", 410])
def test_runtime_unknown_version_has_unavailable_diagnostic(monkeypatch, version) -> None:
    monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace(__version__=version))
    with pytest.raises(StructureRuntimeError) as raised:
        validate_pyspark_runtime(_session(Runtime("4.1.0"), profile=">=4.1,<4.2"))

    assert raised.value.diagnostic.code == "PYSPARK-E2711"
    assert "PySpark client" in raised.value.diagnostic.context["failing_component"]
