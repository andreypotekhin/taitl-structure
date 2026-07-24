from __future__ import annotations

import importlib
import importlib.metadata
import shutil
import site
import subprocess
import sys
from pathlib import Path
from typing import Protocol, cast

from structure.core.plugins.logic.PluginRegistry import PluginRegistry
from structure.plugin.api.v1 import ExecutionRequest
from structure.plugin.conformance import PluginConformance

FIXTURE = Path(__file__).parents[2] / "integration" / "platform_plugins" / "iterable_fixture"


class Collectable(Protocol):
    def collect(self) -> list[dict[str, object]]: ...


def test_isolated_fixture_wheel_discovers_executes_and_serializes(tmp_path, monkeypatch) -> None:
    assert "structure.core" not in "\n".join(path.read_text() for path in FIXTURE.glob("src/**/*.py"))
    wheelhouse = tmp_path / "wheelhouse"
    site_packages = tmp_path / "site-packages"
    poetry = shutil.which("poetry")
    assert poetry is not None
    subprocess.run(
        [poetry, "build", "--format", "wheel", "--output", str(wheelhouse)],
        cwd=FIXTURE,
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(wheelhouse.glob("*.whl"))
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(site_packages), str(wheel)],
        check=True,
        capture_output=True,
        text=True,
    )
    site.addsitedir(str(site_packages))
    monkeypatch.syspath_prepend(str(site_packages))
    importlib.invalidate_caches()

    selected = PluginRegistry().select("iterable")
    fixture = next(entry.load() for entry in importlib.metadata.entry_points(group="structure.plugin") if entry.name == "iterable")
    conformance = PluginConformance.negotiate(
        fixture,
        entry_name="iterable",
        distribution="structure-iterable-fixture",
    )

    assert selected.descriptor == conformance.descriptor
    assert selected.api.executor is not None
    relation = cast(
        Collectable,
        selected.api.executor.execute(ExecutionRequest(payload={"operation": "identity"}, runtime=iter(({"id": 1},)))),
    )
    assert relation.collect() == [{"id": 1}]
    assert relation.collect() == [{"id": 1}]
    assert selected.api.serializer is not None
    assert selected.api.serializer.decode(selected.api.serializer.encode({"operation": "identity"})) == {"operation": "identity"}
    assert fixture.__module__ == "structure_iterable_fixture.IterablePlugin"
