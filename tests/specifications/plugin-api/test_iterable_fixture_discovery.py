from __future__ import annotations

import importlib
import importlib.metadata
import shutil
import site
import subprocess
import sys
from pathlib import Path
from typing import Protocol, cast

import pytest

from structure import StructureSession, Transform, transform
from structure.core.plugins.api import Plugin
from structure.core.plugins.logic.PluginRegistry import PluginRegistry
from structure.core.plugins.model.EngineManifest import EngineManifest
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.core.runtime.execution.commands.ExecutePluginArtifact import ExecutePluginArtifact
from structure.plugin.api.v1 import ExecutionRequest
from structure.plugin.conformance import PluginConformance
from structure.version import VERSION

FIXTURE = Path(__file__).parents[3] / "examples" / "plugins" / "iterable"


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
        distribution="structure-iterable-example",
    )

    assert selected.descriptor == conformance.descriptor
    with pytest.raises(ValueError, match="not installed or is disabled"):
        PluginRegistry().select("iterable", disabled_distributions=frozenset({"structure_iterable_example"}))
    metadata = next(site_packages.glob("structure_iterable_example-*.dist-info"))
    duplicate = site_packages / "structure_iterable_conflict-0.1.dist-info"
    shutil.copytree(metadata, duplicate)
    duplicate_metadata = duplicate / "METADATA"
    duplicate_metadata.write_text(duplicate_metadata.read_text().replace("Name: structure-iterable-example", "Name: structure-iterable-conflict"))
    try:
        with pytest.raises(ValueError, match="multiple distributions"):
            PluginRegistry().discover()
    finally:
        shutil.rmtree(duplicate)
    manifest = EngineManifest(VERSION, "incompatible", {object: object})
    with pytest.raises(ValueError, match="allow_injection"):
        Plugin.resolve_engine()(
            object,
            plugin=selected.descriptor.name,
            distribution=selected.descriptor.distribution,
            manifest=manifest,
            configuration=PluginConfiguration.resolve({}),
        )
    with pytest.raises(ValueError, match="engine revision"):
        Plugin.resolve_engine()(
            object,
            plugin=selected.descriptor.name,
            distribution=selected.descriptor.distribution,
            manifest=manifest,
            configuration=PluginConfiguration.resolve({"plugin": {"plugin_options": "allow_injection"}}),
        )
    assert selected.api.executor is not None
    relation = cast(
        Collectable,
        selected.api.executor.execute(ExecutionRequest(payload={"operation": "identity"}, runtime=iter(({"id": 1},)))),
    )
    assert relation.collect() == [{"id": 1}]
    assert relation.collect() == [{"id": 1}]
    projection = cast(
        Collectable,
        selected.api.executor.execute(
            ExecutionRequest(
                payload={"operation": "project", "fields": {"order": "id"}}, runtime=iter(({"id": 1, "skip": True},))
            )
        ),
    )
    assert projection.collect() == [{"order": 1}]
    joined = cast(
        Collectable,
        selected.api.executor.execute(
            ExecutionRequest(
                payload={"operation": "inner_join", "left": "orders", "right": "customers", "left_on": "customer", "right_on": "id"},
                runtime={"orders": [{"order": 1, "customer": 7}], "customers": [{"id": 7, "name": "Ada"}]},
            )
        ),
    )
    assert joined.collect() == [{"id": 7, "name": "Ada", "order": 1, "customer": 7}]
    left_joined = cast(
        Collectable,
        selected.api.executor.execute(
            ExecutionRequest(
                payload={"operation": "left_join", "left": "orders", "right": "customers", "left_on": "customer", "right_on": "id"},
                runtime={"orders": [{"order": 2, "customer": 8}], "customers": [{"id": 7, "name": "Ada"}]},
            )
        ),
    )
    assert left_joined.collect() == [{"order": 2, "customer": 8, "id": None, "name": None}]
    aggregated = cast(
        Collectable,
        selected.api.executor.execute(
            ExecutionRequest(
                payload={
                    "operation": "aggregate",
                    "group_by": ["customer"],
                    "aggregates": {"total": {"sum": "amount"}, "orders": {"count": None}},
                },
                runtime=[{"customer": 7, "amount": 4}, {"customer": 7, "amount": 6}, {"customer": 8, "amount": 3}],
            )
        ),
    )
    assert aggregated.collect() == [{"customer": 7, "total": 10, "orders": 2}, {"customer": 8, "total": 3, "orders": 1}]
    recurrence = cast(
        Collectable,
        selected.api.executor.execute(
            ExecutionRequest(
                payload={
                    "operation": "recurrence",
                    "index": "index",
                    "value": "sequence",
                    "initial": [1],
                    "output": {"state": 0},
                    "next": [{"add": [{"state": 0}, {"literal": 2}]}],
                },
                runtime=[{"index": 0}, {"index": 1}, {"index": 2}],
            )
        ),
    )
    assert recurrence.collect() == [
        {"index": 0, "sequence": 1},
        {"index": 1, "sequence": 3},
        {"index": 2, "sequence": 5},
    ]
    operations = importlib.import_module("structure_iterable")

    @transform(target="iterable")
    class Projected(Transform):
        operation = operations.projection(fields={"order": "id"})

    @transform(target="iterable")
    class Joined(Transform):
        operation = operations.inner_join(left="orders", right="customers", left_on="customer", right_on="id")

    @transform(target="iterable")
    class Aggregated(Transform):
        operation = operations.grouped(
            group_by=("customer",), aggregates={"total": {"sum": "amount"}, "orders": {"count": None}}
        )

    registry = PluginRegistry()
    configuration = PluginConfiguration.resolve({"plugin": {"iterable": {}}})
    projected = Projected.compile(plugin_registry=registry, plugin_configuration=configuration)
    joined = Joined.compile(plugin_registry=registry, plugin_configuration=configuration)
    aggregated = Aggregated.compile(plugin_registry=registry, plugin_configuration=configuration)
    executor = ExecutePluginArtifact(registry)
    assert cast(Collectable, executor(projected, configuration=configuration, runtime=[{"id": 1, "skip": True}])).collect() == [
        {"order": 1}
    ]
    assert cast(
        Collectable,
        executor(
            joined,
            configuration=configuration,
            runtime={"orders": [{"order": 1, "customer": 7}], "customers": [{"id": 7, "name": "Ada"}]},
        ),
    ).collect() == [{"id": 7, "name": "Ada", "order": 1, "customer": 7}]
    assert cast(
        Collectable,
        executor(
            aggregated,
            configuration=configuration,
            runtime=[{"customer": 7, "amount": 4}, {"customer": 7, "amount": 6}],
        ),
    ).collect() == [{"customer": 7, "total": 10, "orders": 2}]
    assert cast(Collectable, Projected().run(StructureSession(runtime=[{"id": 1, "skip": True}])).result).collect() == [
        {"order": 1}
    ]
    school = importlib.import_module("examples.school.transforms.iterable")
    assert cast(
        Collectable,
        school.ProjectIterableScores(students=[{"student": "Ada", "score": 100, "ignored": True}])
        .run(StructureSession())
        .result,
    ).collect() == [{"student": "Ada", "score": 100}]
    sequences = importlib.import_module("examples.school.transforms.sequences")
    assert cast(
        Collectable,
        sequences.Fibonacci(rows=({"index": index} for index in range(4))).run(StructureSession()).result,
    ).collect() == [
        {"index": 0, "fibonacci": 0},
        {"index": 1, "fibonacci": 1},
        {"index": 2, "fibonacci": 1},
        {"index": 3, "fibonacci": 2},
    ]
    assert selected.api.serializer is not None
    assert selected.api.serializer.decode(selected.api.serializer.encode({"operation": "identity"})) == {"operation": "identity"}
    assert fixture.__module__ == "structure_iterable.IterablePlugin"
