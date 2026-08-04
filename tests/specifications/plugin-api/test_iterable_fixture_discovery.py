import importlib
import importlib.metadata
import shutil
import site
import subprocess
import sys
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from structure import Schema, StructureSession, Transform, input, output, step, transform
from structure.core.plugins.api import Plugin
from structure.core.plugins.logic.PluginRegistry import PluginRegistry
from structure.core.plugins.model.EngineManifest import EngineManifest
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.core.runtime.execution.commands.ExecutePluginArtifact import ExecutePluginArtifact
from structure.plugin.api.v1 import GenerationRequest
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
    operations = importlib.import_module("structure_iterable")

    class Order(Schema):
        id: int = operations.field(nullable=False)
        customer_id: int = operations.field(nullable=False)

    class Customer(Schema):
        id: int = operations.field(nullable=False)
        name: str = operations.field(nullable=False)

    class Enriched(Schema):
        id: int = operations.field(nullable=False, alias="order_id")
        customer_name: str = operations.field(alias="customer")

    class Metadata(Schema):
        value: str = operations.field(description="A documented value", metadata={"source": "test"})

    class Base(Schema):
        value: str = operations.field(alias="result")

    with pytest.raises(TypeError, match="needs a Python type hint"):

        class MissingHint(Schema):
            value = operations.field()

    with pytest.raises(ValueError, match="duplicate Iterable output key"):

        class DuplicateAlias(Base):
            other: str = operations.field(alias="result")

    with pytest.raises(TypeError, match="nullable must be a bool"):
        operations.field(nullable="yes")
    with pytest.raises(ValueError, match="alias must be a non-empty string"):
        operations.field(alias="")
    with pytest.raises(TypeError, match="description must be a string"):
        operations.field(description=1)
    with pytest.raises(TypeError, match="metadata must be a mapping"):
        operations.field(metadata=["source"])
    assert Metadata._structure_fields["value"].metadata == {"source": "test"}
    with pytest.raises(TypeError):
        Metadata._structure_fields["value"].metadata["changed"] = True

    @transform(target="iterable")
    class Projected(Transform):
        orders = input(Order)
        customers = input(Customer)
        result = output(Enriched)

        @step(input=[orders, customers], output=result)
        def enrich(self, order: Order, customer: Customer) -> Enriched:
            operations.left_join(customer, on=customer.id == order.customer_id)
            return Enriched(id=order.id, customer_name=customer.name)

    registry = PluginRegistry()
    configuration = PluginConfiguration.resolve({"plugin": {"iterable": {}}})
    projected = Projected.compile(plugin_registry=registry, plugin_configuration=configuration)
    executor = ExecutePluginArtifact(registry)
    relation = cast(
        Collectable,
        executor(
            projected,
            configuration=configuration,
            runtime={"orders": [{"id": 1, "customer_id": 7}], "customers": [{"id": 7, "name": "Ada"}]},
        ),
    )
    assert relation.collect() == [{"order_id": 1, "customer": "Ada"}]
    assert selected.api.generator is not None
    generated = selected.api.generator.generate(
        GenerationRequest(
            payload={"sample.transforms.Projected": projected.payload},
            source_module="sample.transforms",
            generated_package="generated",
        )
    )
    source = generated.files["generated/iterable/transforms/sample/transforms.py"]
    assert "for row in orders:" in source
    duplicate_one = selected.api.generator.generate(
        GenerationRequest(
            payload={"demo.catalog.prepare.Projected": projected.payload},
            source_module="demo.catalog.prepare",
            generated_package="generated",
        )
    )
    duplicate_two = selected.api.generator.generate(
        GenerationRequest(
            payload={"demo.fulfillment.prepare.Projected": projected.payload},
            source_module="demo.fulfillment.prepare",
            generated_package="generated",
        )
    )
    assert duplicate_one.module_name == "generated.iterable.transforms.demo.catalog.prepare"
    assert duplicate_two.module_name == "generated.iterable.transforms.demo.fulfillment.prepare"
    assert (
        "generated/iterable/transforms/demo/catalog/prepare.py" in duplicate_one.files
        and "generated/iterable/transforms/demo/fulfillment/prepare.py" in duplicate_two.files
    )
    namespace: dict[str, object] = {}
    exec(source, namespace)
    generated_class = cast(Any, namespace["ProjectedGenerated"])
    assert generated_class.run(orders=[{"id": 1, "customer_id": 7}], customers=[{"id": 7, "name": "Ada"}]) == [
        {"order_id": 1, "customer": "Ada"}
    ]

    class RequiredCustomer(Schema):
        id: int = operations.field(nullable=False)
        customer_name: str = operations.field(nullable=False)

    @transform(target="iterable")
    class UnsafeLeftJoin(Transform):
        orders = input(Order)
        customers = input(Customer)
        result = output(RequiredCustomer)

        @step(input=[orders, customers], output=result)
        def enrich(self, order: Order, customer: Customer) -> RequiredCustomer:
            operations.left_join(customer, on=customer.id == order.customer_id)
            return RequiredCustomer(id=order.id, customer_name=customer.name)

    @transform(target="iterable")
    class UnsafeNone(Transform):
        orders = input(Order)
        result = output(RequiredCustomer)

        @step(input=orders, output=result)
        def enrich(self, order: Order) -> RequiredCustomer:
            return RequiredCustomer(id=order.id, customer_name=None)

    with pytest.raises(TypeError, match="RequiredCustomer.customer_name is non-nullable"):
        UnsafeLeftJoin.compile(plugin_registry=registry, plugin_configuration=configuration)
    with pytest.raises(TypeError, match="RequiredCustomer.customer_name is non-nullable"):
        UnsafeNone.compile(plugin_registry=registry, plugin_configuration=configuration)
    assert selected.api.serializer is not None
    assert selected.api.serializer.decode(selected.api.serializer.encode({"operation": "identity"})) == {"operation": "identity"}
    assert fixture.__module__ == "structure_iterable.IterablePlugin"
