from dataclasses import dataclass

from structure import Transform, transform
from structure.core.compiler.artifacts.commands import GeneratePlatformArtifact, SerializePlatformArtifact
from structure.core.platform import PlatformConfiguration, PlatformRegistry
from structure.core.runtime.execution.commands import ExecutePlatformArtifact
from structure.platform.api import PlatformDescriptor
from structure.platform.api.v1 import PlatformAPI, PlatformCompilation


@transform(target="fake")
class FakeTransform(Transform):
    pass


@dataclass
class Distribution:
    name: str


class Entry:
    group = "structure.platform"
    dist = Distribution("fake-wheel")
    name = "fake"

    def load(self):
        return Plugin()


class Compiler:
    def compile(self, request):
        return PlatformCompilation(lowered=object(), fingerprint=f"{request.target}:fingerprint")


class Facet:
    def materialize(self, schema): return schema
    def supports(self, capability): return True


class Executor:
    def execute(self, payload, runtime):
        return runtime[payload]


class Serializer:
    def encode(self, payload): return b"payload"
    def decode(self, payload): return object()


class Generator:
    def generate(self, payload): return {"generated/module.py": "content"}


class Plugin:
    descriptor = PlatformDescriptor("fake", "Fake", "fake-wheel", "1.0", 1, 1)
    def api(self, version): return PlatformAPI(Facet(), Compiler(), Facet(), executor=Executor(), generator=Generator(), serializer=Serializer())


def test_core_artifact_preserves_plugin_identity_without_inspecting_payload() -> None:
    artifact = FakeTransform.compile(
        platform_registry=PlatformRegistry(lambda: [Entry()]),
        platform_configuration=PlatformConfiguration.resolve({"platform": {"fake": {"profile": "test"}}}),
    )

    assert (artifact.platform, artifact.distribution, artifact.api_version) == ("fake", "fake-wheel", 1)
    assert artifact.configuration == (("profile", "test"),)
    assert type(artifact.payload) is object


def test_core_execution_routes_the_opaque_payload_only_after_identity_validation() -> None:
    configuration = PlatformConfiguration.resolve({"platform": {"fake": {}}})
    registry = PlatformRegistry(lambda: [Entry()])
    artifact = FakeTransform.compile(platform_registry=registry, platform_configuration=configuration)

    assert ExecutePlatformArtifact(registry)(artifact, configuration=configuration, runtime={artifact.payload: "done"}) == "done"


def test_core_serialization_preserves_the_artifact_envelope() -> None:
    configuration = PlatformConfiguration.resolve({"platform": {"fake": {}}})
    registry = PlatformRegistry(lambda: [Entry()])
    artifact = FakeTransform.compile(platform_registry=registry, platform_configuration=configuration)

    decoded = SerializePlatformArtifact(registry).decode(artifact, b"payload", configuration=configuration)
    assert SerializePlatformArtifact(registry).encode(artifact, configuration=configuration) == b"payload"
    assert decoded.payload is not artifact.payload
    assert decoded.fingerprint == artifact.fingerprint


def test_core_generation_returns_content_without_writing_files() -> None:
    configuration = PlatformConfiguration.resolve({"platform": {"fake": {}}})
    registry = PlatformRegistry(lambda: [Entry()])
    artifact = FakeTransform.compile(platform_registry=registry, platform_configuration=configuration)

    assert GeneratePlatformArtifact(registry)(artifact, configuration=configuration) == {"generated/module.py": "content"}
