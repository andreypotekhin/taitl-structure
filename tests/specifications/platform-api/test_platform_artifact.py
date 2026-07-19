from dataclasses import dataclass
from typing import Any, cast

from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import Compiler as CoreCompiler
from structure.core.compiler.artifacts.commands import GeneratePlatformArtifact, SerializePlatformArtifact
from structure.core.platforms.api import Platform
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.core.runtime.execution.commands import ExecutePlatformArtifact
from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.field import string
from structure.platform.api import PlatformDescriptor
from structure.platform.api.v1 import ExecutionRequest, GenerationRequest, PlatformAPI, PlatformCompilation


@transform(target="fake")
class FakeTransform(Transform):
    pass


class Source(Schema):
    id = string(nullable=False)


class Result(Schema):
    id = string(nullable=False)


@transform
class CompiledTransform(Transform):
    source = input(Source)
    result = output(Result)

    def publish(self, row: Source) -> Result:
        return Result(id=row.id)


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
    def build(self, request): return request.payload
    def read(self, request): return request.schema
    def source(self, schema, *, to): return schema
    def resolve(self, *, profile, variant): return cast(BackendCapabilities, object())


class Executor:
    def execute(self, request: ExecutionRequest):
        return cast(Any, request.runtime)[request.payload]


class Serializer:
    def encode(self, payload): return b"payload"
    def decode(self, payload): return object()


class Generator:
    def generate(self, request: GenerationRequest): return {"generated/module.py": "content"}


class Plugin:
    descriptor = PlatformDescriptor("fake", "Fake", "fake-wheel", "1.0", 1, 1)
    def api(self, version): return PlatformAPI(Facet(), Compiler(), Facet(), executor=Executor(), generator=Generator(), serializer=Serializer())


class CapturingCompiler:
    def __init__(self) -> None:
        self.request = None
        self.payload = object()

    def compile(self, request):
        self.request = request
        return PlatformCompilation(lowered=self.payload, fingerprint="pyspark:fingerprint")


class PySparkPlugin:
    descriptor = PlatformDescriptor("pyspark", "PySpark", "pyspark-wheel", "1.0", 1, 1)

    def __init__(self, compiler) -> None:
        self._compiler = compiler

    def api(self, version):
        return PlatformAPI(Facet(), self._compiler, Facet())


class PySparkEntry:
    group = "structure.platform"
    dist = Distribution("pyspark-wheel")
    name = "pyspark"

    def __init__(self, compiler) -> None:
        self._compiler = compiler

    def load(self):
        return PySparkPlugin(self._compiler)


def test_core_artifact_preserves_plugin_identity_without_inspecting_payload() -> None:
    artifact = FakeTransform.compile(
        platform_registry=Platform.registry(lambda: [Entry()]),
        platform_configuration=PlatformConfiguration.resolve({"platform": {"fake": {"profile": "test"}}}),
    )

    assert (artifact.platform, artifact.distribution, artifact.api_version) == ("fake", "fake-wheel", 1)
    assert artifact.configuration == (("profile", "test"),)
    assert type(artifact.payload) is object


def test_core_execution_routes_the_opaque_payload_only_after_identity_validation() -> None:
    configuration = PlatformConfiguration.resolve({"platform": {"fake": {}}})
    registry = Platform.registry(lambda: [Entry()])
    artifact = FakeTransform.compile(platform_registry=registry, platform_configuration=configuration)

    assert ExecutePlatformArtifact(registry)(artifact, configuration=configuration, runtime={artifact.payload: "done"}) == "done"


def test_core_serialization_preserves_the_artifact_envelope() -> None:
    configuration = PlatformConfiguration.resolve({"platform": {"fake": {}}})
    registry = Platform.registry(lambda: [Entry()])
    artifact = FakeTransform.compile(platform_registry=registry, platform_configuration=configuration)

    decoded = SerializePlatformArtifact(registry).decode(artifact, b"payload", configuration=configuration)
    assert SerializePlatformArtifact(registry).encode(artifact, configuration=configuration) == b"payload"
    assert decoded.payload is not artifact.payload
    assert decoded.fingerprint == artifact.fingerprint


def test_core_generation_returns_content_without_writing_files() -> None:
    configuration = PlatformConfiguration.resolve({"platform": {"fake": {}}})
    registry = Platform.registry(lambda: [Entry()])
    artifact = FakeTransform.compile(platform_registry=registry, platform_configuration=configuration)

    assert GeneratePlatformArtifact(registry)(artifact, configuration=configuration) == {"generated/module.py": "content"}


def test_core_frontend_compiles_analysis_before_calling_the_platform_facet() -> None:
    compiler = CapturingCompiler()
    registry = Platform.registry(lambda: [PySparkEntry(compiler)])

    compilation = CoreCompiler.frontend.compile()(CompiledTransform, registry=registry, materialize_schemas=False)

    assert compiler.request is not None
    assert compiler.request.analysis is compilation.analysis
    assert compilation.lowered is compiler.payload
    assert compilation.analysis.name == "CompiledTransform"
