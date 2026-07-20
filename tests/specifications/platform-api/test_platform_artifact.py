from dataclasses import dataclass
from typing import Any, cast

import pytest

from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import Compiler as CoreCompiler
from structure.core.compiler.artifacts.commands import GeneratePlatformArtifact, SerializePlatformArtifact
from structure.core.platforms.api import Platform
from structure.core.platforms.model.PlatformConfiguration import PlatformConfiguration
from structure.core.runtime.execution.commands import ExecutePlatformArtifact
from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.platform.api import PlatformDescriptor
from structure.platform.api.v1 import ExecutionRequest, GenerationRequest, PlatformAPI, PlatformCompilation
from structure.platform.pyspark import *
from structure.platform.pyspark.symbolic_execution.model import PySparkSymbolicContext, current_pyspark_context


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
    def validate(self, request): return None
    def materialize(self, schema): return schema
    def build(self, request): return request.payload
    def read(self, request): return request.schema
    def source(self, schema, *, to): return schema
    def resolve(self, *, profile, variant): return cast(BackendCapabilities, object())
    def open_step(self, request): raise AssertionError("Fake platform does not author transform steps.")


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
    def api(self, version): return PlatformAPI(Facet(), Compiler(), Facet(), Facet(), executor=Executor(), generator=Generator(), serializer=Serializer())


class CapturingCompiler:
    def __init__(self) -> None:
        self.request = None
        self.payload = object()

    def compile(self, request):
        self.request = request
        return PlatformCompilation(lowered=self.payload, fingerprint="pyspark:fingerprint")


class RecordingAuthoring:
    def __init__(self) -> None:
        self.requests: list[object] = []
        self.capture_contexts: list[PySparkSymbolicContext | None] = []
        self._delegate = PySparkPlatform().api(1).authoring

    def open_step(self, request):
        self.requests.append(request)
        return RecordingSession(self._delegate.open_step(request), self.capture_contexts)


class RecordingSession:
    def __init__(self, delegate, capture_contexts) -> None:
        self._delegate = delegate
        self._capture_contexts = capture_contexts

    def __enter__(self):
        self._delegate.__enter__()
        return self

    def __exit__(self, exc_type, exc, traceback):
        return self._delegate.__exit__(exc_type, exc, traceback)

    def arguments(self):
        return self._delegate.arguments()

    def context(self):
        return self._delegate.context()

    def capture(self, value):
        self._capture_contexts.append(current_pyspark_context())
        return self._delegate.capture(value)


class PySparkPlugin:
    descriptor = PlatformDescriptor("pyspark", "PySpark", "pyspark-wheel", "1.0", 1, 1)

    def __init__(self, compiler, authoring=None, schema=None) -> None:
        self._compiler = compiler
        self._authoring = authoring or PySparkPlatform().api(1).authoring
        self._schema = schema or Facet()

    def api(self, version):
        return PlatformAPI(self._schema, self._compiler, Facet(), authoring=self._authoring)


class PySparkEntry:
    group = "structure.platform"
    dist = Distribution("pyspark-wheel")
    name = "pyspark"

    def __init__(self, compiler, authoring=None, schema=None) -> None:
        self._compiler = compiler
        self._authoring = authoring
        self._schema = schema

    def load(self):
        return PySparkPlugin(self._compiler, self._authoring, self._schema)


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
    authoring = RecordingAuthoring()
    registry = Platform.registry(lambda: [PySparkEntry(compiler, authoring)])

    compilation = CoreCompiler.frontend.compile()(  # type: ignore[attr-defined]
        CompiledTransform, registry=registry, materialize_schemas=False
    )

    assert compiler.request is not None
    assert [request.name for request in authoring.requests] == ["publish"]
    assert len(authoring.capture_contexts) == 1
    assert authoring.capture_contexts[0] is not None
    assert compiler.request.analysis is compilation.analysis
    assert compilation.lowered is compiler.payload
    assert compilation.analysis.name == "CompiledTransform"
    assert compilation.analysis.steps[0].platform_body is not None


def test_core_validates_schemas_before_invoking_a_platform_authored_step(monkeypatch) -> None:
    class RejectingSchema(Facet):
        def validate(self, request):
            raise ValueError("schema rejected")

    registry = Platform.registry(lambda: [PySparkEntry(CapturingCompiler(), RecordingAuthoring(), RejectingSchema())])
    monkeypatch.setattr(CompiledTransform, "publish", lambda *args: pytest.fail("step method was invoked"))

    with pytest.raises(ValueError, match="schema rejected"):
        CoreCompiler.frontend.compile()(CompiledTransform, registry=registry)  # type: ignore[attr-defined]
