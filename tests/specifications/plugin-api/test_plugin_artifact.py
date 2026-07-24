from dataclasses import dataclass
from typing import Any, cast

import pytest

from structure import Schema, StructureConfig, Transform, input, output, transform
from structure.core.compiler.api import Compiler as CoreCompiler
from structure.core.compiler.artifacts.commands import GeneratePluginArtifact, SerializePluginArtifact
from structure.core.plugins.api import Plugin
from structure.core.plugins.model.PluginConfiguration import PluginConfiguration
from structure.core.runtime.execution.commands import ExecutePluginArtifact
from structure.core.target.capabilities.model.BackendCapabilities import BackendCapabilities
from structure.plugin.api import PluginDescriptor
from structure.plugin.api.v1 import (
    ExecutionRequest,
    GenerationRequest,
    PluginAPI,
    PluginCompilation,
    StepAuthoringRequest,
)
from structure.plugin.pyspark import *
from structure.plugin.pyspark.symbolic_execution.model import (
    PySparkStepBody,
    PySparkSymbolicContext,
    current_pyspark_context,
)


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
    group = "structure.plugin"
    dist = Distribution("fake-wheel")
    name = "fake"

    def load(self):
        return FakePlugin()


class Compiler:
    def compile(self, request):
        return PluginCompilation(lowered=object(), fingerprint=f"{request.target}:fingerprint")


class Facet:
    def validate(self, request):
        return None

    def materialize(self, schema):
        return schema

    def build(self, request):
        return request.payload

    def read(self, request):
        return request.schema

    def source(self, schema, *, to):
        return schema

    def resolve(self, *, profile, variant):
        return cast(BackendCapabilities, object())

    def open_step(self, request):
        raise AssertionError("Fake plugin does not author transform steps.")

    def result_arguments(self, results):
        return ()

    def rewrite_body(self, body, *, frames):
        return body


class Executor:
    def execute(self, request: ExecutionRequest):
        return cast(Any, request.runtime)[request.payload]


class Serializer:
    def encode(self, payload):
        return b"payload"

    def decode(self, payload):
        return object()


class Generator:
    def generate(self, request: GenerationRequest):
        return {"generated/module.py": "content"}


class FakePlugin:
    descriptor = PluginDescriptor("fake", "Fake", "fake-wheel", "1.0", 1, 1)

    def api(self, version):
        return PluginAPI(
            Facet(), Compiler(), Facet(), Facet(), executor=Executor(), generator=Generator(), serializer=Serializer()
        )


class CapturingCompiler:
    def __init__(self) -> None:
        self.request = None
        self.payload = object()

    def compile(self, request):
        self.request = request
        return PluginCompilation(lowered=self.payload, fingerprint="pyspark:fingerprint")


class RecordingAuthoring:
    def __init__(self) -> None:
        self.requests: list[object] = []
        self.capture_contexts: list[PySparkSymbolicContext | None] = []
        self._delegate = PySparkPlugin().api(1).authoring

    def open_step(self, request):
        self.requests.append(request)
        return RecordingSession(self._delegate.open_step(request), self.capture_contexts)

    def result_arguments(self, results):
        return self._delegate.result_arguments(results)

    def rewrite_body(self, body, *, frames):
        return self._delegate.rewrite_body(body, frames=frames)


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

    def validate(self):
        return self._delegate.validate()

    def capture(self, value):
        self._capture_contexts.append(current_pyspark_context())
        return self._delegate.capture(value)


class FakePySparkPlugin:
    descriptor = PluginDescriptor("pyspark", "PySpark", "pyspark-wheel", "1.0", 1, 1)

    def __init__(self, compiler, authoring=None, schema=None) -> None:
        self._compiler = compiler
        self._authoring = authoring or PySparkPlugin().api(1).authoring
        self._schema = schema or Facet()

    def api(self, version):
        return PluginAPI(self._schema, self._compiler, Facet(), authoring=self._authoring)


class PySparkEntry:
    group = "structure.plugin"
    dist = Distribution("pyspark-wheel")
    name = "pyspark"

    def __init__(self, compiler, authoring=None, schema=None) -> None:
        self._compiler = compiler
        self._authoring = authoring
        self._schema = schema

    def load(self):
        return FakePySparkPlugin(self._compiler, self._authoring, self._schema)


def test_core_artifact_preserves_plugin_identity_without_inspecting_payload() -> None:
    artifact = FakeTransform.compile(
        plugin_registry=Plugin.registry(lambda: [Entry()]),
        plugin_configuration=PluginConfiguration.resolve({"plugin": {"fake": {"profile": "test"}}}),
    )

    assert (artifact.plugin, artifact.distribution, artifact.api_version) == ("fake", "fake-wheel", 1)
    assert artifact.configuration == (("profile", "test"),)
    assert type(artifact.payload) is object


def test_core_execution_routes_the_opaque_payload_only_after_identity_validation() -> None:
    configuration = PluginConfiguration.resolve({"plugin": {"fake": {}}})
    registry = Plugin.registry(lambda: [Entry()])
    artifact = FakeTransform.compile(plugin_registry=registry, plugin_configuration=configuration)

    assert (
        ExecutePluginArtifact(registry)(artifact, configuration=configuration, runtime={artifact.payload: "done"})
        == "done"
    )


def test_core_serialization_preserves_the_artifact_envelope() -> None:
    configuration = PluginConfiguration.resolve({"plugin": {"fake": {}}})
    registry = Plugin.registry(lambda: [Entry()])
    artifact = FakeTransform.compile(plugin_registry=registry, plugin_configuration=configuration)

    decoded = SerializePluginArtifact(registry).decode(artifact, b"payload", configuration=configuration)
    assert SerializePluginArtifact(registry).encode(artifact, configuration=configuration) == b"payload"
    assert decoded.payload is not artifact.payload
    assert decoded.fingerprint == artifact.fingerprint


def test_core_generation_returns_content_without_writing_files() -> None:
    configuration = PluginConfiguration.resolve({"plugin": {"fake": {}}})
    registry = Plugin.registry(lambda: [Entry()])
    artifact = FakeTransform.compile(plugin_registry=registry, plugin_configuration=configuration)

    assert GeneratePluginArtifact(registry)(artifact, configuration=configuration) == {"generated/module.py": "content"}


def test_core_frontend_compiles_analysis_before_calling_the_platform_facet() -> None:
    compiler = CapturingCompiler()
    authoring = RecordingAuthoring()
    registry = Plugin.registry(lambda: [PySparkEntry(compiler, authoring)])

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
    step = compilation.analysis.steps[0]
    assert isinstance(step.plugin_body, PySparkStepBody)
    assert not any(hasattr(step, field) for field in ("filters", "projection", "aggregate", "joins", "operations"))
    assert not any(hasattr(step.results[0], field) for field in ("projection", "aggregate"))


def test_core_passes_only_selected_opaque_plugin_options_to_plugin_facets() -> None:
    compiler = CapturingCompiler()
    authoring = RecordingAuthoring()
    registry = Plugin.registry(lambda: [PySparkEntry(compiler, authoring)])
    config = StructureConfig.create(
        plugin={"pyspark": {"vendor_mode": "fast"}, "other": {"must_not_reach_pyspark": True}}
    )

    CoreCompiler.frontend.compile()(  # type: ignore[attr-defined]
        CompiledTransform, config=config, registry=registry, materialize_schemas=False
    )

    request = compiler.request
    authoring_request = authoring.requests[0]
    assert request is not None
    assert isinstance(authoring_request, StepAuthoringRequest)
    assert request.plugin_options == {"vendor_mode": "fast"}
    assert authoring_request.plugin_options == {"vendor_mode": "fast"}
    with pytest.raises(TypeError):
        request.plugin_options["vendor_mode"] = "safe"


def test_pyspark_lowering_consumes_the_captured_body_not_core_target_fields() -> None:
    compiler = CapturingCompiler()
    registry = Plugin.registry(lambda: [PySparkEntry(compiler, RecordingAuthoring())])
    CoreCompiler.frontend.compile()(CompiledTransform, registry=registry, materialize_schemas=False)  # type: ignore[attr-defined]

    assert compiler.request is not None
    plan = compiler.request.analysis
    step = plan.steps[0]
    assert isinstance(step.plugin_body, PySparkStepBody)
    lowered = PySpark.compiler.lower()(plan)

    assert [assignment.field.name for assignment in lowered.steps[0].projection] == ["id"]


def test_core_validates_schemas_before_invoking_a_platform_authored_step(monkeypatch) -> None:
    class RejectingSchema(Facet):
        def validate(self, request):
            raise ValueError("schema rejected")

    registry = Plugin.registry(lambda: [PySparkEntry(CapturingCompiler(), RecordingAuthoring(), RejectingSchema())])
    monkeypatch.setattr(CompiledTransform, "publish", lambda *args: pytest.fail("step method was invoked"))

    with pytest.raises(ValueError, match="schema rejected"):
        CoreCompiler.frontend.compile()(CompiledTransform, registry=registry)  # type: ignore[attr-defined]
