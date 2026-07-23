from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import Compiler
from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.plugin.api.v1 import ExplainRequest
from structure.plugin.api.v1 import PluginAPI as PluginAPIV1
from structure.plugin.pyspark import *
from structure.plugin.pyspark.api.AnalysisAPI import AnalysisAPI
from structure.plugin.pyspark.api.AuthoringAPI import AuthoringAPI
from structure.plugin.pyspark.api.CapabilitiesAPI import CapabilitiesAPI
from structure.plugin.pyspark.api.CompilerAPI import CompilerAPI
from structure.plugin.pyspark.api.ExecutionAPI import ExecutionAPI
from structure.plugin.pyspark.api.ExplainAPI import ExplainAPI
from structure.plugin.pyspark.api.GenerationAPI import GenerationAPI
from structure.plugin.pyspark.api.PluginAPI import PluginAPI as PySparkPluginAPI
from structure.plugin.pyspark.api.SchemaAPI import SchemaAPI


def test_bundled_pyspark_platform_exposes_the_v1_facade() -> None:
    api = PySparkPlugin().api(1)

    assert PySparkPlugin.descriptor.name == "pyspark"
    assert api.schema is not None
    assert api.compiler is not None
    assert api.authoring is not None
    assert (
        api.capabilities.resolve(profile=">=3.5,<4.1", variant="ordinary")
        .require(CapabilityRequirement(group="join", name="inner_join"))
        .supported
    )
    assert api.explainer is not None


def test_pyspark_plugin_api_composes_named_v1_facet_adapters() -> None:
    api = PySparkPluginAPI().create()

    assert isinstance(api, PluginAPIV1)
    assert isinstance(api.schema, SchemaAPI)
    assert isinstance(api.compiler, CompilerAPI)
    assert isinstance(api.capabilities, CapabilitiesAPI)
    assert isinstance(api.authoring, AuthoringAPI)
    assert isinstance(api.executor, ExecutionAPI)
    assert isinstance(api.generator, GenerationAPI)
    assert isinstance(api.explainer, ExplainAPI)
    assert isinstance(api.analysis, AnalysisAPI)


class Source(Schema):
    id = string(nullable=False)


class Result(Schema):
    id = string(nullable=False)


@transform
class Publish(Transform):
    source = input(Source)
    result = output(Result)

    def publish(self, row: Source) -> Result:
        return Result(id=row.id)


def test_pyspark_compiler_and_explainer_consume_core_supplied_analysis(monkeypatch) -> None:
    api = PySparkPlugin().api(1)
    compilation = Compiler.frontend.compile()(Publish, materialize_schemas=False)  # type: ignore[attr-defined]
    analysis = compilation.analysis
    assert analysis is not None

    monkeypatch.setattr(Compiler.frontend, "compile", lambda: (_ for _ in ()).throw(AssertionError("Core re-entry")))

    assert api.explainer is not None
    report = api.explainer.render(ExplainRequest(Publish, payload=compilation.lowered, analysis=analysis))

    assert "Publish" in report
