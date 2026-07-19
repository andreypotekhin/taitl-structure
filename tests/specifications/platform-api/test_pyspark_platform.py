from structure import Schema, Transform, input, output, transform
from structure.core.compiler.api import Compiler
from structure.core.target.capabilities.model.CapabilityRequirement import CapabilityRequirement
from structure.platform.api.v1 import ExplainRequest
from structure.platform.pyspark import PySparkPlatform, field


def test_bundled_pyspark_platform_exposes_the_v1_facade() -> None:
    api = PySparkPlatform().api(1)

    assert PySparkPlatform.descriptor.name == "pyspark"
    assert api.schema is not None
    assert api.compiler is not None
    assert api.authoring is not None
    assert api.capabilities.resolve(profile=">=3.5,<4.1", variant="ordinary").require(
        CapabilityRequirement(group="join", name="inner_join")
    ).supported
    assert api.explainer is not None


class Source(Schema):
    id = field.string(nullable=False)


class Result(Schema):
    id = field.string(nullable=False)


@transform
class Publish(Transform):
    source = input(Source)
    result = output(Result)

    def publish(self, row: Source) -> Result:
        return Result(id=row.id)


def test_pyspark_compiler_and_explainer_consume_core_supplied_analysis(monkeypatch) -> None:
    api = PySparkPlatform().api(1)
    compilation = Compiler.frontend.compile()(Publish, materialize_schemas=False)  # type: ignore[attr-defined]
    analysis = compilation.analysis
    assert analysis is not None

    monkeypatch.setattr(Compiler.frontend, "compile", lambda: (_ for _ in ()).throw(AssertionError("Core re-entry")))

    report = api.explainer.render(ExplainRequest(Publish, payload=compilation.lowered, analysis=analysis))

    assert "Publish" in report
