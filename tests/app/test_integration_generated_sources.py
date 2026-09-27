from typing import Any

from integration.pyspark.support.compiled_artifacts import CompiledArtifacts
from integration.pyspark.support.generated_sources import GeneratedSources

from structure import Transform


def test_generated_source_cache_is_owned_and_configuration_sensitive(monkeypatch) -> None:
    calls = []

    def render(*args, **kwargs):
        calls.append(kwargs)
        return {"generated.py": "source"}

    monkeypatch.setattr("integration.pyspark.support.generated_sources.render_generated_project", render)
    owner = CompiledArtifacts("module")
    sources = GeneratedSources(owner)
    request: dict[str, Any] = dict(generated_package="generated", source_schema_modules={})
    transforms = [(Transform, "example.Transform")]

    first = sources(transforms, **request)
    first["generated.py"] = "changed"
    assert sources(transforms, **request) == {"generated.py": "source"}
    assert len(calls) == 1

    sources(transforms, **request, allow_stage_outputs=False)
    sources(transforms, **request, generated_code_options=("comments",))
    monkeypatch.setenv("STRUCTURE_INTEGRATION_BACKEND", "spark-connect40")
    sources(transforms, **request)
    assert len(calls) == 4
    GeneratedSources()(transforms, **request)
    assert len(calls) == 5

    monkeypatch.setenv("STRUCTURE_PLAN_BOUNDARIES", "off")
    sources(transforms, **request)
    monkeypatch.setenv("STRUCTURE_PLAN_BOUNDARIES", "auto")
    sources(transforms, **request)
    assert len(calls) == 7
    sources(transforms, **request)
    assert len(calls) == 7
    assert sum(call["artifacts"] is not None for call in calls) == 6
    owner.close()
