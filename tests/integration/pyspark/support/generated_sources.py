from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

from integration.pyspark.support.backend_matrix import _plugin, render_generated_project
from integration.pyspark.support.compiled_artifacts import CompiledArtifacts

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.Transform import Transform


class GeneratedSources:
    """Module-owned source cache; each caller receives its own mutable file map."""

    def __init__(self, artifacts: CompiledArtifacts | None = None) -> None:
        self.artifacts = artifacts
        self.cache: dict[tuple[object, ...], dict[str, str]] = {}

    def __call__(
        self,
        transforms: Sequence[tuple[type[Transform], str]],
        *,
        generated_package: str,
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
        generated_code_options: tuple[str, ...] = (),
        allow_stage_outputs: bool = True,
    ) -> dict[str, str]:
        key = (
            tuple(transforms),
            generated_package,
            tuple((name, tuple(schemas)) for name, schemas in source_schema_modules.items()),
            generated_code_options,
            allow_stage_outputs,
            json.dumps(_plugin(), sort_keys=True),
        )
        if key not in self.cache:
            files: dict[str, str] = {}
            for transform, source in transforms:
                files.update(
                    render_generated_project(
                        transform,
                        source_transform=source,
                        generated_package=generated_package,
                        source_schema_modules=source_schema_modules,
                        generated_code_options=generated_code_options,
                        allow_stage_outputs=allow_stage_outputs,
                        artifacts=self.artifacts.pool("source preparation") if self.artifacts else None,
                    )
                )
            self.cache[key] = files
        return dict(self.cache[key])
