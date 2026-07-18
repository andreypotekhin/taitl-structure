from __future__ import annotations

from collections import defaultdict

from structure.core.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.core.compiler.api import Compiler, CompilerOptions
from structure.core.configuration.model.StructureConfig import StructureConfig
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.target.pyspark.api import PySpark


class RenderConfiguredPySparkProject:

    def __call__(
        self,
        config: StructureConfig,
        project: DiscoveredStructureProject,
        *,
        transforms: tuple[type[Transform], ...] | None = None,
    ) -> dict[str, str]:
        files: dict[str, str] = {}
        options = CompilerOptions.from_config(config)
        builder = Compiler.artifacts.build()
        for source_module, group in self._source_units(transforms or project.transforms).items():
            plans = {}
            fingerprints = {}
            for transform in group:
                source_transform = f"{transform.__module__}.{transform.__name__}"
                artifact = builder(transform, options=options, materialize_schemas=False)
                plans[source_transform] = artifact.pyspark_plan
                fingerprints[source_transform] = artifact.semantic_fingerprint
            files.update(
                PySpark.render.project().source_unit(
                    plans,
                    source_module=source_module,
                    source_schema_modules=project.schema_modules,
                    generated_package=config.generated_package,
                    semantic_fingerprints=fingerprints,
                    generated_code_options=config.generated_code_options,
                )
            )
        return files

    def _source_units(
        self,
        transforms: tuple[type[Transform], ...],
    ) -> dict[str, tuple[type[Transform], ...]]:
        grouped: dict[str, list[type[Transform]]] = defaultdict(list)
        for transform in transforms:
            grouped[transform.__module__].append(transform)
        return {module: tuple(items) for module, items in grouped.items()}


render_configured_pyspark_project = RenderConfiguredPySparkProject()
