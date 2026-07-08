from __future__ import annotations

from collections import defaultdict

from structure.app.cli.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.app.compiler.api import Compiler
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.target.capabilities.api import Capabilities
from structure.app.target.pyspark.api import PySpark


class RenderConfiguredPySparkProject:

    def __call__(
        self,
        config: StructureConfig,
        project: DiscoveredStructureProject,
        *,
        transforms: tuple[type[Transform], ...] | None = None,
    ) -> dict[str, str]:
        files: dict[str, str] = {}
        capabilities = Capabilities.resolve()(
            target_backend=config.target_backend,
            target_profile=config.target_profile,
            target_variant=config.target_variant,
        )
        for source_module, group in self._source_units(transforms or project.transforms).items():
            plans = {}
            for transform in group:
                source_transform = f"{transform.__module__}.{transform.__name__}"
                plans[source_transform] = PySpark.plan.lower()(
                    Compiler.frontend.compile()(transform),
                    capabilities=capabilities,
                )
            files.update(
                PySpark.render.project().source_unit(
                    plans,
                    source_module=source_module,
                    source_schema_modules=project.schema_modules,
                    generated_package=config.generated_package,
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
