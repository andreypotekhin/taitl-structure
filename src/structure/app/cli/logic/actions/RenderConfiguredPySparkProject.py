from __future__ import annotations

from structure.app.target.pyspark.api import lower_pyspark_plan, render_pyspark_project
from structure.app.cli.logic.model.DiscoveredStructureProject import DiscoveredStructureProject
from structure.app.configuration.logic.model.StructureConfig import StructureConfig
from structure.app.dsl.api import compile_transform
from structure.app.dsl.logic.model.transforms.Transform import Transform


class RenderConfiguredPySparkProject:

    def __call__(
        self,
        config: StructureConfig,
        project: DiscoveredStructureProject,
        *,
        transforms: tuple[type[Transform], ...] | None = None,
    ) -> dict[str, str]:
        files: dict[str, str] = {}
        for transform in transforms or project.transforms:
            files.update(
                render_pyspark_project(
                    lower_pyspark_plan(compile_transform(transform)),
                    source_transform=f"{transform.__module__}.{transform.__name__}",
                    source_schema_modules=project.schema_modules,
                    generated_package=config.generated_package,
                )
            )
        return files


render_configured_pyspark_project = RenderConfiguredPySparkProject()
