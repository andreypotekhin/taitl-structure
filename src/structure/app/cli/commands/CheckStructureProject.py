from __future__ import annotations

from pathlib import Path

from structure.app.cli.commands.DiscoverStructureProject import DiscoverStructureProject
from structure.app.cli.commands.RenderExplainReport import RenderExplainReport
from structure.app.configuration.model.StructureConfig import StructureConfig


class CheckStructureProject:

    def __call__(self, config: StructureConfig) -> tuple[str, ...]:
        project = DiscoverStructureProject()(config)
        for transform in project.transforms:
            RenderExplainReport()(transform)
        return (
            "Structure check passed",
            f"  source roots: {', '.join(self._relative(config, root) for root in config.source_roots)}",
            f"  transforms: {len(project.transforms)}",
            f"  schemas: {len(project.schemas())}",
        )

    def _relative(self, config: StructureConfig, path: Path) -> str:
        try:
            return path.relative_to(config.project_root).as_posix()
        except ValueError:
            return path.as_posix()
