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
        lines = [
            "Structure check passed",
            f"  source roots: {', '.join(self._relative(config, root) for root in config.source_roots)}",
            f"  transforms: {len(project.transforms)}",
            f"  schemas: {len(project.schemas())}",
        ]
        lines.extend(self._compatibility(config))
        return tuple(lines)

    def _relative(self, config: StructureConfig, path: Path) -> str:
        try:
            return path.relative_to(config.project_root).as_posix()
        except ValueError:
            return path.as_posix()

    def _compatibility(self, config: StructureConfig) -> tuple[str, ...]:
        targets = tuple(target for target in config.compat_targets if target != "pyspark")
        if not targets:
            return ()
        return (
            f"  compatibility targets: {', '.join(targets)}",
            "  compatibility status: non-PySpark target checks are reserved for v2+; active PySpark checks passed",
        )
