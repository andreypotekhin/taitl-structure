from __future__ import annotations

import importlib
import sys

from structure.app.cli.commands.RenderExplainReport import RenderExplainReport
from structure.app.configuration.model.StructureConfig import StructureConfig


class ExplainStructureTransform:

    def __call__(self, config: StructureConfig, transform: str) -> tuple[str, ...]:
        for root in config.source_roots:
            text = str(root)
            if text not in sys.path:
                sys.path.insert(0, text)
        module_name, name = transform.rsplit(".", 1)
        module = importlib.import_module(module_name)
        return (RenderExplainReport()(getattr(module, name)),)
