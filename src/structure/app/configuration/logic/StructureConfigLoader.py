from collections.abc import Mapping
from pathlib import Path

import toml  # type: ignore[import-untyped]


class StructureConfigLoader:

    def structure_toml(self, root: Path) -> dict[str, object]:
        path = root / "structure.toml"
        if not path.exists():
            return {}
        data = toml.load(path)
        return self._section(data)

    def pyproject(self, root: Path) -> dict[str, object]:
        path = root / "pyproject.toml"
        if not path.exists():
            return {}
        data = toml.load(path)
        tool = data.get("tool", {})
        if not isinstance(tool, dict):
            return {}
        structure = tool.get("structure", {})
        return self._flatten(structure) if isinstance(structure, dict) else {}

    def _section(self, data: Mapping[str, object]) -> dict[str, object]:
        tool = data.get("tool")
        if isinstance(tool, dict) and isinstance(tool.get("structure"), dict):
            return self._flatten(tool["structure"])
        return self._flatten(data)

    def _flatten(self, data: Mapping[str, object], prefix: str = "") -> dict[str, object]:
        flat: dict[str, object] = {}
        for key, value in data.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(value, dict):
                flat.update(self._flatten(value, name))
            else:
                flat[name] = value
        return flat
