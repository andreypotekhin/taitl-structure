from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from structure.app.configuration.logic.StructureConfigBuilder import StructureConfigBuilder
from structure.app.configuration.logic.StructureConfigDefaults import StructureConfigDefaults
from structure.app.configuration.logic.StructureConfigLoader import StructureConfigLoader
from structure.app.configuration.logic.StructureConfigMerger import StructureConfigMerger
from structure.app.configuration.logic.StructureConfigValidator import StructureConfigValidator
from structure.app.configuration.model.StructureConfig import StructureConfig
from structure.app.target.capabilities.api import Capabilities


class ResolveStructureConfig:

    _keys = {
        "source_roots",
        "generated_dir",
        "generated_package",
        "execution_mode",
        "target_backend",
        "target_pyspark",
        "target_profile",
        "compat_targets",
        "hook_target_default",
        "traceability",
        "validate_inputs",
        "input_validation_mode",
        "validate_intermediate",
        "intermediate_validation_mode",
        "validate_outputs",
        "output_validation_mode",
        "strict_performance",
        "fail_on_diff",
        "spark.sql.ansi.enabled",
        "spark.sql.storeAssignmentPolicy",
    }

    def __init__(self) -> None:
        self._builder = StructureConfigBuilder()
        self._defaults = StructureConfigDefaults()
        self._loader = StructureConfigLoader()
        self._merger = StructureConfigMerger(self._keys)
        self._validator = StructureConfigValidator()

    def __call__(
        self,
        *,
        project_root: Path | str | None = None,
        overrides: Mapping[str, object] | None = None,
    ) -> StructureConfig:
        root = Path(project_root or Path.cwd()).resolve()
        values, sources = self._defaults.resolve(root)
        self._merger.merge(values, sources, self._loader.structure_toml(root), "structure.toml")
        self._merger.merge(values, sources, self._loader.pyproject(root), "pyproject.toml [tool.structure]")
        self._merger.merge(
            values, sources, {key: value for key, value in (overrides or {}).items() if value is not None}, "CLI"
        )
        self._validator.validate(values, root)
        Capabilities.resolve()(
            target_backend=str(values["target_backend"]),
            target_pyspark=str(values["target_pyspark"]),
        )
        return self._builder.build(root, values, sources)


resolve_structure_config = ResolveStructureConfig()
