from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class StructureConfig:
    project_root: Path
    source_roots: tuple[Path, ...]
    generated_dir: Path
    generated_package: str
    generated_docs: bool
    generated_docs_dir: Path
    generated_docs_formats: tuple[str, ...]
    generated_code_options: tuple[str, ...]
    generated_code_hard_wrap: int
    execution_mode: str
    target: str
    hook_target_default: tuple[str, ...] | str
    traceability: str
    validate_inputs: bool
    input_validation_mode: str
    validate_intermediate: bool
    intermediate_validation_mode: str
    validate_outputs: bool
    output_validation_mode: str
    strict_performance: bool
    warn_on_udfs: bool
    warn_on_lineage_growth: bool
    allow_stream_to_batch: bool
    stream_to_batch_policy: str
    allow_output_to_input: bool
    allow_to_reassign_output: bool
    allow_stage_outputs: bool
    fail_on_diff: bool
    spark_sql: Mapping[str, object]
    plugin_options: Mapping[str, Mapping[str, object]]
    source_map: Mapping[str, str]

    @classmethod
    def resolve(
        cls,
        *,
        project_root: Path | str | None = None,
        overrides: Mapping[str, object] | None = None,
        **settings: object,
    ) -> StructureConfig:
        merged = dict(overrides or {})
        duplicates = set(merged).intersection(settings)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(f"Configuration override supplied twice: {names}.")
        merged.update(settings)

        from structure.core.configuration.commands.ResolveStructureConfig import ResolveStructureConfig

        return ResolveStructureConfig()(project_root=project_root, overrides=merged, override_source="programmatic")

    @classmethod
    def create(cls, **settings: object) -> "StructureConfig":
        from structure.core.configuration.commands.ResolveStructureConfig import ResolveStructureConfig
        from structure.core.configuration.logic.StructureConfigBuilder import StructureConfigBuilder
        from structure.core.configuration.logic.StructureConfigDefaults import StructureConfigDefaults
        from structure.core.configuration.logic.StructureConfigMerger import StructureConfigMerger
        from structure.core.configuration.logic.StructureConfigValidator import StructureConfigValidator

        root = Path.cwd()
        defaults = StructureConfigDefaults()
        values, sources = defaults.programmatic()
        resolver = ResolveStructureConfig()
        StructureConfigMerger(resolver._keys).merge(values, sources, settings, "programmatic")
        StructureConfigValidator().validate(values, root, allow_empty_source_roots=True)
        return StructureConfigBuilder().build(root, values, sources)
