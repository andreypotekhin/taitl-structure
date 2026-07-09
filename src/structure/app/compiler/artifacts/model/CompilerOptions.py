from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from structure.app.configuration.model.StructureConfig import StructureConfig


@dataclass(frozen=True)
class CompilerOptions:
    project_root: Path
    source_roots: tuple[Path, ...]
    generated_dir: Path
    generated_package: str
    target_backend: str
    target_profile: str
    target_variant: str
    validate_inputs: bool
    input_validation_mode: str
    validate_intermediate: bool
    intermediate_validation_mode: str
    validate_outputs: bool
    output_validation_mode: str
    strict_performance: bool
    warn_on_udfs: bool
    schema_types_key: str | None = None

    @classmethod
    def resolve(
        cls,
        options: CompilerOptions | StructureConfig | None = None,
        *,
        project_root: Path | str | None = None,
        config: StructureConfig | None = None,
        schema_types=None,
        overrides: Mapping[str, object] | None = None,
        **settings: object,
    ) -> CompilerOptions:
        if isinstance(options, CompilerOptions):
            return options
        if isinstance(options, StructureConfig):
            config = options
        if config is not None and (project_root is not None or overrides or settings):
            raise ValueError(
                "Pass either config/options, or pass project_root/config override fields, not both."
            )
        resolved = config or StructureConfig.resolve(project_root=project_root, overrides=overrides, **settings)
        return cls.from_config(resolved, schema_types=schema_types)

    @classmethod
    def from_config(cls, config: StructureConfig, *, schema_types=None) -> CompilerOptions:
        return cls(
            project_root=config.project_root,
            source_roots=config.source_roots,
            generated_dir=config.generated_dir,
            generated_package=config.generated_package,
            target_backend=config.target_backend,
            target_profile=config.target_profile,
            target_variant=config.target_variant,
            validate_inputs=config.validate_inputs,
            input_validation_mode=config.input_validation_mode,
            validate_intermediate=config.validate_intermediate,
            intermediate_validation_mode=config.intermediate_validation_mode,
            validate_outputs=config.validate_outputs,
            output_validation_mode=config.output_validation_mode,
            strict_performance=config.strict_performance,
            warn_on_udfs=config.warn_on_udfs,
            schema_types_key=cls._schema_types_key(schema_types),
        )

    def fingerprint(self) -> tuple[object, ...]:
        return (
            self.generated_package,
            self.target_backend,
            self.target_profile,
            self.target_variant,
            self.validate_inputs,
            self.input_validation_mode,
            self.validate_intermediate,
            self.intermediate_validation_mode,
            self.validate_outputs,
            self.output_validation_mode,
            self.strict_performance,
            self.warn_on_udfs,
            self.schema_types_key,
        )

    @staticmethod
    def _schema_types_key(schema_types) -> str | None:
        if schema_types is None:
            return None
        return f"{type(schema_types).__module__}.{type(schema_types).__qualname__}:{id(schema_types)}"
