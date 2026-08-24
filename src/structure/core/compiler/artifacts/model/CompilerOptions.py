from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from structure.core.configuration.model.StructureConfig import StructureConfig


@dataclass(frozen=True)
class CompilerOptions:
    project_root: Path
    source_roots: tuple[Path, ...]
    generated_dir: Path
    generated_package: str
    generated_code_options: tuple[str, ...]
    generated_code_hard_wrap: int
    target: str
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
    schema_types_key: str | None = None
    plugin_options: Mapping[str, Mapping[str, object]] = field(default_factory=lambda: MappingProxyType({}))

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
            raise ValueError("Pass either config/options, or pass project_root/config override fields, not both.")
        resolved = config or StructureConfig.resolve(project_root=project_root, overrides=overrides, **settings)
        return cls.from_config(resolved, schema_types=schema_types)

    @classmethod
    def from_config(cls, config: StructureConfig, *, schema_types=None) -> CompilerOptions:
        return cls(
            project_root=config.project_root,
            source_roots=config.source_roots,
            generated_dir=config.generated_dir,
            generated_package=config.generated_package,
            generated_code_options=config.generated_code_options,
            generated_code_hard_wrap=config.generated_code_hard_wrap,
            target=config.target,
            traceability=config.traceability,
            validate_inputs=config.validate_inputs,
            input_validation_mode=config.input_validation_mode,
            validate_intermediate=config.validate_intermediate,
            intermediate_validation_mode=config.intermediate_validation_mode,
            validate_outputs=config.validate_outputs,
            output_validation_mode=config.output_validation_mode,
            strict_performance=config.strict_performance,
            warn_on_udfs=config.warn_on_udfs,
            warn_on_lineage_growth=config.warn_on_lineage_growth,
            allow_stream_to_batch=config.allow_stream_to_batch,
            stream_to_batch_policy=config.stream_to_batch_policy,
            allow_output_to_input=config.allow_output_to_input,
            allow_to_reassign_output=config.allow_to_reassign_output,
            plugin_options=config.plugin_options,
            schema_types_key=cls._schema_types_key(schema_types),
        )

    def fingerprint(self) -> tuple[object, ...]:
        return (
            self.generated_package,
            self.generated_code_options,
            self.generated_code_hard_wrap,
            self.target,
            self.traceability,
            self.validate_inputs,
            self.input_validation_mode,
            self.validate_intermediate,
            self.intermediate_validation_mode,
            self.validate_outputs,
            self.output_validation_mode,
            self.strict_performance,
            self.warn_on_udfs,
            self.warn_on_lineage_growth,
            self.allow_stream_to_batch,
            self.stream_to_batch_policy,
            self.allow_output_to_input,
            self.allow_to_reassign_output,
            self._plugin_options_key(),
            self.schema_types_key,
        )

    def selected_plugin_options(self) -> Mapping[str, object]:
        return self.plugin_options.get(self.target, {})

    def _plugin_options_key(self) -> tuple[tuple[str, object], ...]:
        return tuple(sorted((name, self._freeze(value)) for name, value in self.selected_plugin_options().items()))

    @classmethod
    def _freeze(cls, value: object) -> object:
        if isinstance(value, Mapping):
            return tuple(sorted((str(name), cls._freeze(item)) for name, item in value.items()))
        if isinstance(value, (list, tuple)):
            return tuple(cls._freeze(item) for item in value)
        try:
            hash(value)
        except TypeError:
            return (type(value).__qualname__, id(value))
        return value

    @staticmethod
    def _schema_types_key(schema_types) -> str | None:
        if schema_types is None:
            return None
        return f"{type(schema_types).__module__}.{type(schema_types).__qualname__}:{id(schema_types)}"
