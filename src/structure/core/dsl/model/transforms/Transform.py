from __future__ import annotations

import inspect
from collections.abc import Mapping

from structure.core.dsl.model.transforms.aliases import require_alias
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.core.dsl.model.transforms.StageDeclaration import StageDeclaration
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline


class Transform:

    _structure_inputs: dict[str, InputDeclaration] = {}
    _structure_lanes: dict[str, LaneDeclaration] = {}
    _structure_outputs: dict[str, OutputDeclaration] = {}
    _structure_input_aliases: dict[str, str] = {}
    _structure_lane_aliases: dict[str, str] = {}
    _structure_output_aliases: dict[str, str] = {}
    _structure_pipeline: TransformPipeline | None = None
    _structure_stages: dict[str, StageDeclaration] = {}
    _structure_transform = False
    _structure_transform_options: dict[str, object] = {}
    _structure_step_method_options: dict[str, object] = {}

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        inputs: dict[str, InputDeclaration] = {}
        lanes: dict[str, LaneDeclaration] = {}
        outputs: dict[str, OutputDeclaration] = {}
        for base in cls.__bases__:
            inputs.update(getattr(base, "_structure_inputs", {}))
            lanes.update(getattr(base, "_structure_lanes", {}))
            outputs.update(getattr(base, "_structure_outputs", {}))

        for value in cls.__dict__.values():
            if isinstance(value, InputDeclaration):
                inputs[value.name] = value
            if isinstance(value, LaneDeclaration):
                lanes[value.name] = value
            if isinstance(value, OutputDeclaration):
                outputs[value.name] = value

        cls._structure_inputs = inputs
        cls._structure_lanes = lanes
        cls._structure_outputs = outputs
        cls._structure_input_aliases = cls._alias_index("input", inputs)
        cls._structure_lane_aliases = cls._alias_index("lane", lanes)
        cls._structure_output_aliases = cls._alias_index("output", outputs)
        pipelines = [value for value in cls.__dict__.values() if isinstance(value, TransformPipeline)]
        if len(pipelines) > 1:
            raise TypeError(f"{cls.__name__} declares more than one transform pipeline field")
        stages = {value.name: value for value in cls.__dict__.values() if isinstance(value, StageDeclaration)}
        if pipelines and stages:
            raise TypeError(f"{cls.__name__} cannot combine Transform.to(...) pipeline and stage(...) composition")
        cls._structure_pipeline = pipelines[0] if pipelines else None
        cls._structure_stages = stages
        cls._structure_transform = False
        cls._structure_transform_options = {}
        cls._structure_step_method_options = {}

    def __init__(self, **inputs: object) -> None:
        normalized: dict[str, object] = {}
        unknown = []
        for name, value in inputs.items():
            canonical = self._structure_input_aliases.get(name, name)
            if canonical not in self._structure_inputs:
                unknown.append(name)
                continue
            if canonical in normalized:
                raise TypeError(
                    f"{type(self).__name__} got input {canonical} more than once. "
                    "Pass either the canonical input name or one alias."
                )
            normalized[canonical] = value
        if unknown:
            allowed = ", ".join((*self._structure_inputs, *self._structure_input_aliases))
            raise TypeError(
                f"{type(self).__name__} got unknown input(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_bound_inputs = normalized
        self._structure_output_renames: dict[str, str] = {}

    def run(self, session):
        return session.run(self)

    @classmethod
    def compile(
        cls,
        options=None,
        *,
        project_root=None,
        config=None,
        schema_types=None,
        force: bool = False,
        plugin_configuration=None,
        plugin_registry=None,
        target: str | None = None,
        **settings: object,
    ):
        from structure.core.compiler.api.Compiler import Compiler
        from structure.core.compiler.artifacts.model import CompilerOptions
        from structure.core.sources.model.StructureSources import StructureSources

        if plugin_configuration is not None or plugin_registry is not None:
            if plugin_configuration is None or plugin_registry is None:
                raise ValueError("plugin_configuration and plugin_registry must be supplied together.")
            return Compiler.artifacts.plugin(plugin_registry)(cls, configuration=plugin_configuration, target=target)
        resolved = CompilerOptions.resolve(
            options,
            project_root=project_root,
            config=config,
            schema_types=schema_types,
            overrides=settings,
        )
        if cls is Transform and isinstance(options, StructureSources):
            return Compiler.artifacts.sources()(
                options,
                compile_one=lambda subject: Compiler.artifacts.build()(
                    subject, options=resolved, schema_types=schema_types
                ),
            )
        return Compiler.artifacts.build()(cls, options=resolved, schema_types=schema_types)

    @classmethod
    def generate(
        cls,
        options=None,
        *,
        project_root=None,
        config=None,
        storage=None,
        schema_types=None,
        force: bool = False,
        **settings: object,
    ):
        from structure.core.cli.api import CliApp
        from structure.core.compiler.artifacts.model import CompilerOptions, GeneratedTransform
        from structure.core.compiler.artifacts.storage import DiskStorage
        from structure.core.configuration.model.StructureConfig import StructureConfig
        from structure.core.plugins.api.Plugin import Plugin
        from structure.plugin.api.v1.model import GenerationRequest

        resolved = CompilerOptions.resolve(
            options,
            project_root=project_root,
            config=config,
            schema_types=schema_types,
            overrides=settings,
        )
        artifact = cls.compile(resolved, schema_types=schema_types, force=force)
        structure_config = config or StructureConfig.resolve(
            project_root=resolved.project_root,
            generated_package=resolved.generated_package,
            generated_code_options=resolved.generated_code_options,
            generated_code_hard_wrap=resolved.generated_code_hard_wrap,
            plugin={
                "default": resolved.target,
                resolved.target: dict(resolved.selected_plugin_options()),
            },
        )
        project = CliApp.discover_project()(structure_config)
        source_unit = cls.__module__
        transforms = cls._source_unit_transforms(project.transforms)
        plans = {}
        fingerprints = {}
        for transform in transforms:
            transform_artifact = (
                artifact if transform is cls else transform.compile(resolved, schema_types=schema_types, force=force)
            )
            plans[f"{transform.__module__}.{transform.__name__}"] = transform_artifact.payload
            fingerprints[f"{transform.__module__}.{transform.__name__}"] = transform_artifact.semantic_fingerprint
        plugin = Plugin.registry().select(resolved.target)
        if plugin.api.generator is None:
            raise ValueError(f"PLUGIN-E2709: Plugin {resolved.target!r} does not provide generation.")
        generated = plugin.api.generator.generate(
            GenerationRequest(
                payload=plans,
                source_module=source_unit,
                source_schema_modules=project.schema_modules,
                generated_package=resolved.generated_package,
                semantic_fingerprints=fingerprints,
                generated_code_options=resolved.generated_code_options,
                generated_code_hard_wrap=resolved.generated_code_hard_wrap,
            )
        )
        target = storage or DiskStorage(resolved.generated_dir)
        result = target.write(generated.files)
        return GeneratedTransform(
            source_unit=source_unit,
            module_name=generated.module_name,
            classes=generated.classes,
            generated_package=resolved.generated_package,
            files=generated.files,
            storage=target,
            result=result,
        )

    def to(self, *stages: "Transform") -> TransformPipeline:
        return TransformPipeline((self, *stages))

    def rename(self, **outputs: str) -> "Transform":
        unknown = set(outputs) - set(self._structure_outputs)
        if unknown:
            allowed = ", ".join(self._structure_outputs)
            raise TypeError(
                f"{type(self).__name__}.rename(...) got unknown output(s): {', '.join(sorted(unknown))}. "
                f"Allowed: {allowed}"
            )
        renames = dict(self._structure_output_renames)
        for name, target in outputs.items():
            alias = require_alias(target)
            if alias == name:
                raise TypeError(f"{type(self).__name__}.rename(...) cannot rename {name} to itself")
            renames[name] = alias
        self._validate_rename_aliases(renames)
        self._structure_output_renames = renames
        return self

    @classmethod
    def _alias_index(cls, role: str, declarations: Mapping[str, object]) -> dict[str, str]:
        aliases: dict[str, str] = {}
        for name, declaration in declarations.items():
            for alias in getattr(declaration, "aliases", ()):
                if alias == name:
                    raise TypeError(f"{cls.__name__} {role} {name} aliases itself")
                if alias in declarations:
                    raise TypeError(f"{cls.__name__} {role} alias {alias} collides with a declared {role} field")
                existing = aliases.get(alias)
                if existing is not None:
                    raise TypeError(f"{cls.__name__} {role} alias {alias} is used by both {existing} and {name}")
                aliases[alias] = name
        return aliases

    @classmethod
    def _source_unit_transforms(cls, discovered: tuple[type["Transform"], ...]) -> tuple[type["Transform"], ...]:
        in_project = tuple(transform for transform in discovered if transform.__module__ == cls.__module__)
        if in_project:
            return in_project

        module = inspect.getmodule(cls)
        if module is None:
            return (cls,)

        transforms = []
        for value in module.__dict__.values():
            if (
                isinstance(value, type)
                and issubclass(value, Transform)
                and value is not Transform
                and value.__module__ == cls.__module__
                and (value._structure_outputs or value._structure_pipeline is not None)
                and not inspect.isabstract(value)
            ):
                transforms.append(value)
        return tuple(dict.fromkeys(transforms)) or (cls,)

    def _validate_rename_aliases(self, renames: Mapping[str, str]) -> None:
        aliases: dict[str, str] = {}
        for name, alias in renames.items():
            if alias in self._structure_outputs:
                raise TypeError(
                    f"{type(self).__name__}.rename(...) alias {alias} collides with a declared output field"
                )
            if alias in self._structure_output_aliases:
                raise TypeError(
                    f"{type(self).__name__}.rename(...) alias {alias} collides with a declared output alias"
                )
            existing = aliases.get(alias)
            if existing is not None:
                raise TypeError(
                    f"{type(self).__name__}.rename(...) alias {alias} is used by both {existing} and {name}"
                )
            aliases[alias] = name
