from __future__ import annotations

from collections.abc import Mapping
from threading import RLock

from structure.app.dsl.model.transforms.aliases import require_alias
from structure.app.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.app.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.app.dsl.model.transforms.OutputDeclaration import OutputDeclaration
from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline


class Transform:

    _structure_inputs: dict[str, InputDeclaration] = {}
    _structure_lanes: dict[str, LaneDeclaration] = {}
    _structure_outputs: dict[str, OutputDeclaration] = {}
    _structure_input_aliases: dict[str, str] = {}
    _structure_lane_aliases: dict[str, str] = {}
    _structure_output_aliases: dict[str, str] = {}
    _structure_pipeline: TransformPipeline | None = None
    _structure_transform = False
    _structure_transform_options: dict[str, object] = {}
    _structure_subtransform_options: dict[str, object] = {}
    _structure_compiled: dict[object, object] = {}
    _structure_compile_lock = RLock()

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
        cls._structure_pipeline = pipelines[0] if pipelines else None
        cls._structure_transform = False
        cls._structure_transform_options = {}
        cls._structure_subtransform_options = {}
        cls._structure_compiled = {}
        cls._structure_compile_lock = RLock()

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
        **settings: object,
    ):
        from structure.app.compiler.artifacts.commands import BuildCompiledTransform
        from structure.app.compiler.artifacts.model import CompilerOptions

        resolved = CompilerOptions.resolve(
            options,
            project_root=project_root,
            config=config,
            schema_types=schema_types,
            overrides=settings,
        )
        builder = BuildCompiledTransform()
        key = builder.key(cls, options=resolved)
        with cls._structure_compile_lock:
            if not force and key in cls._structure_compiled:
                return cls._structure_compiled[key]

        artifact = builder(cls, options=resolved, schema_types=schema_types)
        with cls._structure_compile_lock:
            if not force:
                existing = cls._structure_compiled.get(key)
                if existing is not None:
                    return existing
            cls._structure_compiled[key] = artifact
            return artifact

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
        from structure.app.cli.commands.DiscoverStructureProject import DiscoverStructureProject
        from structure.app.compiler.artifacts.model import CompilerOptions, GeneratedTransform
        from structure.app.configuration.model.StructureConfig import StructureConfig
        from structure.app.target.pyspark.api import PySpark
        from structure.app.target.pyspark.storage import DiskStorage

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
            target_backend=resolved.target_backend,
            target_profile=resolved.target_profile,
            target_variant=resolved.target_variant,
        )
        project = DiscoverStructureProject()(structure_config)
        files = PySpark.render.project()(
            artifact.pyspark_plan,
            source_transform=f"{cls.__module__}.{cls.__name__}",
            source_schema_modules=project.schema_modules,
            generated_package=resolved.generated_package,
        )
        target = storage or DiskStorage(resolved.generated_dir)
        target.write(files)
        return GeneratedTransform(
            generated_package=resolved.generated_package,
            files=files,
            storage=target,
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
                    raise TypeError(
                        f"{cls.__name__} {role} alias {alias} collides with a declared {role} field"
                    )
                existing = aliases.get(alias)
                if existing is not None:
                    raise TypeError(
                        f"{cls.__name__} {role} alias {alias} is used by both {existing} and {name}"
                    )
                aliases[alias] = name
        return aliases

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
