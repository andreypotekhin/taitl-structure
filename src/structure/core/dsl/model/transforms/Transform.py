"""Base class for Structure transforms."""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Mapping

from structure.core.dsl.model.transforms.aliases import require_alias
from structure.core.dsl.model.transforms.InputDeclaration import InputDeclaration
from structure.core.dsl.model.transforms.LaneDeclaration import LaneDeclaration
from structure.core.dsl.model.transforms.OutputDeclaration import OutputBindings, OutputDeclaration
from structure.core.dsl.model.transforms.ParameterDeclaration import ParameterDeclaration
from structure.core.dsl.model.transforms.StageDeclaration import (
    StageDeclaration,
    StageOutputReference,
    _output_reference,
)
from structure.core.dsl.model.transforms.TransformPipeline import TransformPipeline


class Transform:
    """A declarative transform made of schema-bound step methods.

    Subclasses declare external inputs, intermediate lanes, outputs, and
    ``@step`` methods.  Instances bind runtime input values; class methods
    compile or generate target-specific code.

    Example:
        @transform
        class PublishOrders(Transform):
            orders = input(Order)
            published = output(PublishedOrder)

            @step(input=orders, output=published)
            def publish(self, order):
                return PublishedOrder.project(order)

        artifact = PublishOrders.compile(target="pyspark")
    """

    _structure_inputs: dict[str, InputDeclaration] = {}
    _structure_lanes: dict[str, LaneDeclaration] = {}
    _structure_outputs: dict[str, OutputDeclaration] = {}
    _structure_parameters: dict[str, ParameterDeclaration] = {}
    _structure_input_aliases: dict[str, str] = {}
    _structure_lane_aliases: dict[str, str] = {}
    _structure_output_aliases: dict[str, str] = {}
    _structure_output_bindings: dict[str, object] = {}
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
        output_bindings: dict[str, object] = {}
        parameters: dict[str, ParameterDeclaration] = {}
        for base in cls.__bases__:
            inputs.update(getattr(base, "_structure_inputs", {}))
            lanes.update(getattr(base, "_structure_lanes", {}))
            outputs.update(getattr(base, "_structure_outputs", {}))
            output_bindings.update(getattr(base, "_structure_output_bindings", {}))
            parameters.update(getattr(base, "_structure_parameters", {}))

        for name, value in cls.__dict__.items():
            if isinstance(value, InputDeclaration):
                inputs[value.name] = value
            if isinstance(value, LaneDeclaration):
                lanes[value.name] = value
            if isinstance(value, OutputDeclaration):
                outputs[value.name] = value
                output_bindings.pop(value.name, None)
            if isinstance(value, ParameterDeclaration):
                parameters[value.name] = value
            elif name in parameters:
                del parameters[name]

        cls._structure_inputs = inputs
        cls._structure_lanes = lanes
        cls._structure_outputs = outputs
        binding_blocks = [value for value in cls.__dict__.values() if isinstance(value, OutputBindings)]
        if len(binding_blocks) > 1:
            raise TypeError(f"{cls.__name__} declares more than one output binding block")
        if binding_blocks:
            for name, source in binding_blocks[0].bindings:
                declaration = outputs.get(name)
                if declaration is None:
                    allowed = ", ".join(outputs) or "none"
                    raise TypeError(
                        f"{cls.__name__} output binding {name!r} is not declared. Available outputs: {allowed}"
                    )
                if declaration.source is not None:
                    raise TypeError(f"{cls.__name__} output {name!r} has both an inline source and an output binding")
                output_bindings[name] = source
        cls._structure_output_bindings = output_bindings
        cls._structure_parameters = parameters
        cls._structure_input_aliases = cls._alias_index("input", inputs)
        cls._structure_lane_aliases = cls._alias_index("lane", lanes)
        cls._structure_output_aliases = cls._alias_index("output", outputs)
        pipelines = [value for value in cls.__dict__.values() if isinstance(value, TransformPipeline)]
        if len(pipelines) > 1:
            raise TypeError(f"{cls.__name__} declares more than one transform pipeline field")
        stages: dict[str, StageDeclaration] = {}
        for base in cls.__bases__:
            stages.update(getattr(base, "_structure_stages", {}))
        for name, value in cls.__dict__.items():
            if isinstance(value, StageDeclaration):
                stages[value.name] = value
            elif isinstance(value, Transform):
                implicit_stage = value._implicit_stage_declaration()
                implicit_stage.__set_name__(cls, name)
                stages[name] = implicit_stage
        if pipelines and stages:
            raise TypeError(f"{cls.__name__} cannot combine Transform.to(...) pipeline and stage(...) composition")
        cls._structure_pipeline = pipelines[0] if pipelines else None
        cls._structure_stages = stages
        cls._structure_transform = False
        cls._structure_transform_options = {}
        cls._structure_step_method_options = {}

    def __init__(self, **inputs: object) -> None:
        normalized: dict[str, object] = {}
        parameters: dict[str, object] = {}
        unknown = []
        for name, value in inputs.items():
            canonical = self._structure_input_aliases.get(name, name)
            if canonical in self._structure_inputs:
                if canonical in normalized:
                    raise TypeError(
                        f"{type(self).__name__} got input {canonical} more than once. "
                        "Pass either the canonical input name or one alias."
                    )
                normalized[canonical] = value
                continue
            if name in self._structure_parameters:
                parameters[name] = value
                continue
            if canonical not in self._structure_inputs:
                unknown.append(name)
                continue
        if unknown:
            allowed = ", ".join((*self._structure_inputs, *self._structure_input_aliases, *self._structure_parameters))
            raise TypeError(
                f"{type(self).__name__} got unknown input(s): {', '.join(sorted(unknown))}. Allowed: {allowed}"
            )
        self._structure_bound_inputs = normalized
        self._structure_bound_parameters = parameters
        self._structure_output_renames: dict[str, str] = {}
        self._structure_implicit_stage: StageDeclaration | None = None

    def __getattribute__(self, name: str) -> object:
        """Expose declared outputs as references when an invocation is used as a stage."""
        if not name.startswith("_"):
            cls = object.__getattribute__(self, "__class__")
            outputs = getattr(cls, "_structure_outputs", {})
            if name in outputs:
                stage = object.__getattribute__(self, "_implicit_stage_declaration")()
                return _output_reference(stage, name)
        return object.__getattribute__(self, name)

    def __getattr__(self, name: str) -> StageOutputReference:
        """Return a typed output reference for an implicitly declared stage."""
        if name.startswith("_"):
            raise AttributeError(name)
        outputs = getattr(type(self), "_structure_outputs", {})
        if name not in outputs:
            transform = type(self).__name__
            allowed = ", ".join(outputs) or "none"
            raise AttributeError(f"{transform} has no output {name!r}. Available outputs: {allowed}")
        return _output_reference(self._implicit_stage_declaration(), name)

    def _implicit_stage_declaration(self) -> StageDeclaration:
        stage = self._structure_implicit_stage
        if stage is None:
            stage = StageDeclaration(invocation=self)
            self._structure_implicit_stage = stage
        return stage

    def run(self, session):
        """Run this transform invocation through a Structure session."""
        return session.run(self)

    @classmethod
    def effective_transform_options(cls) -> dict[str, object]:
        """Resolve explicit class options without inferring transform streaming."""
        return cls.resolve_transform_options(
            cls.__dict__.get("_structure_transform_options", {}),
            inputs=cls._structure_inputs.values(),
            transform_name=cls.__name__,
        )

    @staticmethod
    def resolve_transform_options(
        options: Mapping[str, object] | None = None,
        *,
        inputs: Iterable[object] = (),
        transform_name: str,
    ) -> dict[str, object]:
        """Resolve effective transform options from declared options and inputs."""
        resolved = dict(options or {})
        streaming = resolved.get("streaming")
        if streaming is False and any(bool(getattr(input, "streaming", False)) for input in inputs):
            raise TypeError(
                f"{transform_name} declares streaming input(s) but @transform(streaming=False). "
                "Remove the class option or declare all inputs batch-only."
            )
        return resolved

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
        """Compile this transform class or source tree with a target plugin.

        Args:
            options: Optional compiler options or source tree.
            project_root: Optional project root used for configuration lookup.
            config: Explicit Structure configuration object.
            schema_types: Optional schema type registry override.
            force: Rebuild even when cached artifacts may exist.
            plugin_configuration: Advanced plugin configuration override.
            plugin_registry: Advanced plugin registry override.
            target: Optional target name, such as ``"pyspark"``.
            **settings: Compiler option overrides.

        Returns:
            A compiler artifact for this transform, or a source-tree artifact
            when called as ``Transform.compile(sources)``.
        """
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
        """Generate target code for every transform in the source module.

        Args:
            options: Optional compiler options.
            project_root: Optional project root used for configuration lookup.
            config: Explicit Structure configuration object.
            storage: Optional generated-file storage implementation.
            schema_types: Optional schema type registry override.
            force: Rebuild even when cached artifacts may exist.
            **settings: Compiler option overrides.

        Returns:
            A generated transform artifact containing files and write results.

        Example:
            generated = PublishOrders.generate(target="pyspark")
            print(generated.files)
        """
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
                traceability=resolved.traceability,
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
        """Compose this invocation with later transform invocations."""
        return TransformPipeline((self, *stages))

    def rename(self, **outputs: str) -> "Transform":
        """Rename declared outputs on this invocation.

        Args:
            **outputs: Mapping from declared output name to public alias.

        Returns:
            This transform invocation, updated with output aliases.

        Example:
            pipeline = BuildCustomerFacts().rename(facts="customer_facts")
        """
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
