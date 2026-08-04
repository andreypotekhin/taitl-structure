from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence

from structure.dsl import Schema
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.GeneratedPySparkTransformModule import generated_pyspark_transform_module
from structure.plugin.pyspark.render.commands.RenderPySparkRuntimeModule import render_pyspark_runtime_module
from structure.plugin.pyspark.render.commands.RenderPySparkTransformModule import render_pyspark_transform_module
from structure.plugin.pyspark.render.logic.PySparkTraceabilityReport import PySparkTraceabilityReport


class RenderPySparkProject:

    def __init__(self) -> None:
        self._traceability = PySparkTraceabilityReport()

    @property
    def _schema(self):
        from structure.plugin.pyspark.api.PySpark import PySpark

        return PySpark.schema

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
        generated_package: str,
        semantic_fingerprint: str | None = None,
        generated_code_options: tuple[str, ...] = (),
        generated_code_hard_wrap: int = 120,
        traceability: str = "none",
    ) -> dict[str, str]:
        schema_source_modules = self._schema_source_modules(
            source_schema_modules, generated_package=generated_package
        )
        schema_modules = self._schema_modules(schema_source_modules, source_schema_modules)
        runtime_module = f"{generated_package}.runtime.schema_assert"
        transform_module = self._transform_module(source_transform, generated_package=generated_package)

        files: OrderedDict[str, str] = OrderedDict()
        for package in self._packages(generated_package, transform_module=transform_module):
            files[self._module_path(package, package=True)] = self._package_header("Structure generated package")

        files[self._module_path(runtime_module)] = (
            self._header("Structure generated runtime") + render_pyspark_runtime_module()
        )

        for source_module in sorted(source_schema_modules):
            module = schema_source_modules[source_module]
            schemas = source_schema_modules[source_module]
            files[self._module_path(module)] = self._header(source_module) + self._schema.module()(
                schemas,
                dependency_modules=schema_modules,
            )

        files[self._module_path(transform_module)] = self._header(
            self._transform_source(source_transform, generated_code_options)
        ) + render_pyspark_transform_module(
            plan,
            source_transform=source_transform,
            schema_modules=schema_modules,
            runtime_module=runtime_module,
            semantic_fingerprint=semantic_fingerprint,
            generated_code_options=generated_code_options,
            generated_code_hard_wrap=generated_code_hard_wrap,
        )

        if traceability != "none":
            files[self._traceability_path(generated_package, source_transform, plan)] = self._traceability.render(
                plan,
                source_transform=source_transform,
                transform_module=transform_module,
                schema_modules=schema_modules,
            )
        return dict(files)

    def source_unit(
        self,
        plans: Mapping[str, PySparkExecutionPlan],
        *,
        source_module: str,
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
        generated_package: str,
        semantic_fingerprints: Mapping[str, str] | None = None,
        generated_code_options: tuple[str, ...] = (),
        generated_code_hard_wrap: int = 120,
        traceability: str = "none",
    ) -> dict[str, str]:
        schema_source_modules = self._schema_source_modules(
            source_schema_modules, generated_package=generated_package
        )
        schema_modules = self._schema_modules(schema_source_modules, source_schema_modules)
        runtime_module = f"{generated_package}.runtime.schema_assert"
        transform_module = self._source_transform_module(source_module, generated_package=generated_package)

        files: OrderedDict[str, str] = OrderedDict()
        for package in self._packages(generated_package, transform_module=transform_module):
            files[self._module_path(package, package=True)] = self._package_header("Structure generated package")

        files[self._module_path(runtime_module)] = (
            self._header("Structure generated runtime") + render_pyspark_runtime_module()
        )

        for schema_source_module in sorted(source_schema_modules):
            module = schema_source_modules[schema_source_module]
            schemas = source_schema_modules[schema_source_module]
            files[self._module_path(module)] = self._header(schema_source_module) + self._schema.module()(
                schemas,
                dependency_modules=schema_modules,
            )

        files[self._module_path(transform_module)] = self._header(
            self._transform_source(source_module, generated_code_options)
        ) + render_pyspark_transform_module.source_unit(
            plans,
            schema_modules=schema_modules,
            runtime_module=runtime_module,
            semantic_fingerprints=semantic_fingerprints,
            generated_code_options=generated_code_options,
            generated_code_hard_wrap=generated_code_hard_wrap,
        )

        if traceability != "none":
            for source_transform, plan in plans.items():
                files[self._traceability_path(generated_package, source_transform, plan)] = (
                    self._traceability.render(
                        plan,
                        source_transform=source_transform,
                        transform_module=transform_module,
                        schema_modules=schema_modules,
                    )
                )
        return dict(files)

    def _schema_modules(
        self,
        generated_modules: Mapping[str, str],
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
    ) -> dict[type[Schema], str]:
        modules: dict[type[Schema], str] = {}
        for source_module, schemas in source_schema_modules.items():
            module = generated_modules[source_module]
            for schema in schemas:
                modules[schema] = module
        return modules

    def _schema_source_modules(
        self,
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
        *,
        generated_package: str,
    ) -> dict[str, str]:
        names = self._schema_module_names(tuple(source_schema_modules))
        return {
            source_module: f"{generated_package}.pyspark.schemas.{names[source_module]}"
            for source_module in source_schema_modules
        }

    def _schema_module_names(self, source_modules: tuple[str, ...]) -> dict[str, str]:
        parts = {module: module.split(".") for module in source_modules}
        widths = {module: 1 for module in source_modules}
        while True:
            names = {
                module: "_".join(module_parts[-widths[module] :])
                for module, module_parts in parts.items()
            }
            duplicates = {
                name
                for name in names.values()
                if sum(candidate == name for candidate in names.values()) > 1
            }
            if not duplicates:
                return names
            for module, name in names.items():
                if name not in duplicates:
                    continue
                widths[module] = min(widths[module] + 1, len(parts[module]))

    def _transform_module(self, source_transform: str, *, generated_package: str) -> str:
        return generated_pyspark_transform_module(source_transform.rsplit(".", 1)[0], generated_package=generated_package)

    def _source_transform_module(self, source_module: str, *, generated_package: str) -> str:
        return generated_pyspark_transform_module(source_module, generated_package=generated_package)

    def _packages(self, generated_package: str, *, transform_module: str) -> tuple[str, ...]:
        packages: tuple[str, ...] = (
            generated_package,
            f"{generated_package}.pyspark",
            f"{generated_package}.pyspark.schemas",
            f"{generated_package}.pyspark.transforms",
            f"{generated_package}.runtime",
        )
        transform_parts = transform_module.split(".")[:-1]
        base_parts = generated_package.split(".")
        for width in range(len(base_parts) + 3, len(transform_parts) + 1):
            packages += (".".join(transform_parts[:width]),)
        return packages

    def _module_path(self, module: str, *, package: bool = False) -> str:
        suffix = "/__init__.py" if package else ".py"
        return module.replace(".", "/") + suffix

    def _traceability_path(self, generated_package: str, source_transform: str, plan: PySparkExecutionPlan) -> str:
        root = generated_package.replace(".", "/")
        transform_module = self._transform_module(source_transform, generated_package=generated_package)
        module = transform_module.removeprefix(f"{generated_package}.pyspark.transforms.")
        return f"{root}/traceability/transforms/{module.replace('.', '/')}.{plan.transform}.json"

    def _header(self, source: str) -> str:
        return f"# Generated by Structure. Do not edit by hand.\n# Source: {source}\n\n"

    def _package_header(self, source: str) -> str:
        return f"# Generated by Structure. Do not edit by hand.\n# Source: {source}\n"

    def _transform_source(self, source: str, generated_code_options: tuple[str, ...]) -> str:
        if "embed_hooks" in generated_code_options:
            return "embedded transform"
        return source


render_pyspark_project = RenderPySparkProject()
