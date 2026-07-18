from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence

from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.target.pyspark.commands.RenderPySparkRuntimeModule import render_pyspark_runtime_module
from structure.core.target.pyspark.commands.RenderPySparkSchemaModule import render_pyspark_schema_module
from structure.core.target.pyspark.commands.RenderPySparkTransformModule import render_pyspark_transform_module
from structure.core.target.pyspark.logic.render.PySparkTraceabilityReport import PySparkTraceabilityReport
from structure.core.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan


class RenderPySparkProject:

    def __init__(self) -> None:
        self._traceability = PySparkTraceabilityReport()

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
        generated_package: str,
        semantic_fingerprint: str | None = None,
        generated_code_options: tuple[str, ...] = (),
    ) -> dict[str, str]:
        schema_modules = self._schema_modules(source_schema_modules, generated_package=generated_package)
        runtime_module = f"{generated_package}.runtime.schema_assert"
        transform_module = self._transform_module(source_transform, generated_package=generated_package)

        files: OrderedDict[str, str] = OrderedDict()
        for package in self._packages(generated_package):
            files[self._module_path(package, package=True)] = self._header("Structure generated package")

        files[self._module_path(runtime_module)] = (
            self._header("Structure generated runtime") + render_pyspark_runtime_module()
        )

        for source_module in sorted(source_schema_modules):
            module = self._schema_module(source_module, generated_package=generated_package)
            schemas = source_schema_modules[source_module]
            files[self._module_path(module)] = self._header(source_module) + render_pyspark_schema_module(
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
        )

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
    ) -> dict[str, str]:
        schema_modules = self._schema_modules(source_schema_modules, generated_package=generated_package)
        runtime_module = f"{generated_package}.runtime.schema_assert"
        transform_module = self._source_transform_module(source_module, generated_package=generated_package)

        files: OrderedDict[str, str] = OrderedDict()
        for package in self._packages(generated_package):
            files[self._module_path(package, package=True)] = self._header("Structure generated package")

        files[self._module_path(runtime_module)] = (
            self._header("Structure generated runtime") + render_pyspark_runtime_module()
        )

        for schema_source_module in sorted(source_schema_modules):
            module = self._schema_module(schema_source_module, generated_package=generated_package)
            schemas = source_schema_modules[schema_source_module]
            files[self._module_path(module)] = self._header(schema_source_module) + render_pyspark_schema_module(
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
        )

        for source_transform, plan in plans.items():
            files[self._traceability_path(generated_package, source_transform, plan)] = self._traceability.render(
                plan,
                source_transform=source_transform,
                transform_module=transform_module,
                schema_modules=schema_modules,
            )
        return dict(files)

    def _schema_modules(
        self,
        source_schema_modules: Mapping[str, Sequence[type[Schema]]],
        *,
        generated_package: str,
    ) -> dict[type[Schema], str]:
        modules: dict[type[Schema], str] = {}
        for source_module, schemas in source_schema_modules.items():
            module = self._schema_module(source_module, generated_package=generated_package)
            for schema in schemas:
                modules[schema] = module
        return modules

    def _schema_module(self, source_module: str, *, generated_package: str) -> str:
        name = source_module.rsplit(".", 1)[1]
        return f"{generated_package}.pyspark.schemas.{name}"

    def _transform_module(self, source_transform: str, *, generated_package: str) -> str:
        name = source_transform.rsplit(".", 2)[1]
        return f"{generated_package}.pyspark.transforms.{name}"

    def _source_transform_module(self, source_module: str, *, generated_package: str) -> str:
        name = source_module.rsplit(".", 1)[1]
        return f"{generated_package}.pyspark.transforms.{name}"

    def _packages(self, generated_package: str) -> tuple[str, ...]:
        return (
            generated_package,
            f"{generated_package}.pyspark",
            f"{generated_package}.pyspark.schemas",
            f"{generated_package}.pyspark.transforms",
            f"{generated_package}.runtime",
        )

    def _module_path(self, module: str, *, package: bool = False) -> str:
        suffix = "/__init__.py" if package else ".py"
        return module.replace(".", "/") + suffix

    def _traceability_path(self, generated_package: str, source_transform: str, plan: PySparkExecutionPlan) -> str:
        root = generated_package.replace(".", "/")
        return f"{root}/traceability/transforms/{source_transform.rsplit('.', 2)[1]}.{plan.transform}.json"

    def _header(self, source: str) -> str:
        return f"# Generated by Structure. Do not edit by hand.\n# Source: {source}\n\n"

    def _transform_source(self, source: str, generated_code_options: tuple[str, ...]) -> str:
        if "embed_hooks" in generated_code_options:
            return "embedded transform"
        return source


render_pyspark_project = RenderPySparkProject()
