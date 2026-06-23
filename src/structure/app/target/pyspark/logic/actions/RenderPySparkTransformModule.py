from __future__ import annotations

from collections import defaultdict
from typing import Mapping

from structure.app.target.pyspark.logic.actions.RenderPySparkSchema import render_pyspark_schema
from structure.app.target.pyspark.logic.actions.RenderPySparkStep import render_pyspark_step
from structure.app.target.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.dsl.logic.model.schemas.Structure import Structure


class RenderPySparkTransformModule:

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        schema_modules: Mapping[type[Structure], str],
        runtime_module: str,
    ) -> str:
        imports = self._imports(
            plan, source_transform=source_transform, schema_modules=schema_modules, runtime_module=runtime_module
        )
        body = self._class(plan, source_transform=source_transform)
        return f"{imports}\n\n\n{body}\n"

    def _imports(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        schema_modules: Mapping[type[Structure], str],
        runtime_module: str,
    ) -> str:
        lines = [
            "from pyspark.sql import DataFrame, SparkSession",
            "from pyspark.sql import functions as F",
        ]
        if self._has_hooks(plan):
            module, name = source_transform.rsplit(".", 1)
            lines.append(f"from {module} import {name}")

        helpers = ["TransformResult", "assert_schema", "project_schema"]
        if plan.requires_hook_inputs:
            helpers.append("HookInputs")
        lines.append(f"from {runtime_module} import {', '.join(helpers)}")

        for module, constants in self._schema_imports(plan, schema_modules).items():
            lines.append(f"from {module} import {', '.join(constants)}")
        return "\n".join(lines)

    def _class(self, plan: PySparkExecutionPlan, *, source_transform: str) -> str:
        class_name = f"{plan.transform}Generated"
        source_name = source_transform.rsplit(".", 1)[1]
        lines = [f"class {class_name}:", "", "    def __init__(self, *, spark: SparkSession, ctx=None):"]
        lines.append("        self.spark = spark")
        lines.append("        self.ctx = ctx")
        if self._has_hooks(plan):
            lines.append(f"        self._impl = {source_name}()")
        lines.extend(["", "    def run(", "        self,", "        *,"])
        for input in plan.inputs:
            lines.append(f"        {input.name}: DataFrame,")
        lines.extend(["    ) -> TransformResult:"])
        for input in plan.inputs:
            lines.extend(self._validation(input.validation))
        if plan.requires_hook_inputs:
            lines.extend(self._hook_inputs(plan))

        sources = {input.name: input.name for input in plan.inputs}
        for step in plan.steps:
            lines.append("")
            lines.append(render_pyspark_step(step, current=sources[step.source]))
            source_name = f"{step.name}_df"
            lines.append(f"        {source_name} = df")
            sources[step.name] = source_name

        result_entries: list[str] = []
        for output in plan.outputs:
            lines.append("")
            lines.append(render_pyspark_step(output, current=sources[output.source]))
            output_name = f"{output.name}_df"
            lines.append(f"        {output_name} = df")
            result_entries.append(f'"{output.name}": {output_name}')
        single = "True" if len(plan.outputs) == 1 else "False"
        lines.append(f"        return TransformResult({{{', '.join(result_entries)}}}, single={single})")
        return "\n".join(lines)

    def _last_step_validates_final(self, plan: PySparkExecutionPlan) -> bool:
        if not plan.steps:
            return False
        final = plan.final_validation
        return any(
            validation.schema is final.schema and validation.mode is final.mode and validation.project == final.project
            for validation in plan.steps[-1].validations
        )

    def _hook_inputs(self, plan: PySparkExecutionPlan) -> list[str]:
        lines = ["        inputs = HookInputs("]
        for input in plan.inputs:
            lines.append(f"            {input.name}={input.name},")
        lines.append("        )")
        return lines

    def _validation(self, validation: PySparkValidationRecipe) -> list[str]:
        schema = render_pyspark_schema.constant_name(validation.schema)
        target = validation.target if validation.reason == "input" else "df"
        lines = [
            f'        assert_schema({target}, {schema}, name="{validation.schema.__name__}", mode="{validation.mode.value}")'
        ]
        if validation.project:
            lines.append(f"        df = project_schema(df, {schema})")
        return lines

    def _schema_imports(
        self,
        plan: PySparkExecutionPlan,
        schema_modules: Mapping[type[Structure], str],
    ) -> dict[str, tuple[str, ...]]:
        modules: dict[str, set[str]] = defaultdict(set)
        for schema in self._schemas(plan):
            module = schema_modules[schema]
            modules[module].add(render_pyspark_schema.constant_name(schema))
        return {module: tuple(sorted(constants)) for module, constants in sorted(modules.items())}

    def _schemas(self, plan: PySparkExecutionPlan) -> set[type[Structure]]:
        schemas: set[type[Structure]] = {output.output_schema for output in plan.outputs}
        for input in plan.inputs:
            schemas.add(input.schema)
        for step in plan.steps:
            schemas.add(step.output_schema)
        return schemas

    def _has_hooks(self, plan: PySparkExecutionPlan) -> bool:
        return any(step.before_hooks or step.after_hooks for step in plan.steps)


render_pyspark_transform_module = RenderPySparkTransformModule()
