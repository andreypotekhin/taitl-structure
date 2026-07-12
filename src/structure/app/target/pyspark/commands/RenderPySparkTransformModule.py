from __future__ import annotations

from collections import defaultdict
from typing import Mapping, cast

from structure.app.dsl.model.schemas.Structure import Structure
from structure.app.dsl.model.types.StructureType import StructureType
from structure.app.target.pyspark.commands.RenderPySparkSchema import render_pyspark_schema
from structure.app.target.pyspark.commands.RenderPySparkStep import render_pyspark_step
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class RenderPySparkTransformModule:

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        schema_modules: Mapping[type[Structure], str],
        runtime_module: str,
        semantic_fingerprint: str | None = None,
    ) -> str:
        imports = self._imports(
            plan, source_transform=source_transform, schema_modules=schema_modules, runtime_module=runtime_module
        )
        body = self._class(plan, source_transform=source_transform)
        metadata = self._fingerprints({source_transform: semantic_fingerprint} if semantic_fingerprint else {})
        return f"{imports}\n\n\n{metadata}{body}\n"

    def source_unit(
        self,
        plans: Mapping[str, PySparkExecutionPlan],
        *,
        schema_modules: Mapping[type[Structure], str],
        runtime_module: str,
        semantic_fingerprints: Mapping[str, str] | None = None,
    ) -> str:
        imports: list[str] = []
        bodies: list[str] = []
        for source_transform, plan in plans.items():
            imports.extend(
                self._imports(
                    plan,
                    source_transform=source_transform,
                    schema_modules=schema_modules,
                    runtime_module=runtime_module,
                ).splitlines()
            )
            bodies.append(self._class(plan, source_transform=source_transform))
        separator = "\n\n\n"
        metadata = self._fingerprints(semantic_fingerprints or {})
        return f"{self._unique(imports)}\n\n\n{metadata}{separator.join(bodies)}\n"

    def _fingerprints(self, fingerprints: Mapping[str, str]) -> str:
        if not fingerprints:
            return ""
        return f"STRUCTURE_ARTIFACT_FINGERPRINTS = {dict(sorted(fingerprints.items()))!r}\n\n\n"

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
            "from pyspark.sql import types as T",
        ]
        if self._has_window(plan):
            lines.insert(1, "from pyspark.sql import Window")
        lines.extend(self._source_imports(plan, source_transform=source_transform))

        helpers = ["TransformResult", "assert_schema", "project_schema"]
        lines.append(f"from {runtime_module} import {', '.join(helpers)}")

        for module, constants in self._schema_imports(plan, schema_modules).items():
            lines.append(f"from {module} import {', '.join(constants)}")
        return "\n".join(lines)

    def _unique(self, lines: list[str]) -> str:
        unique: list[str] = []
        seen: set[str] = set()
        for line in lines:
            if line in seen:
                continue
            seen.add(line)
            unique.append(line)
        return "\n".join(unique)

    def _class(self, plan: PySparkExecutionPlan, *, source_transform: str) -> str:
        parent_classes = self._parent_classes(plan, source_transform=source_transform)
        if not parent_classes:
            return "\n".join(self._legacy_class(plan, source_transform=source_transform))
        lines: list[str] = []
        for parent in parent_classes:
            lines.extend(self._owner_class(parent, plan, source_transform=source_transform))
            lines.append("")
            lines.append("")
        lines.extend(self._concrete_class(plan, source_transform=source_transform, parent_classes=parent_classes))
        return "\n".join(lines)

    def _legacy_class(self, plan: PySparkExecutionPlan, *, source_transform: str) -> list[str]:
        class_name = f"{plan.transform}Generated"
        source_name = source_transform.rsplit(".", 1)[1]
        lines = [f"class {class_name}:", "", "    def __init__(self, *, spark: SparkSession, ctx=None):"]
        lines.append("        self.spark = spark")
        lines.append("        self.ctx = ctx")
        if self._requires_impl(plan):
            lines.append(f"        self._impl = {source_name}()")
        lines.extend(self._udf_initializers(plan, source_name=source_name))
        lines.extend(["", "    def run(", "        self,", "        *,"])
        for input in plan.inputs:
            lines.append(f"        {input.name}: DataFrame,")
        lines.extend(["    ) -> TransformResult:"])
        for input in plan.inputs:
            lines.extend(self._validation(input.validation))
        for input in plan.inputs:
            lines.append(f"        {self._raw_input_name(input.name)} = {input.name}")

        sources = {input.name: input.name for input in plan.inputs}
        sources.update({f"input:{input.name}": self._raw_input_name(input.name) for input in plan.inputs})
        for step in plan.steps:
            lines.append("")
            lines.append(render_pyspark_step(step, current=sources[step.source], sources=sources))
            for result in step.results:
                sources[result.frame] = result.frame

        result_entries: list[str] = []
        schema_entries: list[str] = []
        for output in plan.outputs:
            lines.append("")
            lines.append(render_pyspark_step(output, current=sources[output.source], sources=sources))
            result_entries.append(f'"{output.name}": {output.name}')
            schema_entries.append(f'"{output.name}": {render_pyspark_schema.constant_name(output.output_schema)}')
        single = "True" if len(plan.outputs) == 1 else "False"
        aliases = self._output_aliases(plan)
        alias_argument = f", aliases={aliases!r}" if aliases else ""
        lines.append(
            f"        return TransformResult("
            f"{{{', '.join(result_entries)}}}, "
            f"single={single}, "
            f"schema={{{', '.join(schema_entries)}}}"
            f"{alias_argument})"
        )
        return lines

    def _owner_class(
        self,
        owner: str,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
    ) -> list[str]:
        class_name = self._generated_class_name(owner)
        lines = [f"class {class_name}:"]
        lines.extend(self._step_methods(plan, owner=owner, source_transform=source_transform))
        if len(lines) == 1:
            lines.append("    pass")
        return lines

    def _concrete_class(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        parent_classes: tuple[str, ...],
    ) -> list[str]:
        class_name = f"{plan.transform}Generated"
        bases = f"({', '.join(self._generated_class_name(owner) for owner in parent_classes)})" if parent_classes else ""
        source_name = source_transform.rsplit(".", 1)[1]
        lines = [f"class {class_name}{bases}:"]
        lines.extend(self._step_methods(plan, owner=source_transform, source_transform=source_transform))
        lines.extend(["", "    def __init__(self, *, spark: SparkSession, ctx=None):"])
        lines.append("        self.spark = spark")
        lines.append("        self.ctx = ctx")
        if self._requires_impl(plan):
            lines.append(f"        self._impl = {source_name}()")
        lines.extend(self._udf_initializers(plan, source_name=source_name))
        lines.extend(["", "    def run(", "        self,", "        *,"])
        for input in plan.inputs:
            lines.append(f"        {input.name}: DataFrame,")
        lines.extend(["    ) -> TransformResult:"])
        for input in plan.inputs:
            lines.extend(self._validation(input.validation))
        for input in plan.inputs:
            lines.append(f"        {self._raw_input_name(input.name)} = {input.name}")
        lines.extend(self._frames(plan))

        for step in plan.steps:
            lines.append(f"        frames.update(self.{self._step_method(step)}(frames))")

        sources = self._frame_sources(plan)
        result_entries: list[str] = []
        schema_entries: list[str] = []
        for output in plan.outputs:
            lines.append("")
            lines.append(render_pyspark_step(output, current=sources[output.source], sources=sources))
            result_entries.append(f'"{output.name}": {output.name}')
            schema_entries.append(f'"{output.name}": {render_pyspark_schema.constant_name(output.output_schema)}')
        single = "True" if len(plan.outputs) == 1 else "False"
        aliases = self._output_aliases(plan)
        alias_argument = f", aliases={aliases!r}" if aliases else ""
        lines.append(
            f"        return TransformResult("
            f"{{{', '.join(result_entries)}}}, "
            f"single={single}, "
            f"schema={{{', '.join(schema_entries)}}}"
            f"{alias_argument})"
        )
        return lines

    def _step_methods(
        self,
        plan: PySparkExecutionPlan,
        *,
        owner: str,
        source_transform: str,
    ) -> list[str]:
        methods: list[str] = []
        sources = self._frame_sources(plan)
        for step in plan.steps:
            if self._step_owner(step, source_transform=source_transform) != owner:
                continue
            if methods:
                methods.append("")
            methods.append(f"    def {self._step_method(step)}(self, frames):")
            methods.append(
                render_pyspark_step(
                    step,
                    current=sources[step.source],
                    sources=sources,
                    source_transform=source_transform,
                )
            )
            methods.append("        return {")
            for result in step.results:
                methods.append(f'            "{result.frame}": {result.frame},')
            methods.append("        }")
        return methods

    def _frames(self, plan: PySparkExecutionPlan) -> list[str]:
        lines = ["        frames = {"]
        for input in plan.inputs:
            lines.append(f'            "{input.name}": {input.name},')
        for input in plan.inputs:
            lines.append(f'            "input:{input.name}": {self._raw_input_name(input.name)},')
        lines.append("        }")
        return lines

    def _frame_sources(self, plan: PySparkExecutionPlan) -> dict[str, str]:
        sources = {input.name: f'frames["{input.name}"]' for input in plan.inputs}
        sources.update({f"input:{input.name}": f'frames["input:{input.name}"]' for input in plan.inputs})
        for step in plan.steps:
            for result in step.results:
                sources[result.frame] = f'frames["{result.frame}"]'
        return sources

    def _step_method(self, step) -> str:
        name = "".join(character.lower() if character.isalnum() else "_" for character in step.name).strip("_")
        return f"_step_{name}_{step.ordinal}"

    def _parent_classes(self, plan: PySparkExecutionPlan, *, source_transform: str) -> tuple[str, ...]:
        owners: list[str] = []
        for step in plan.steps:
            owner = self._step_owner(step, source_transform=source_transform)
            if owner == source_transform or owner in owners:
                continue
            owners.append(owner)
        return tuple(owners)

    def _step_owner(self, step, *, source_transform: str) -> str:
        if step.origin is None or step.origin.class_name == source_transform.rsplit(".", 1)[1]:
            return source_transform
        return step.origin.import_name

    def _generated_class_name(self, owner: str) -> str:
        return f"{owner.rsplit('.', 1)[1]}Generated"

    def _source_imports(self, plan: PySparkExecutionPlan, *, source_transform: str) -> list[str]:
        if not self._requires_impl(plan):
            return []
        imports: dict[str, set[str]] = defaultdict(set)
        module, name = source_transform.rsplit(".", 1)
        imports[module].add(name)
        for hook in self._hooks(plan):
            if hook.origin is None:
                continue
            imports[hook.origin.module].add(hook.origin.class_name)
        return [
            f"from {module} import {', '.join(sorted(names))}"
            for module, names in sorted(imports.items())
        ]

    def _requires_impl(self, plan: PySparkExecutionPlan) -> bool:
        return self._has_hooks(plan) or bool(self._udfs(plan))

    def _udf_initializers(self, plan: PySparkExecutionPlan, *, source_name: str) -> list[str]:
        lines: list[str] = []
        for udf in self._udfs(plan):
            function_name = udf["function_name"]
            return_type = self._udf_return_type(udf, source_name=source_name)
            lines.append(
                f"        self.{udf['udf_name']} = F.udf(self._impl.{function_name}, returnType={return_type})"
            )
        return lines

    def _udf_return_type(self, udf: dict[str, object], *, source_name: str) -> str:
        if udf.get("pyspark_return_type"):
            return f"{source_name}.{udf['function_name']}.return_type"
        return render_pyspark_schema.type(cast(StructureType, udf["return_type"]))

    def _udfs(self, plan: PySparkExecutionPlan) -> tuple[dict[str, object], ...]:
        found: dict[str, dict[str, object]] = {}
        for expression in self._expressions(plan):
            for udf in self._expression_udfs(expression):
                found[str(udf["udf_name"])] = udf
        return tuple(found[name] for name in sorted(found))

    def _expressions(self, plan: PySparkExecutionPlan):
        for step in plan.steps:
            yield from step.filters
            yield from (assignment.expression for assignment in step.projection)
            for result in step.results:
                yield from (assignment.expression for assignment in result.projection)
            for operation in step.operations:
                if operation.filter is not None:
                    yield operation.filter
                if operation.watermark is not None:
                    yield operation.watermark.expression
        for output in plan.outputs:
            yield from output.filters
            yield from (assignment.expression for assignment in output.projection)
            for operation in output.operations:
                if operation.filter is not None:
                    yield operation.filter

    def _expression_udfs(self, expression) -> tuple[dict[str, object], ...]:
        found = [dict(expression.data)] if expression.kind == "python_udf" else []
        for argument in expression.args:
            found.extend(self._expression_udfs(argument))
        return tuple(found)

    def _last_step_validates_final(self, plan: PySparkExecutionPlan) -> bool:
        if not plan.steps:
            return False
        final = plan.final_validation
        return any(
            validation.schema is final.schema and validation.mode is final.mode and validation.project == final.project
            for validation in plan.steps[-1].validations
        )

    def _raw_input_name(self, name: str) -> str:
        return f"_input_{name}"

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
            schemas.update(result.schema for result in step.results)
        return schemas

    def _output_aliases(self, plan: PySparkExecutionPlan) -> dict[str, tuple[str, ...]]:
        return {output.name: output.aliases for output in plan.outputs if output.aliases}

    def _has_hooks(self, plan: PySparkExecutionPlan) -> bool:
        return bool(self._hooks(plan))

    def _hooks(self, plan: PySparkExecutionPlan):
        return tuple(
            hook
            for step in plan.steps
            for hook in (
                *step.before_hooks,
                *step.after_hooks,
                *(hook for result in step.results for hook in result.after_hooks),
            )
        )

    def _has_window(self, plan: PySparkExecutionPlan) -> bool:
        joins = [join for step in plan.steps for join in step.joins]
        joins.extend(join for output in plan.outputs for join in output.joins)
        joins.extend(operation.join for step in plan.steps for operation in step.operations if operation.join)
        joins.extend(operation.join for output in plan.outputs for operation in output.operations if operation.join)
        selected_rows = [
            operation.selected_rows
            for step in plan.steps
            for operation in step.operations
            if operation.selected_rows is not None
        ]
        selected_rows.extend(
            operation.selected_rows
            for output in plan.outputs
            for operation in output.operations
            if operation.selected_rows is not None
        )
        return (
            bool(selected_rows)
            or any(join.dedupe is not None or join.as_of is not None for join in joins)
            or any(self._has_window_projection(assignment.expression) for step in plan.steps for assignment in step.projection)
            or any(
                self._has_window_projection(assignment.expression)
                for step in plan.steps
                for result in step.results
                for assignment in result.projection
            )
            or any(
                self._has_window_projection(assignment.expression)
                for output in plan.outputs
                for assignment in output.projection
            )
        )

    def _has_window_projection(self, expression) -> bool:
        data = expression.data or {}
        function = data.get("function")
        return (
            expression.kind == "reserved_v2"
            and isinstance(function, str)
            and function.startswith("window_")
        ) or any(self._has_window_projection(argument) for argument in expression.args)


render_pyspark_transform_module = RenderPySparkTransformModule()
