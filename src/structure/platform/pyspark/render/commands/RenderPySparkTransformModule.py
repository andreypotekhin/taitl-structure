from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Callable, Iterable, Mapping, cast

from structure.core.dsl.model.schemas.Schema import Schema
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.platform.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.platform.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.platform.pyspark.compiler.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.platform.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.platform.pyspark.compiler.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.platform.pyspark.dsl.types import StructureType
from structure.platform.pyspark.render.commands.RenderPySparkExpression import render_pyspark_expression
from structure.platform.pyspark.render.commands.RenderPySparkStep import render_pyspark_step
from structure.platform.pyspark.render.logic.GeneratedCodeOptions import GeneratedCodeOptions
from structure.platform.pyspark.render.logic.RenderEmbeddedHooks import (
    EmbeddedHook,
    EmbeddedHookError,
    RenderEmbeddedHooks,
)


class RenderPySparkTransformModule:

    def __init__(self) -> None:
        self._embedded_hooks = RenderEmbeddedHooks()
        self._options = GeneratedCodeOptions()

    @property
    def _schema(self):
        from structure.platform.pyspark.api.PySpark import PySpark

        return PySpark.schema

    def __call__(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        schema_modules: Mapping[type[Schema], str],
        runtime_module: str,
        semantic_fingerprint: str | None = None,
        generated_code_options: tuple[str, ...] = (),
    ) -> str:
        imports = self._imports(
            plan,
            source_transform=source_transform,
            schema_modules=schema_modules,
            runtime_module=runtime_module,
            generated_code_options=generated_code_options,
        )
        body = self._class(plan, source_transform=source_transform, generated_code_options=generated_code_options)
        metadata = self._fingerprints(
            {source_transform: semantic_fingerprint} if semantic_fingerprint else {},
            generated_code_options=generated_code_options,
        )
        return f"{imports}\n\n\n{metadata}{body}\n"

    def source_unit(
        self,
        plans: Mapping[str, PySparkExecutionPlan],
        *,
        schema_modules: Mapping[type[Schema], str],
        runtime_module: str,
        semantic_fingerprints: Mapping[str, str] | None = None,
        generated_code_options: tuple[str, ...] = (),
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
                    generated_code_options=generated_code_options,
                ).splitlines()
            )
            bodies.append(
                self._class(plan, source_transform=source_transform, generated_code_options=generated_code_options)
            )
        separator = "\n\n\n"
        metadata = self._fingerprints(semantic_fingerprints or {}, generated_code_options=generated_code_options)
        return f"{self._unique(imports)}\n\n\n{metadata}{separator.join(bodies)}\n"

    def _fingerprints(
        self,
        fingerprints: Mapping[str, str],
        *,
        generated_code_options: tuple[str, ...] = (),
    ) -> str:
        if not fingerprints:
            return ""
        if self._options.enabled(generated_code_options, "embed_hooks"):
            fingerprints = {source.rsplit(".", 1)[1]: fingerprint for source, fingerprint in fingerprints.items()}
        return f"STRUCTURE_ARTIFACT_FINGERPRINTS = {dict(sorted(fingerprints.items()))!r}\n\n\n"

    def _imports(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        schema_modules: Mapping[type[Schema], str],
        runtime_module: str,
        generated_code_options: tuple[str, ...],
    ) -> str:
        lines = [
            "from pyspark.sql import DataFrame, SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql import types as T",
        ]
        if self._has_temporal_literal(plan):
            lines.insert(0, "import datetime")
        if self._has_decimal_literal(plan):
            lines.insert(0, "from decimal import Decimal")
        if self._has_window(plan):
            lines.insert(1, "from pyspark.sql import Window")
        if self._has_explicit_cache_level(plan):
            lines.insert(1, "from pyspark import StorageLevel")
        lines.extend(
            self._source_imports(
                plan,
                source_transform=source_transform,
                generated_code_options=generated_code_options,
            )
        )

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

    def _class(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        generated_code_options: tuple[str, ...],
    ) -> str:
        embedded_hooks = self._embedded(plan, generated_code_options=generated_code_options)
        embed_exprs = self._options.enabled(generated_code_options, "embed_exprs")
        with render_pyspark_expression.embed_exprs(embed_exprs):
            if self._options.enabled(generated_code_options, "mirror_methods"):
                lines = self._mirror_class(
                    plan,
                    source_transform=source_transform,
                    generated_code_options=generated_code_options,
                    embedded_hooks=embedded_hooks,
                )
            else:
                parent_classes = self._parent_classes(
                    plan,
                    source_transform=source_transform,
                    embedded_hooks=embedded_hooks,
                )
                if not parent_classes:
                    lines = self._legacy_class(
                        plan,
                        source_transform=source_transform,
                        generated_code_options=generated_code_options,
                        embedded_hooks=embedded_hooks,
                    )
                else:
                    lines = []
                    for parent in parent_classes:
                        lines.extend(
                            self._owner_class(
                                parent,
                                plan,
                                source_transform=source_transform,
                                embedded_hooks=embedded_hooks,
                            )
                        )
                        lines.append("")
                        lines.append("")
                    lines.extend(
                        self._concrete_class(
                            plan,
                            source_transform=source_transform,
                            parent_classes=parent_classes,
                            generated_code_options=generated_code_options,
                            embedded_hooks=embedded_hooks,
                        )
                    )
            if embed_exprs:
                self._insert_special_helpers(lines, plan)
            return "\n".join(lines)

    def _mirror_class(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        generated_code_options: tuple[str, ...],
        embedded_hooks: tuple[EmbeddedHook, ...],
    ) -> list[str]:
        class_name = f"{plan.transform}Generated"
        source_name = source_transform.rsplit(".", 1)[1]
        fields = self._mirror_fields(plan)
        lines = [f"class {class_name}:", "", "    def __init__(self, *, spark: SparkSession, ctx=None,"]
        for input in plan.inputs:
            lines.append(f"        {input.name}: DataFrame,")
        lines.extend(["    ):", "        self.spark = spark", "        self.ctx = ctx", "        self._ran = False"])
        for input in plan.inputs:
            lines.append(f"        self.{fields[f'input:{input.name}']} = {input.name}")
        if self._requires_impl(plan, generated_code_options=generated_code_options):
            lines.append(f"        self._impl = {source_name}()")
        lines.extend(
            self._udf_initializers(plan, source_name=source_name, generated_code_options=generated_code_options)
        )

        methods = self._mirror_step_methods(plan, source_transform=source_transform, fields=fields)
        if methods:
            lines.extend(["", *methods])

        lines.extend(["", "    def run(self) -> TransformResult:"])
        lines.extend(
            [
                "        if self._ran:",
                '            raise RuntimeError("A mirrored generated transform instance can run only once.")',
                "        self._ran = True",
            ]
        )
        for input in plan.inputs:
            current = fields[input.name]
            original = fields[f"input:{input.name}"]
            lines.append(f"        self.{current} = self.{original}")
            lines.extend(self._validation(input.validation, target=f"self.{current}"))
        for step in plan.steps:
            lines.append(f"        self.{self._mirror_step_method(step)}()")

        sources = {frame: f"self.{field}" for frame, field in fields.items()}
        result_entries: list[str] = []
        schema_entries: list[str] = []
        for output in plan.outputs:
            lines.append("")
            lines.append(render_pyspark_step(output, current=sources[output.source], sources=sources))
            lines.append(f"        self.{fields[output.name]} = {output.name}")
            result_entries.append(f'"{output.name}": self.{fields[output.name]}')
            schema_entries.append(f'"{output.name}": {self._schema.render().constant_name(output.output_schema)}')
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
        wrappers = (
            self._flat_methods(embedded_hooks)
            if self._options.enabled(generated_code_options, "embed_hooks")
            else self._hook_methods(plan, embed_hooks=False)
        )
        if wrappers:
            lines.extend(["", *wrappers])
        if self._options.enabled(generated_code_options, "embed_udfs"):
            lines.extend(["", *self._udf_methods(plan)])
        return lines

    def _mirror_step_methods(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        fields: Mapping[str, str],
    ) -> list[str]:
        methods: list[str] = []
        sources = {frame: f"self.{field}" for frame, field in fields.items()}
        for step in plan.steps:
            if methods:
                methods.append("")
            methods.append(f"    def {self._mirror_step_method(step)}(self):")
            methods.append(
                render_pyspark_step(
                    step,
                    current=sources[step.source],
                    sources=sources,
                    source_transform=source_transform,
                    generated_hooks=True,
                )
            )
            for result in step.results:
                methods.append(f"        self.{fields[result.frame]} = {result.frame}")
        return methods

    def _hook_methods(self, plan: PySparkExecutionPlan, *, embed_hooks: bool) -> list[str]:
        methods: list[str] = []
        seen: set[str] = set()
        for hook in self._hooks(plan):
            if hook.name in seen:
                continue
            seen.add(hook.name)
            if methods:
                methods.append("")
            if embed_hooks:
                raise AssertionError("Embedded hook methods are rendered by RenderEmbeddedHooks.")
            parameters = ", ".join((*hook.lanes, "spark", "ctx"))
            arguments = ", ".join(f"{parameter}={parameter}" for parameter in (*hook.lanes, "spark", "ctx"))
            methods.extend(
                [
                    f"    def {hook.name}(self, *, {parameters}):",
                    f"        return self._impl.{hook.name}({arguments})",
                ]
            )
        return methods

    def _embedded_function(self, function: Callable, *, static: bool = False) -> list[str]:
        import ast
        import inspect
        import textwrap

        node = ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]
        assert isinstance(node, ast.FunctionDef)
        node.decorator_list = []
        rendered = ast.unparse(node)
        decorators = ["    @staticmethod"] if static else []
        return [*decorators, *(f"    {line}" if line else "" for line in rendered.splitlines())]

    def _mirror_fields(self, plan: PySparkExecutionPlan) -> dict[str, str]:
        fields: dict[str, str] = {}
        used = {"spark", "ctx", "run", "_impl"}
        for input in plan.inputs:
            fields[f"input:{input.name}"] = f"_input_{input.name}"
            fields[input.name] = self._mirror_field(input.name, used)
            used.add(fields[input.name])
        for step in plan.steps:
            for result in step.results:
                if result.frame not in fields:
                    fields[result.frame] = self._mirror_field(result.frame, used)
                    used.add(fields[result.frame])
        for output in plan.outputs:
            if output.name not in fields:
                fields[output.name] = self._mirror_field(output.name, used)
                used.add(fields[output.name])
        return fields

    def _mirror_field(self, frame: str, used: set[str]) -> str:
        name = "".join(character if character.isalnum() or character == "_" else "_" for character in frame)
        name = name if name.isidentifier() else f"_frame_{name}"
        candidate = name
        ordinal = 2
        while candidate in used:
            candidate = f"{name}_{ordinal}"
            ordinal += 1
        return candidate

    def _mirror_step_method(self, step) -> str:
        name = step.origin.member_name if step.origin is not None else step.name
        return "".join(character if character.isalnum() or character == "_" else "_" for character in name)

    def _insert_special_helpers(self, lines: list[str], plan: PySparkExecutionPlan) -> None:
        definitions: dict[str, PySparkExpressionRecipe] = {}
        for expression in self._expressions(plan):
            for special in self._expression_specials(expression):
                name = str(special.data["name"])
                definitions.setdefault(name, special)
        for name in sorted(definitions):
            special = definitions[name]
            parameters = ", ".join(cast(tuple[str, ...], special.data["parameters"]))
            with render_pyspark_expression.embed_exprs(False):
                body = render_pyspark_expression(cast(PySparkExpressionRecipe, special.data["body"]))
            lines.extend(["", "    @staticmethod", f"    def {name}({parameters}):", f"        return {body}"])

    def _expression_specials(self, expression):
        if expression.kind == "special_expr":
            yield expression
            yield from self._expression_specials(expression.data["expanded"])
            return
        for argument in expression.args:
            yield from self._expression_specials(argument)

    def _legacy_class(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        generated_code_options: tuple[str, ...],
        embedded_hooks: tuple[EmbeddedHook, ...],
    ) -> list[str]:
        class_name = f"{plan.transform}Generated"
        source_name = source_transform.rsplit(".", 1)[1]
        lines = [f"class {class_name}:", "", "    def __init__(self, *, spark: SparkSession, ctx=None):"]
        lines.append("        self.spark = spark")
        lines.append("        self.ctx = ctx")
        if self._requires_impl(plan, generated_code_options=generated_code_options):
            lines.append(f"        self._impl = {source_name}()")
        lines.extend(
            self._udf_initializers(plan, source_name=source_name, generated_code_options=generated_code_options)
        )
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
            lines.append(
                render_pyspark_step(
                    step,
                    current=sources[step.source],
                    sources=sources,
                    generated_hooks=self._options.enabled(generated_code_options, "embed_hooks"),
                )
            )
            for result in step.results:
                sources[result.frame] = result.frame

        result_entries: list[str] = []
        schema_entries: list[str] = []
        for output in plan.outputs:
            lines.append("")
            lines.append(render_pyspark_step(output, current=sources[output.source], sources=sources))
            result_entries.append(f'"{output.name}": {output.name}')
            schema_entries.append(f'"{output.name}": {self._schema.render().constant_name(output.output_schema)}')
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
        if self._options.enabled(generated_code_options, "embed_hooks"):
            lines.extend(["", *self._methods_for(embedded_hooks, source_transform)])
        if self._options.enabled(generated_code_options, "embed_udfs"):
            lines.extend(["", *self._udf_methods(plan)])
        return lines

    def _owner_class(
        self,
        owner: str,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        embedded_hooks: tuple[EmbeddedHook, ...],
    ) -> list[str]:
        class_name = self._generated_class_name(owner)
        lines = [f"class {class_name}:"]
        lines.extend(
            self._step_methods(
                plan,
                owner=owner,
                source_transform=source_transform,
                generated_hooks=bool(embedded_hooks),
            )
        )
        methods = self._methods_for(embedded_hooks, owner)
        if methods:
            lines.extend(["", *methods])
        if len(lines) == 1:
            lines.append("    pass")
        return lines

    def _concrete_class(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        parent_classes: tuple[str, ...],
        generated_code_options: tuple[str, ...],
        embedded_hooks: tuple[EmbeddedHook, ...],
    ) -> list[str]:
        class_name = f"{plan.transform}Generated"
        bases = (
            f"({', '.join(self._generated_class_name(owner) for owner in parent_classes)})" if parent_classes else ""
        )
        source_name = source_transform.rsplit(".", 1)[1]
        lines = [f"class {class_name}{bases}:"]
        lines.extend(
            self._step_methods(
                plan,
                owner=source_transform,
                source_transform=source_transform,
                generated_hooks=bool(embedded_hooks),
            )
        )
        lines.extend(["", "    def __init__(self, *, spark: SparkSession, ctx=None):"])
        lines.append("        self.spark = spark")
        lines.append("        self.ctx = ctx")
        if self._requires_impl(plan, generated_code_options=generated_code_options):
            lines.append(f"        self._impl = {source_name}()")
        lines.extend(
            self._udf_initializers(plan, source_name=source_name, generated_code_options=generated_code_options)
        )
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
            schema_entries.append(f'"{output.name}": {self._schema.render().constant_name(output.output_schema)}')
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
        if self._options.enabled(generated_code_options, "embed_hooks"):
            lines.extend(["", *self._methods_for(embedded_hooks, source_transform)])
        if self._options.enabled(generated_code_options, "embed_udfs"):
            lines.extend(["", *self._udf_methods(plan)])
        return lines

    def _step_methods(
        self,
        plan: PySparkExecutionPlan,
        *,
        owner: str,
        source_transform: str,
        generated_hooks: bool,
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
                    generated_hooks=generated_hooks,
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

    def _parent_classes(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        embedded_hooks: tuple[EmbeddedHook, ...] = (),
    ) -> tuple[str, ...]:
        owners: list[str] = []
        for step in plan.steps:
            owner = self._step_owner(step, source_transform=source_transform)
            if owner == source_transform or owner in owners:
                continue
            owners.append(owner)
        for hook in embedded_hooks:
            owner = hook.origin.import_name
            if owner != source_transform and owner not in owners:
                owners.append(owner)
        return tuple(owners)

    def _step_owner(self, step, *, source_transform: str) -> str:
        if step.origin is None or step.origin.class_name == source_transform.rsplit(".", 1)[1]:
            return source_transform
        return step.origin.import_name

    def _generated_class_name(self, owner: str) -> str:
        return f"{owner.rsplit('.', 1)[1]}Generated"

    def _embedded(
        self,
        plan: PySparkExecutionPlan,
        *,
        generated_code_options: tuple[str, ...],
    ) -> tuple[EmbeddedHook, ...]:
        if not self._options.enabled(generated_code_options, "embed_hooks"):
            return ()
        if self._udfs(plan) and not self._options.enabled(generated_code_options, "embed_udfs"):
            self._embedded_fail(
                "Python UDFs still require the source transform implementation; "
                "remove embed_hooks or enable embed_udfs when that option is available."
            )
        hooks = self._embedded_hooks(self._hooks(plan))
        self._validate_embedded_names(plan, hooks)
        return hooks

    def _validate_embedded_names(self, plan: PySparkExecutionPlan, hooks: tuple[EmbeddedHook, ...]) -> None:
        reserved = {"__init__", "run"}
        reserved.update(self._step_method(step) for step in plan.steps)
        reserved.update(self._mirror_step_method(step) for step in plan.steps)
        for hook in hooks:
            if hook.name in reserved:
                self._embedded_fail(
                    f"Embedded hook {hook.name!r} collides with a generated class member.", hook=hook.name
                )

    def _methods_for(self, hooks: tuple[EmbeddedHook, ...], owner: str) -> list[str]:
        methods: list[str] = []
        for hook in hooks:
            if hook.origin.import_name != owner:
                continue
            if methods:
                methods.append("")
            methods.extend(hook.lines)
        return methods

    def _flat_methods(self, hooks: tuple[EmbeddedHook, ...]) -> list[str]:
        names: set[str] = set()
        methods: list[str] = []
        for hook in hooks:
            if hook.name in names:
                self._embedded_fail(
                    f"Embedded hook {hook.name!r} has multiple declaring owners in a flat generated class.",
                    hook=hook.name,
                )
            names.add(hook.name)
            if methods:
                methods.append("")
            methods.extend(hook.lines)
        return methods

    def _embedded_fail(self, problem: str, *, hook: str | None = None) -> None:
        context = {"hook": hook} if hook is not None else {}
        raise EmbeddedHookError(
            Diagnostic(
                entry=diagnostic_registry["GEN-E0903"],
                problem=problem,
                use="Use delegated hooks, or remove the conflicting dependency from the generated layout.",
                context=context,
            )
        )

    def _source_imports(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_transform: str,
        generated_code_options: tuple[str, ...],
    ) -> list[str]:
        if not self._requires_impl(plan, generated_code_options=generated_code_options):
            return []
        imports: dict[str, set[str]] = defaultdict(set)
        module, name = source_transform.rsplit(".", 1)
        imports[module].add(name)
        for hook in self._hooks(plan):
            if hook.origin is None:
                continue
            imports[hook.origin.module].add(hook.origin.class_name)
        return [f"from {module} import {', '.join(sorted(names))}" for module, names in sorted(imports.items())]

    def _requires_impl(self, plan: PySparkExecutionPlan, *, generated_code_options: tuple[str, ...] = ()) -> bool:
        return (self._has_hooks(plan) and not self._options.enabled(generated_code_options, "embed_hooks")) or (
            bool(self._udfs(plan)) and not self._options.enabled(generated_code_options, "embed_udfs")
        )

    def _udf_initializers(
        self,
        plan: PySparkExecutionPlan,
        *,
        source_name: str,
        generated_code_options: tuple[str, ...],
    ) -> list[str]:
        lines: list[str] = []
        for udf in self._udfs(plan):
            function_name = udf["function_name"]
            return_type = self._udf_return_type(udf, source_name=source_name)
            implementation = (
                f"self.{function_name}"
                if self._options.enabled(generated_code_options, "embed_udfs")
                else (f"self._impl.{function_name}")
            )
            lines.append(f"        self.{udf['udf_name']} = F.udf({implementation}, returnType={return_type})")
        return lines

    def _udf_methods(self, plan: PySparkExecutionPlan) -> list[str]:
        methods: list[str] = []
        for udf in self._udfs(plan):
            if methods:
                methods.append("")
            methods.extend(self._embedded_function(cast(Callable, udf["function"]), static=True))
        return methods

    def _udf_return_type(self, udf: dict[str, object], *, source_name: str) -> str:
        if udf.get("pyspark_return_type"):
            return f"{source_name}.{udf['function_name']}.return_type"
        return self._schema.render().type(cast(StructureType, udf["return_type"]))

    def _udfs(self, plan: PySparkExecutionPlan) -> tuple[dict[str, object], ...]:
        found: dict[str, dict[str, object]] = {}
        for expression in self._expressions(plan):
            for udf in self._expression_udfs(expression):
                found[str(udf["udf_name"])] = udf
        return tuple(found[name] for name in sorted(found))

    def _expressions(self, plan: PySparkExecutionPlan):
        for step in plan.steps:
            yield from self._step_expressions(step)
        for output in plan.outputs:
            yield from self._step_expressions(output)

    def _step_expressions(self, step: PySparkStepRecipe | PySparkOutputRecipe):
        yield from step.filters
        yield from (assignment.expression for assignment in step.projection)
        yield from self._joins_expressions(step.joins)
        yield from self._aggregate_expressions(getattr(step, "aggregate", None))
        for result in getattr(step, "results", ()):
            yield from (assignment.expression for assignment in result.projection)
            yield from self._aggregate_expressions(result.aggregate)
        for operation in step.operations:
            yield from self._operation_expressions(operation)

    def _operation_expressions(self, operation):
        if operation.filter is not None:
            yield operation.filter
        if operation.join is not None:
            yield from self._joins_expressions((operation.join,))
        if operation.aggregate is not None:
            yield from self._aggregate_expressions(operation.aggregate)
        if operation.selected_rows is not None:
            yield operation.selected_rows.order_by
            yield from operation.selected_rows.partition_by
        if operation.duplicate_rows is not None:
            yield from operation.duplicate_rows.subset
        if operation.watermark is not None:
            yield operation.watermark.expression

    def _has_explicit_cache_level(self, plan: PySparkExecutionPlan) -> bool:
        return self._operations_have_explicit_cache_level(plan.steps) or self._operations_have_explicit_cache_level(
            plan.outputs
        )

    def _operations_have_explicit_cache_level(
        self,
        items: Iterable[PySparkStepRecipe | PySparkOutputRecipe],
    ) -> bool:
        return any(
            operation.kind == "cache" and operation.cache is not None and operation.cache.storage_level is not None
            for item in items
            for operation in item.operations
        )

    def _joins_expressions(self, joins):
        for join in joins:
            yield join.predicate
            if join.dedupe is not None:
                yield join.dedupe.order_by
            if join.temporal is not None:
                yield join.temporal.at
                yield join.temporal.valid_from
                yield join.temporal.valid_to
            if join.as_of is not None:
                yield join.as_of.left_time
                yield join.as_of.right_time
                if join.as_of.tolerance is not None:
                    yield join.as_of.tolerance

    def _aggregate_expressions(self, aggregate):
        if aggregate is None:
            return
        yield from (key.expression for key in aggregate.keys)
        if aggregate.having is not None:
            yield aggregate.having
        for assignment in aggregate.assignments:
            if assignment.expression is not None:
                yield assignment.expression
            yield from assignment.arguments
            if assignment.filter is not None:
                yield assignment.filter
            if assignment.order_by is not None:
                yield assignment.order_by

    def _expression_udfs(self, expression) -> tuple[dict[str, object], ...]:
        found = [dict(expression.data)] if expression.kind == "python_udf" else []
        for argument in expression.args:
            found.extend(self._expression_udfs(argument))
        return tuple(found)

    def _has_temporal_literal(self, plan: PySparkExecutionPlan) -> bool:
        return any(self._has_temporal_literal_expression(expression) for expression in self._expressions(plan))

    def _has_temporal_literal_expression(self, expression) -> bool:
        return (
            isinstance(expression.data.get("default"), (date, datetime))
            or (expression.kind == "literal" and isinstance(expression.data.get("value"), (date, datetime)))
        ) or any(self._has_temporal_literal_expression(argument) for argument in expression.args)

    def _has_decimal_literal(self, plan: PySparkExecutionPlan) -> bool:
        return any(self._has_decimal_literal_expression(expression) for expression in self._expressions(plan))

    def _has_decimal_literal_expression(self, expression) -> bool:
        return (
            isinstance(expression.data.get("default"), Decimal)
            or (expression.kind == "literal" and isinstance(expression.data.get("value"), Decimal))
        ) or any(self._has_decimal_literal_expression(argument) for argument in expression.args)

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

    def _validation(self, validation: PySparkValidationRecipe, *, target: str | None = None) -> list[str]:
        schema = self._schema.render().constant_name(validation.schema)
        frame = target or (validation.target if validation.reason == "input" else "df")
        lines = [
            f'        assert_schema({frame}, {schema}, name="{validation.schema.__name__}", mode="{validation.mode.value}")'
        ]
        if validation.project:
            lines.append(f"        {frame} = project_schema({frame}, {schema})")
        return lines

    def _schema_imports(
        self,
        plan: PySparkExecutionPlan,
        schema_modules: Mapping[type[Schema], str],
    ) -> dict[str, tuple[str, ...]]:
        modules: dict[str, set[str]] = defaultdict(set)
        for schema in self._schemas(plan):
            module = schema_modules[schema]
            modules[module].add(self._schema.render().constant_name(schema))
        return {module: tuple(sorted(constants)) for module, constants in sorted(modules.items())}

    def _schemas(self, plan: PySparkExecutionPlan) -> set[type[Schema]]:
        schemas: set[type[Schema]] = {output.output_schema for output in plan.outputs}
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
            or any(
                self._has_window_projection(assignment.expression)
                for step in plan.steps
                for assignment in step.projection
            )
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
            expression.kind == "transform_expression" and isinstance(function, str) and function.startswith("window_")
        ) or any(self._has_window_projection(argument) for argument in expression.args)


render_pyspark_transform_module = RenderPySparkTransformModule()
