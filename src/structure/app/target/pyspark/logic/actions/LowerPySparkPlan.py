from __future__ import annotations

import re

from structure.app.target.capabilities.api.capabilities import resolve_backend_capabilities
from structure.app.target.capabilities.logic.model.BackendCapabilities import BackendCapabilities
from structure.app.target.capabilities.logic.model.CapabilityRequirement import CapabilityRequirement
from structure.app.target.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.logic.model.PySparkInputRecipe import PySparkInputRecipe
from structure.app.target.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.logic.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.logic.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.logic.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.dsl.logic.model.expr.Expression import Expression
from structure.app.compiler.ir.logic.model.HookPlan import HookPlan
from structure.app.compiler.ir.logic.model.JoinPlan import JoinPlan
from structure.app.compiler.ir.logic.model.OutputPlan import OutputPlan
from structure.app.compiler.ir.logic.model.ProjectAssignment import ProjectAssignment
from structure.app.compiler.ir.logic.model.StepPlan import StepPlan
from structure.app.compiler.ir.logic.model.TransformPlan import TransformPlan
from structure.app.dsl.logic.model.schemas.Structure import Structure
from structure.app.dsl.logic.model.transforms.SchemaMode import SchemaMode


class LowerPySparkPlan:

    def __call__(
        self,
        plan: TransformPlan,
        *,
        capabilities: BackendCapabilities | None = None,
    ) -> PySparkExecutionPlan:
        target = capabilities or resolve_backend_capabilities()
        inputs = tuple(
            self._input(input_plan.name, input_plan.schema, input_plan.ordinal) for input_plan in plan.inputs
        )
        steps = tuple(
            self._step(step, index=index, last=index == len(plan.steps) - 1, capabilities=target)
            for index, step in enumerate(plan.steps)
        )
        outputs = tuple(self._output(output, capabilities=target) for output in plan.outputs)
        return PySparkExecutionPlan(
            transform=plan.name,
            backend=target.id,
            inputs=inputs,
            steps=steps,
            outputs=outputs,
            requires_hook_inputs=any(
                hook.pass_inputs for step in steps for hook in (*step.before_hooks, *step.after_hooks)
            ),
        )

    def _input(self, name: str, schema: type[Structure], ordinal: int) -> PySparkInputRecipe:
        return PySparkInputRecipe(
            name=name,
            schema=schema,
            ordinal=ordinal,
            validation=PySparkValidationRecipe(
                target=name,
                schema=schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="input",
            ),
        )

    def _step(
        self,
        step: StepPlan,
        *,
        index: int,
        last: bool,
        capabilities: BackendCapabilities,
    ) -> PySparkStepRecipe:
        input_alias = self._alias(step.input_schema.__name__)
        output_alias = self._alias(step.output_schema.__name__)
        return PySparkStepRecipe(
            name=step.name,
            ordinal=step.ordinal,
            source=step.source,
            source_scope=step.source_scope,
            input_schema=step.input_schema,
            output_schema=step.output_schema,
            input_alias=input_alias,
            output_alias=output_alias,
            before_hooks=tuple(self._hook(hook) for hook in step.before_hooks),
            filters=tuple(self._expression(filter, capabilities=capabilities) for filter in step.filters),
            joins=tuple(
                self._join(join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities)
                for occurrence, join in enumerate(step.joins, start=1)
            ),
            projection=tuple(self._projection(assignment, capabilities=capabilities) for assignment in step.projection),
            after_hooks=tuple(self._hook(hook) for hook in step.after_hooks),
            validations=self._validations(step, last=last),
        )

    def _output(
        self,
        output: OutputPlan,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkOutputRecipe:
        input_alias = self._alias(output.source_schema.__name__)
        output_alias = self._alias(output.schema.__name__)
        return PySparkOutputRecipe(
            name=output.name,
            ordinal=output.ordinal,
            source=output.source,
            source_scope=output.source_scope,
            input_schema=output.source_schema,
            output_schema=output.schema,
            input_alias=input_alias,
            output_alias=output_alias,
            filters=tuple(self._expression(filter, capabilities=capabilities) for filter in output.filters),
            joins=tuple(
                self._join(join, occurrence=occurrence, left_alias=input_alias, capabilities=capabilities)
                for occurrence, join in enumerate(output.joins, start=1)
            ),
            projection=tuple(
                self._projection(assignment, capabilities=capabilities) for assignment in output.projection
            ),
            validation=PySparkValidationRecipe(
                target=output.name,
                schema=output.schema,
                mode=SchemaMode.STRICT,
                project=False,
                reason="final",
            ),
        )

    def _join(
        self,
        join: JoinPlan,
        *,
        occurrence: int,
        left_alias: str,
        capabilities: BackendCapabilities,
    ) -> PySparkJoinRecipe:
        capabilities.require(CapabilityRequirement(group="join", name="join_one"))
        capabilities.require(CapabilityRequirement(group="join", name=f"{join.how.value}_join"))
        if join.hint is not None:
            capabilities.require(CapabilityRequirement(group="join", name=f"{join.hint.value}_hint"))

        return PySparkJoinRecipe(
            input_name=join.input_name,
            input_schema=join.input_schema,
            left_alias=left_alias,
            right_alias=self._join_alias(join.input_name, occurrence),
            how=join.how,
            hint=join.hint,
            predicate=self._expression(join.predicate, capabilities=capabilities),
            occurrence=occurrence,
        )

    def _projection(
        self,
        assignment: ProjectAssignment,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkProjectionRecipe:
        capabilities.require(CapabilityRequirement(group="expression", name="projection"))
        return PySparkProjectionRecipe(
            field=assignment.field,
            expression=self._expression(assignment.expression, capabilities=capabilities),
        )

    def _expression(
        self,
        expression: Expression,
        *,
        capabilities: BackendCapabilities,
    ) -> PySparkExpressionRecipe:
        capabilities.require(CapabilityRequirement(group="expression", name=self._requirement(expression)))
        return PySparkExpressionRecipe(
            kind=expression.kind,
            type=expression.type,
            nullable=expression.nullable,
            data=dict(expression.data or {}),
            args=tuple(self._expression(argument, capabilities=capabilities) for argument in expression.args),
        )

    def _requirement(self, expression: Expression) -> str:
        if expression.kind == "field":
            return "field_ref"
        if expression.kind == "literal":
            return "literal"
        if expression.kind in {"and", "or", "not", "is_null", "is_not_null"}:
            return "boolean_ops"
        if expression.kind in {"eq", "ne", "gt"}:
            return "equality"
        if expression.kind == "null_safe_eq":
            return "null_safe_equality"
        if expression.kind == "sub":
            return "standard_helper_call"
        if expression.kind == "call":
            function = (expression.data or {}).get("function")
            if function == "to_decimal":
                return "cast"
            return "standard_helper_call"
        return "standard_helper_call"

    def _hook(self, hook: HookPlan) -> PySparkHookRecipe:
        return PySparkHookRecipe(
            name=hook.name,
            phase=hook.phase,
            target=hook.target,
            pass_inputs=hook.pass_inputs,
            schema_mode=hook.schema_mode,
            project_output=hook.project_output,
            streaming_safe=hook.streaming_safe,
        )

    def _validations(self, step: StepPlan, *, last: bool) -> tuple[PySparkValidationRecipe, ...]:
        recipes: list[PySparkValidationRecipe] = []
        for hook in step.after_hooks:
            recipes.append(
                PySparkValidationRecipe(
                    target=f"hook:{hook.name}",
                    schema=step.output_schema,
                    mode=hook.schema_mode,
                    project=hook.project_output,
                    reason="hook",
                )
            )
            if hook.project_output:
                recipes.append(
                    PySparkValidationRecipe(
                        target=f"hook:{hook.name}",
                        schema=step.output_schema,
                        mode=SchemaMode.STRICT,
                        project=False,
                        reason="hook_projected",
                    )
                )

        if not last:
            recipes.append(
                PySparkValidationRecipe(
                    target=step.name,
                    schema=step.output_schema,
                    mode=SchemaMode.STRICT,
                    project=False,
                    reason="intermediate",
                )
            )
        return tuple(recipes)

    def _alias(self, name: str) -> str:
        return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()

    def _join_alias(self, name: str, occurrence: int) -> str:
        if occurrence == 1:
            return name
        return f"{name}_{occurrence}"


lower_pyspark_plan = LowerPySparkPlan()
