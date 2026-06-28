from __future__ import annotations

from structure.app.dsl.model.transforms.Transform import Transform
from structure.app.runtime.execution.online.logic.PySparkExpressionEvaluator import PySparkExpressionEvaluator
from structure.app.runtime.execution.online.logic.PySparkFrameValidator import PySparkFrameValidator
from structure.app.runtime.execution.online.logic.PySparkHookInvoker import HookInputs, PySparkHookInvoker
from structure.app.runtime.session.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.app.runtime.session.model.TransformResult import TransformResult
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe


class RunOnlinePySparkTransform:

    def __init__(self) -> None:
        self._expressions = PySparkExpressionEvaluator()
        self._hooks = PySparkHookInvoker()
        self._validator = PySparkFrameValidator()

    def __call__(
        self,
        invocation: Transform,
        plan: PySparkExecutionPlan,
        *,
        session,
    ) -> TransformResult:
        if session.online_executor is not None:
            result = session.online_executor(
                plan=plan,
                inputs=invocation._structure_bound_inputs,
                spark=session.spark,
                ctx=session.ctx,
            )
            if isinstance(result, TransformResult):
                return result
            if len(plan.outputs) == 1:
                return TransformResult({plan.outputs[0].name: result}, single=True)
            raise TypeError("Injected online executor must return TransformResult for multi-output transforms")
        if session.spark is None:
            raise self._missing_executor(invocation, session=session)

        return self._run(invocation, plan, session=session)

    def _run(self, invocation: Transform, plan: PySparkExecutionPlan, *, session):
        from pyspark.sql import functions as F  # type: ignore[import-not-found]
        from pyspark.sql import types as T  # type: ignore[import-not-found]

        inputs = invocation._structure_bound_inputs
        for input in plan.inputs:
            self._validator.validate(inputs[input.name], input.validation, types=T)

        hook_inputs = HookInputs(**inputs) if plan.requires_hook_inputs else None
        frames = dict(inputs)
        frames.update({f"input:{name}": frame for name, frame in inputs.items()})
        for step in plan.steps:
            produced = self._step(
                step,
                current=frames[step.source],
                frames=frames,
                inputs=inputs,
                hook_inputs=hook_inputs,
                invocation=invocation,
                session=session,
                functions=F,
                types=T,
            )
            frames.update(produced)

        outputs = {}
        for output in plan.outputs:
            outputs[output.name] = self._output(
                output, source=frames[output.source], inputs=inputs, functions=F, types=T
            )
        return TransformResult(outputs, single=len(plan.outputs) == 1)

    def _step(
        self,
        step: PySparkStepRecipe,
        *,
        current,
        frames,
        inputs,
        hook_inputs,
        invocation: Transform,
        session,
        functions,
        types,
    ):
        active = current
        if step.before_hooks:
            self._hooks.apply(
                step.before_hooks,
                frames=frames,
                inputs=hook_inputs,
                invocation=invocation,
                session=session,
            )
            active = frames[step.source]

        df = active.alias(step.input_alias)
        for join in step.joins:
            right = frames[join.source].alias(join.right_alias)
            if join.hint is not None and join.hint.value == "broadcast":
                right = functions.broadcast(right)
            predicate = self._expressions.evaluate(
                join.predicate, functions=functions, aliases=self._scope_aliases(step, join)
            )
            df = df.join(right, predicate, join.how.value)

        for filter in step.filters:
            df = df.where(self._expressions.evaluate(filter, functions=functions, aliases=self._scope_aliases(step)))

        if len(step.results) > 1:
            produced = {}
            for result in step.results:
                projected = df.select(
                    *(
                        self._validator.cast(
                            self._expressions.evaluate(
                                assignment.expression,
                                functions=functions,
                                aliases=self._scope_aliases(step),
                            ),
                            assignment.field,
                            types=types,
                        ).alias(assignment.field.column)
                        for assignment in result.projection
                    )
                )
                produced[result.frame] = projected
            for result in step.results:
                if result.after_hooks:
                    hook_frames = dict(frames)
                    hook_frames.update(produced)
                    self._hooks.apply(
                        result.after_hooks,
                        frames=hook_frames,
                        inputs=hook_inputs,
                        invocation=invocation,
                        session=session,
                    )
                    produced.update({name: hook_frames[name] for hook in result.after_hooks for name in hook.outputs})
                projected = produced[result.frame]
                for validation in result.validations:
                    self._validator.validate(projected, validation, types=types)
                    if validation.project:
                        projected = self._validator.project(projected, validation, types=types, functions=functions)
                produced[result.frame] = projected
            return produced

        df = df.select(
            *(
                self._validator.cast(
                    self._expressions.evaluate(
                        assignment.expression, functions=functions, aliases=self._scope_aliases(step)
                    ),
                    assignment.field,
                    types=types,
                ).alias(assignment.field.column)
                for assignment in step.projection
            )
        )
        if step.after_hooks:
            step_frames = dict(frames)
            step_frames[step.results[0].frame] = df
            self._hooks.apply(
                step.after_hooks,
                frames=step_frames,
                inputs=hook_inputs,
                invocation=invocation,
                session=session,
            )
            df = step_frames[step.results[0].frame]
        for validation in step.validations:
            self._validator.validate(df, validation, types=types)
            if validation.project:
                df = self._validator.project(df, validation, types=types, functions=functions)
        return {step.results[0].frame: df}

    def _output(
        self,
        output: PySparkOutputRecipe,
        *,
        source,
        inputs,
        functions,
        types,
    ):
        df = source.alias(output.input_alias)
        for join in output.joins:
            right = inputs[join.source].alias(join.right_alias)
            if join.hint is not None and join.hint.value == "broadcast":
                right = functions.broadcast(right)
            predicate = self._expressions.evaluate(
                join.predicate, functions=functions, aliases=self._scope_aliases(output, join)
            )
            df = df.join(right, predicate, join.how.value)

        for filter in output.filters:
            df = df.where(self._expressions.evaluate(filter, functions=functions, aliases=self._scope_aliases(output)))

        if output.projection:
            df = df.select(
                *(
                    self._validator.cast(
                        self._expressions.evaluate(
                            assignment.expression, functions=functions, aliases=self._scope_aliases(output)
                        ),
                        assignment.field,
                        types=types,
                    ).alias(assignment.field.column)
                    for assignment in output.projection
                )
            )
        self._validator.validate(df, output.validation, types=types)
        return df

    def _scope_aliases(
        self, step: PySparkStepRecipe | PySparkOutputRecipe, join: PySparkJoinRecipe | None = None
    ) -> dict[str, str]:
        aliases = {
            step.input_schema.__name__: step.input_alias,
        }
        source_scope = getattr(step, "source_scope", None)
        if source_scope is not None:
            aliases[source_scope] = step.input_alias
        if step.ordinal == 0:
            aliases["orders"] = step.input_alias
        for item in step.joins:
            aliases[item.input_name] = item.right_alias
        if join is not None:
            aliases[join.input_name] = join.right_alias
        return aliases

    def _missing_executor(self, invocation: Transform, *, session) -> StructureRuntimeError:
        transform = f"{type(invocation).__module__}.{type(invocation).__name__}"
        diagnostic = RuntimeDiagnostic(
            code="ONLINE-E1202",
            title="Online PySpark runner is not configured",
            transform=transform,
            execution_mode=session.execution_mode,
            target_backend=session.target_backend,
            problem="Structure has no live SparkSession or injected online executor for this session.",
            use="Pass spark or online_executor to StructureSession, or use execution_mode = \"generated\".",
            docs="docs/specifications/OnlineExecution.md",
        )
        return StructureRuntimeError(diagnostic)


run_online_pyspark_transform = RunOnlinePySparkTransform()
