from __future__ import annotations

from structure.app.backend.pyspark.logic.actions.MaterializePySparkSchema import materialize_pyspark_schema
from structure.app.backend.pyspark.logic.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.backend.pyspark.logic.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.backend.pyspark.logic.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.backend.pyspark.logic.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.backend.pyspark.logic.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.backend.pyspark.logic.model.PySparkValidationRecipe import PySparkValidationRecipe
from structure.app.dsl.logic.model.transforms.Transform import Transform
from structure.app.runtime.logic.model.RuntimeDiagnostic import RuntimeDiagnostic
from structure.app.runtime.logic.model.StructureRuntimeError import StructureRuntimeError


class HookInputs:

    def __init__(self, **inputs) -> None:
        object.__setattr__(self, "_structure_frozen", False)
        object.__setattr__(self, "_structure_names", tuple(inputs))
        for name, value in inputs.items():
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_structure_frozen", True)

    def __setattr__(self, name, value) -> None:
        if getattr(self, "_structure_frozen", False):
            raise AttributeError("HookInputs is read-only")
        object.__setattr__(self, name, value)


class RunOnlinePySparkTransform:

    def __call__(
        self,
        invocation: Transform,
        plan: PySparkExecutionPlan,
        *,
        session,
    ) -> object:
        if session.online_executor is not None:
            return session.online_executor(
                plan=plan,
                inputs=invocation._structure_bound_inputs,
                spark=session.spark,
                ctx=session.ctx,
            )
        if session.spark is None:
            raise self._missing_executor(invocation, session=session)

        return self._run(invocation, plan, session=session)

    def _run(self, invocation: Transform, plan: PySparkExecutionPlan, *, session):
        from pyspark.sql import functions as F  # type: ignore[import-not-found]
        from pyspark.sql import types as T  # type: ignore[import-not-found]

        inputs = invocation._structure_bound_inputs
        for input in plan.inputs:
            self._validate(inputs[input.name], input.validation, types=T)

        hook_inputs = HookInputs(**inputs) if plan.requires_hook_inputs else None
        current = inputs[plan.inputs[0].name]
        for step in plan.steps:
            current = self._step(
                step,
                current=current,
                inputs=inputs,
                hook_inputs=hook_inputs,
                invocation=invocation,
                session=session,
                functions=F,
                types=T,
            )
        if not self._last_step_validates_final(plan):
            self._validate(current, plan.final_validation, types=T)
        return current

    def _step(
        self,
        step: PySparkStepRecipe,
        *,
        current,
        inputs,
        hook_inputs,
        invocation: Transform,
        session,
        functions,
        types,
    ):
        active = current
        if step.before_hooks:
            active = self._hooks(
                step.before_hooks,
                current=active,
                inputs=hook_inputs,
                invocation=invocation,
                session=session,
            )

        df = active.alias(step.input_alias)
        for join in step.joins:
            right = inputs[join.input_name].alias(join.right_alias)
            if join.hint is not None and join.hint.value == "broadcast":
                right = functions.broadcast(right)
            predicate = self._expression(join.predicate, functions=functions, aliases=self._scope_aliases(step, join))
            df = df.join(right, predicate, join.how.value)

        for filter in step.filters:
            df = df.where(self._expression(filter, functions=functions, aliases=self._scope_aliases(step)))

        df = df.select(
            *(
                self._expression(assignment.expression, functions=functions, aliases=self._scope_aliases(step)).alias(
                    assignment.field.name
                )
                for assignment in step.projection
            )
        )
        if step.after_hooks:
            df = self._hooks(
                step.after_hooks,
                current=df,
                inputs=hook_inputs,
                invocation=invocation,
                session=session,
            )
        for validation in step.validations:
            self._validate(df, validation, types=types)
            if validation.project:
                df = self._project(df, validation, types=types, functions=functions)
        return df

    def _hooks(
        self,
        hooks: tuple[PySparkHookRecipe, ...],
        *,
        current,
        inputs,
        invocation: Transform,
        session,
    ):
        df = current
        for hook in hooks:
            kwargs = {"df": df, "spark": session.spark, "ctx": session.ctx}
            if hook.pass_inputs:
                kwargs["inputs"] = inputs
            df = getattr(invocation, hook.name)(**kwargs)
        return df

    def _expression(self, expression: PySparkExpressionRecipe, *, functions, aliases):
        if expression.kind == "field":
            scope = str(expression.data["scope"])
            field = str(expression.data["field"])
            alias = aliases.get(scope, scope)
            return functions.col(f"{alias}.{field}")
        if expression.kind == "literal":
            return functions.lit(expression.data["value"])
        if expression.kind == "call":
            return self._call(expression, functions=functions, aliases=aliases)
        if expression.kind == "is_not_null":
            return self._expression(expression.args[0], functions=functions, aliases=aliases).isNotNull()
        if expression.kind == "is_null":
            return self._expression(expression.args[0], functions=functions, aliases=aliases).isNull()
        if expression.kind == "and":
            return self._binary(expression, functions=functions, aliases=aliases, operator="and")
        if expression.kind == "or":
            return self._binary(expression, functions=functions, aliases=aliases, operator="or")
        if expression.kind == "eq":
            return self._binary(expression, functions=functions, aliases=aliases, operator="eq")
        if expression.kind == "ne":
            return self._binary(expression, functions=functions, aliases=aliases, operator="ne")
        if expression.kind == "gt":
            return self._binary(expression, functions=functions, aliases=aliases, operator="gt")
        if expression.kind == "sub":
            return self._binary(expression, functions=functions, aliases=aliases, operator="sub")
        if expression.kind == "null_safe_eq":
            left, right = expression.args
            return self._expression(left, functions=functions, aliases=aliases).eqNullSafe(
                self._expression(right, functions=functions, aliases=aliases)
            )
        if expression.kind == "not":
            return ~self._expression(expression.args[0], functions=functions, aliases=aliases)
        raise TypeError(f"Unsupported PySpark expression recipe: {expression.kind}")

    def _call(self, expression: PySparkExpressionRecipe, *, functions, aliases):
        function = expression.data["function"]
        args = [self._expression(argument, functions=functions, aliases=aliases) for argument in expression.args]
        if function == "lower":
            return functions.lower(args[0])
        if function == "trim":
            return functions.trim(args[0])
        if function == "coalesce":
            return functions.coalesce(*args)
        if function == "to_decimal":
            precision = expression.data["precision"]
            scale = expression.data["scale"]
            return args[0].cast(f"decimal({precision},{scale})")
        raise TypeError(f"Unsupported PySpark helper call: {function}")

    def _binary(self, expression: PySparkExpressionRecipe, *, functions, aliases, operator: str):
        left, right = (self._expression(argument, functions=functions, aliases=aliases) for argument in expression.args)
        if operator == "and":
            return left & right
        if operator == "or":
            return left | right
        if operator == "eq":
            return left == right
        if operator == "ne":
            return left != right
        if operator == "gt":
            return left > right
        if operator == "sub":
            return left - right
        raise TypeError(f"Unsupported PySpark binary operator: {operator}")

    def _validate(self, df, validation: PySparkValidationRecipe, *, types) -> None:
        schema = materialize_pyspark_schema(validation.schema, types=types)
        actual = df.schema
        actual_names = set(actual.fieldNames())
        expected_names = {field.name for field in schema}
        missing = expected_names - actual_names
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(f"{validation.schema.__name__} is missing required column(s): {names}")
        if validation.mode.value == "strict":
            extra = actual_names - expected_names
            if extra:
                names = ", ".join(sorted(extra))
                raise ValueError(f"{validation.schema.__name__} has unexpected column(s): {names}")
        for expected in schema:
            actual_field = actual[expected.name]
            if actual_field.dataType != expected.dataType:
                raise ValueError(
                    f"{validation.schema.__name__}.{expected.name} expected "
                    f"{expected.dataType}, got {actual_field.dataType}"
                )

    def _project(self, df, validation: PySparkValidationRecipe, *, types, functions):
        schema = materialize_pyspark_schema(validation.schema, types=types)
        return df.select(*(functions.col(field.name) for field in schema))

    def _last_step_validates_final(self, plan: PySparkExecutionPlan) -> bool:
        if not plan.steps:
            return False
        final = plan.final_validation
        return any(
            validation.schema is final.schema and validation.mode is final.mode and validation.project == final.project
            for validation in plan.steps[-1].validations
        )

    def _scope_aliases(self, step: PySparkStepRecipe, join: PySparkJoinRecipe | None = None) -> dict[str, str]:
        aliases = {
            step.input_schema.__name__: step.input_alias,
        }
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
