from __future__ import annotations

from dataclasses import replace

from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.plugin.pyspark.compiler.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.plugin.pyspark.compiler.model.PySparkOperationRecipe import PySparkOperationRecipe
from structure.plugin.pyspark.compiler.model.PySparkOptimizationTrace import PySparkOptimizationTrace
from structure.plugin.pyspark.compiler.model.PySparkPosexplodeStructRecipe import PySparkPosexplodeStructRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepRecipe import PySparkStepRecipe
from structure.plugin.pyspark.compiler.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.plugin.pyspark.dsl.types import ArrayType, StructType


class OptimizePySparkProjectionUnions:
    """Fuse a private deterministic projection branch into its same-source union."""

    def __call__(self, plan: PySparkExecutionPlan) -> PySparkExecutionPlan:
        steps = list(plan.steps)
        traces = list(plan.optimizations)
        eliminated: set[str] = set()
        consumers = self._consumers(plan)
        for merge_index, merge in enumerate(steps):
            match = self._match(merge, steps, consumers, eliminated)
            if match is None:
                continue
            projection_index, projection = match
            steps[merge_index] = self._fuse(merge, projection)
            eliminated.add(projection.name)
            traces.append(
                PySparkOptimizationTrace(
                    kind="projection-union-fusion",
                    eliminated_steps=(projection.name,),
                    detail=f"projection-union fusion: {projection.name} + {merge.name}",
                )
            )
        if not eliminated:
            return plan
        return replace(
            plan,
            steps=tuple(step for step in steps if step.name not in eliminated),
            optimizations=tuple(traces),
        )

    def _consumers(self, plan: PySparkExecutionPlan) -> dict[str, int]:
        counts: dict[str, int] = {}
        for step in plan.steps:
            sources = set(step.input_sources)
            sources.update(
                relation_set.source
                for operation in step.operations
                if (relation_set := operation.relation_set) is not None
            )
            for source in sources:
                counts[source] = counts.get(source, 0) + 1
        for output in plan.outputs:
            counts[output.source] = counts.get(output.source, 0) + 1
        return counts

    def _match(
        self,
        merge: PySparkStepRecipe,
        steps: list[PySparkStepRecipe],
        consumers: dict[str, int],
        eliminated: set[str],
    ) -> tuple[int, PySparkStepRecipe] | None:
        if (
            len(merge.results) != 1
            or merge.filters
            or merge.joins
            or merge.aggregate is not None
            or merge.before_hooks
            or merge.after_hooks
            or not self._identity_projection(merge)
            or any(validation.check or validation.project or validation.boundary for validation in merge.validations)
        ):
            return None
        union_operations = [
            operation
            for operation in merge.operations
            if operation.relation_set is not None and operation.kind in {"union_all", "union_by_name"}
        ]
        if len(merge.operations) != 1 or len(union_operations) != 1:
            return None
        relation_set = union_operations[0].relation_set
        assert relation_set is not None
        if relation_set.allow_missing_columns or relation_set.defaults:
            return None
        for index, projection in enumerate(steps):
            if projection.name in eliminated or projection.results and len(projection.results) != 1:
                continue
            if (
                projection.source != merge.source
                or relation_set.source != projection.results[0].frame
                or consumers.get(projection.results[0].frame, 0) != 1
                or projection.filters
                or projection.joins
                or projection.aggregate is not None
                or projection.operations
                or projection.before_hooks
                or projection.after_hooks
                or any(
                    validation.check or validation.project or validation.boundary
                    for validation in projection.validations
                )
                or not projection.projection
                or projection.input_schema is not merge.input_schema
                or projection.output_schema is not merge.output_schema
                or not self._deterministic(projection.projection)
            ):
                continue
            result = projection.results[0]
            if (
                result.schema is not merge.output_schema
                or result.after_hooks
                or any(
                    validation.check or validation.project or validation.boundary for validation in result.validations
                )
            ):
                continue
            return index, projection
        return None

    def _fuse(self, merge: PySparkStepRecipe, projection: PySparkStepRecipe) -> PySparkStepRecipe:
        input_schema = merge.input_schema
        fields = tuple(input_schema._structure_fields.values())
        forward = PySparkExpressionRecipe(
            kind="struct",
            type=StructType(input_schema),
            nullable=False,
            data={"fields": fields},
            args=tuple(self._field(input_schema.__name__, field) for field in fields),
        )
        projected = PySparkExpressionRecipe(
            kind="struct",
            type=StructType(merge.output_schema),
            nullable=False,
            data={"fields": tuple(field for field in merge.output_schema._structure_fields.values())},
            args=tuple(
                self._retarget(expression.expression, projection, merge) for expression in projection.projection
            ),
        )
        array = PySparkExpressionRecipe(
            kind="call",
            type=ArrayType(StructType(merge.output_schema), contains_null=False),
            nullable=False,
            data={"function": "array", "capability_group": "higher_order", "capability_name": "array"},
            args=(forward, projected),
        )
        generator = PySparkPosexplodeStructRecipe(
            expression=array,
            scope="projection_union",
            schema=merge.output_schema,
            ordinal=None,
            function="explode",
        )
        return replace(
            merge,
            operations=(PySparkOperationRecipe.explode_struct_operation(generator),),
            input_sources=(merge.source,),
        )

    def _field(self, scope: str, field) -> PySparkExpressionRecipe:
        return PySparkExpressionRecipe(
            kind="field",
            type=field.type,
            nullable=field.nullable,
            data={
                "scope": scope,
                "field": field.column,
                "name": field.name,
                "path": (field.column,),
                "name_path": (field.name,),
            },
        )

    def _retarget(self, expression: PySparkExpressionRecipe, projection: PySparkStepRecipe, merge: PySparkStepRecipe):
        data = dict(expression.data)
        if expression.kind == "field":
            data["scope"] = merge.input_schema.__name__
        return replace(
            expression,
            data=data,
            args=tuple(self._retarget(argument, projection, merge) for argument in expression.args),
        )

    def _identity_projection(self, step: PySparkStepRecipe) -> bool:
        if not step.projection:
            return True
        fields = tuple(step.input_schema._structure_fields.values())
        return len(step.projection) == len(fields) and all(
            assignment.field.column == field.column
            and assignment.expression.kind == "field"
            and assignment.expression.data.get("path") == (field.column,)
            for assignment, field in zip(step.projection, fields, strict=True)
        )

    def _deterministic(self, assignments) -> bool:
        return all(self._deterministic_expression(assignment.expression) for assignment in assignments)

    def _deterministic_expression(self, expression: PySparkExpressionRecipe) -> bool:
        function = expression.data.get("function")
        return (
            expression.kind not in {"python_udf", "special_expr"}
            and function != "rand"
            and all(self._deterministic_expression(argument) for argument in expression.args)
        )


optimize_pyspark_projection_unions = OptimizePySparkProjectionUnions()
