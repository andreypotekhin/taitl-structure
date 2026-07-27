from collections.abc import Mapping
from dataclasses import replace

from structure.plugin.pyspark.dsl.aggregation import AggregateAssignment, AggregateKey, AggregatePlan, ProjectAssignment
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.joins import JoinPlan
from structure.plugin.pyspark.dsl.operations import DuplicateRowsPlan, OperationPlan, SelectedRowsPlan
from structure.plugin.pyspark.symbolic_execution.model.PySparkResultBody import PySparkResultBody
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class RewritePySparkStepBody:
    """Rewrite a captured PySpark body when Core composes structural lanes."""

    def __call__(self, body: object, *, frames: Mapping[str, str]) -> object:
        if not isinstance(body, PySparkStepBody):
            return body
        return replace(
            body,
            filters=tuple(self._expression(expression) for expression in body.filters),
            joins=tuple(self._join(join, frames=frames) for join in body.joins),
            operations=tuple(self._operation(operation, frames=frames) for operation in body.operations),
            projection=tuple(self._projection(assignment) for assignment in body.projection),
            aggregate=None if body.aggregate is None else self._aggregate(body.aggregate),
            results=tuple(self._result(result) for result in body.results),
        )

    def _result(self, result: PySparkResultBody) -> PySparkResultBody:
        return replace(
            result,
            projection=tuple(self._projection(assignment) for assignment in result.projection),
            aggregate=None if result.aggregate is None else self._aggregate(result.aggregate),
        )

    def _operation(self, operation: OperationPlan, *, frames: Mapping[str, str]) -> OperationPlan:
        return replace(
            operation,
            filter=None if operation.filter is None else self._expression(operation.filter),
            join=None if operation.join is None else self._join(operation.join, frames=frames),
            aggregate=None if operation.aggregate is None else self._aggregate(operation.aggregate),
            selected_rows=None if operation.selected_rows is None else self._selected_rows(operation.selected_rows),
            duplicate_rows=None if operation.duplicate_rows is None else self._duplicate_rows(operation.duplicate_rows),
            posexplode_struct=(
                None if operation.posexplode_struct is None else self._posexplode_struct(operation.posexplode_struct)
            ),
            relation_alias=(
                None
                if operation.relation_alias is None
                else self._relation_alias(operation.relation_alias, frames=frames)
            ),
            relation_assertion=(
                None
                if operation.relation_assertion is None
                else self._relation_assertion(operation.relation_assertion)
            ),
            relation_order=(
                None if operation.relation_order is None else self._relation_order(operation.relation_order)
            ),
            relation_set=(
                None if operation.relation_set is None else self._relation_set(operation.relation_set, frames=frames)
            ),
        )

    def _join(self, join: JoinPlan, *, frames: Mapping[str, str]) -> JoinPlan:
        temporal = join.temporal
        as_of = join.as_of
        dedupe = join.dedupe
        if temporal is not None:
            temporal = replace(
                temporal,
                at=self._expression(temporal.at),
                valid_from=self._expression(temporal.valid_from),
                valid_to=self._expression(temporal.valid_to),
            )
        if as_of is not None:
            as_of = replace(
                as_of,
                left_time=self._expression(as_of.left_time),
                right_time=self._expression(as_of.right_time),
                tolerance=None if as_of.tolerance is None else self._expression(as_of.tolerance),
            )
        if dedupe is not None:
            dedupe = replace(dedupe, order_by=self._expression(dedupe.order_by))
        return replace(
            join,
            source=frames.get(join.source, join.source),
            predicate=self._expression(join.predicate),
            dedupe=dedupe,
            temporal=temporal,
            as_of=as_of,
        )

    def _aggregate(self, aggregate: AggregatePlan) -> AggregatePlan:
        return AggregatePlan(
            keys=tuple(AggregateKey(name=key.name, expression=self._expression(key.expression)) for key in aggregate.keys),
            assignments=tuple(
                AggregateAssignment(
                    field=assignment.field,
                    function=assignment.function,
                    expression=None if assignment.expression is None else self._expression(assignment.expression),
                    key=assignment.key,
                    arguments=tuple(self._expression(argument) for argument in assignment.arguments),
                    filter=None if assignment.filter is None else self._expression(assignment.filter),
                    order_by=None if assignment.order_by is None else self._expression(assignment.order_by),
                    options=assignment.options,
                )
                for assignment in aggregate.assignments
            ),
            grouping=aggregate.grouping,
            levels=aggregate.levels,
            having=None if aggregate.having is None else self._expression(aggregate.having),
        )

    def _selected_rows(self, selected_rows: SelectedRowsPlan) -> SelectedRowsPlan:
        return replace(
            selected_rows,
            order_by=self._expression(selected_rows.order_by),
            partition_by=tuple(self._expression(expression) for expression in selected_rows.partition_by),
        )

    def _duplicate_rows(self, duplicate_rows: DuplicateRowsPlan) -> DuplicateRowsPlan:
        return DuplicateRowsPlan(
            subset=tuple(self._expression(expression) for expression in duplicate_rows.subset),
            scope=duplicate_rows.scope,
        )

    def _posexplode_struct(self, posexplode_struct):
        return replace(posexplode_struct, expression=self._expression(posexplode_struct.expression))

    def _relation_alias(self, relation_alias, *, frames: Mapping[str, str]):
        return replace(relation_alias, source=frames.get(relation_alias.source, relation_alias.source))

    def _relation_assertion(self, relation_assertion):
        return replace(
            relation_assertion,
            keys=tuple(self._expression(expression) for expression in relation_assertion.keys),
            predicate=(
                None
                if relation_assertion.predicate is None
                else self._expression(relation_assertion.predicate)
            ),
            value=None if relation_assertion.value is None else self._expression(relation_assertion.value),
            reference_key=(
                None
                if relation_assertion.reference_key is None
                else self._expression(relation_assertion.reference_key)
            ),
        )

    def _relation_order(self, relation_order):
        return replace(
            relation_order,
            order_by=tuple(self._expression(expression) for expression in relation_order.order_by),
        )

    def _relation_set(self, relation_set, *, frames: Mapping[str, str]):
        return replace(relation_set, source=frames.get(relation_set.source, relation_set.source))

    def _projection(self, assignment: ProjectAssignment) -> ProjectAssignment:
        return ProjectAssignment(field=assignment.field, expression=self._expression(assignment.expression))

    def _expression(self, expression: Expression) -> Expression:
        return replace(expression, args=tuple(self._expression(argument) for argument in expression.args))
