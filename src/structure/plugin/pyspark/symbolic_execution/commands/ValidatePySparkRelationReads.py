from structure import StructureCompileError
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class ValidatePySparkRelationReads:
    """Reject relation-field reads that precede their PySpark join operation."""

    def __call__(self, body: PySparkStepBody, *, request: StepAuthoringRequest) -> None:
        joined: set[str] = set()
        relations = {binding.scope: binding.parameter for binding in request.inputs if not binding.driving}
        for operation in body.operations:
            if operation.kind == "filter" and operation.filter is not None:
                self._validate(relations, joined, self._scopes(operation.filter), request)
            if operation.kind == "join" and operation.join is not None:
                join = operation.join
                if join.temporal is not None:
                    self._validate(relations, joined, self._scopes(join.temporal.at), request)
                if join.as_of is not None:
                    self._validate(relations, joined, self._scopes(join.as_of.left_time), request)
                    if join.as_of.tolerance is not None:
                        self._validate(relations, joined, self._scopes(join.as_of.tolerance), request)
                if join.method.exposes_fields():
                    joined.add(join.input_name)
            if operation.kind == "aggregate" and operation.aggregate is not None:
                aggregate = operation.aggregate
                reads = set().union(
                    *(self._scopes(key.expression) for key in aggregate.keys),
                    *(
                        self._scopes(assignment.expression)
                        for assignment in aggregate.assignments
                        if assignment.expression is not None
                    ),
                )
                self._validate(relations, joined, reads, request)
            if operation.kind == "selected_rows" and operation.selected_rows is not None:
                selected = operation.selected_rows
                reads = set().union(
                    self._scopes(selected.order_by),
                    *(self._scopes(expression) for expression in selected.partition_by),
                )
                self._validate(relations, joined, reads, request)
            if operation.kind == "drop_duplicates" and operation.duplicate_rows is not None:
                duplicate = operation.duplicate_rows
                reads = set().union(*(self._scopes(expression) for expression in duplicate.subset))
                if not (duplicate.scope is not None and reads <= {duplicate.scope}):
                    self._validate(relations, joined, reads, request)

        reads = set().union(
            *(self._scopes(assignment.expression) for result in body.results for assignment in result.projection)
        )
        self._validate(relations, joined, reads, request)

    def _validate(
        self,
        relations: dict[str, str],
        joined: set[str],
        reads: set[str],
        request: StepAuthoringRequest,
    ) -> None:
        for scope, parameter in relations.items():
            if scope not in reads or scope in joined:
                continue
            origin = request.origin
            class_name = getattr(origin, "class_name", "Transform")
            member = getattr(origin, "member_name", request.name)
            raise StructureCompileError(
                Diagnostic(
                    entry=diagnostic_registry.get("JOIN-E0601"),
                    problem=f"{class_name}.{member} reads relation parameter {parameter} before it is joined.",
                    use=f"Use left_join({parameter}, on=...) or lookup_join({parameter}, on=...) before reading its fields.",
                    context={"input": parameter},
                    source=member,
                )
            )

    def _scopes(self, expression: Expression) -> set[str]:
        scopes = set().union(*(self._scopes(argument) for argument in expression.args))
        if expression.kind == "field" and expression.data and "scope" in expression.data:
            scopes.add(str(expression.data["scope"]))
        return scopes
