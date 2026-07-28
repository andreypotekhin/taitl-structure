from structure import StructureCompileError
from structure.lib.cross.errors import Diagnostic, diagnostic_registry
from structure.plugin.api.v1.model.StepAuthoringRequest import StepAuthoringRequest
from structure.plugin.pyspark.dsl.Expression import Expression
from structure.plugin.pyspark.dsl.joins import (
    AsOf,
    Join,
    JoinDedupe,
    JoinHint,
    JoinMethod,
    JoinStrategy,
    OverlapPolicy,
    TiePolicy,
)
from structure.plugin.pyspark.dsl.types import DecimalType, StructType, StructureType
from structure.plugin.pyspark.symbolic_execution.model.PySparkStepBody import PySparkStepBody


class ValidatePySparkJoins:
    """Validate PySpark join semantics from a captured symbolic body."""

    def __call__(self, body: PySparkStepBody, *, request: StepAuthoringRequest) -> tuple[Diagnostic, ...]:
        diagnostics: list[Diagnostic] = []
        for occurrence, join in enumerate(body.joins, start=1):
            self._shape(join, occurrence, request)
            if join.dedupe is not None:
                self._dedupe(join, occurrence, request)
            if join.temporal is not None:
                self._temporal(join, occurrence, request)
            if join.as_of is not None:
                self._as_of(join, occurrence, request)
            conditions = self._conditions(join, occurrence, request)
            for condition in conditions:
                self._pair(join, occurrence, condition.args[0], condition.args[1], request)
            if join.method is JoinMethod.LOOKUP and join.dedupe is None:
                diagnostics.append(
                    Diagnostic(
                        entry=diagnostic_registry.get("JOIN-W0601"),
                        problem=f"lookup_join(...) uniqueness is not proven for input {join.input_name}.",
                        use="Use JoinDedupe.latest_by(...) or JoinDedupe.earliest_by(...), or use left_join(...) or inner_join(...) when multiplication is intended.",
                        context={"input": join.input_name, "occurrence": str(occurrence)},
                        source=self._source(request),
                    )
                )
        return tuple(diagnostics)

    def _shape(self, join, occurrence: int, request: StepAuthoringRequest) -> None:
        if join.method in {JoinMethod.LOOKUP, JoinMethod.TEMPORAL_ONE, JoinMethod.AS_OF_ONE} and join.how not in {
            Join.LEFT,
            Join.INNER,
        }:
            self._error(
                join,
                occurrence,
                request,
                f'{join.method.value}(...) supports "left" and "inner", not {join.how!r}.',
                'Use "left" or "inner", or use rowset_join(...) for broad rowset joins.',
            )
        if join.method is JoinMethod.ROWSET and join.how not in {
            Join.LEFT,
            Join.INNER,
            Join.RIGHT,
            Join.FULL,
            Join.CROSS,
        }:
            self._error(
                join,
                occurrence,
                request,
                f"rowset_join(...) does not support join type {join.how!r}.",
                'Use "left", "inner", "right", "full", or "cross".',
            )
        if join.hint is not None and not isinstance(join.hint, JoinHint):
            self._error(
                join,
                occurrence,
                request,
                f"{join.method.value}(...) hint must be a JoinHint value, not {type(join.hint).__name__}.",
                'Use "broadcast" or omit hint=.',
            )
        if join.strategy is not None and not isinstance(join.strategy, JoinStrategy):
            self._error(
                join,
                occurrence,
                request,
                f"{join.method.value}(...) strategy must be a JoinStrategy value, not {type(join.strategy).__name__}.",
                'Use "broadcast", "shuffle_hash", "merge", "shuffle_replicate_nl", or omit strategy=.',
            )

    def _temporal(self, join, occurrence: int, request: StepAuthoringRequest) -> None:
        temporal = join.temporal
        assert temporal is not None
        if temporal.overlaps is not OverlapPolicy.ERROR:
            self._error(
                join,
                occurrence,
                request,
                f"temporal_one(overlaps=...) policy {temporal.overlaps!r} is not supported.",
                'Use "error" or omit overlaps=.',
            )
        if join.input_name in self._scopes(temporal.at):
            self._error(
                join,
                occurrence,
                request,
                "temporal_one(at=...) must not read the joined temporal input.",
                "Use a current-row event time such as order.order_time.",
            )
        for field, expression in (("valid_from", temporal.valid_from), ("valid_to", temporal.valid_to)):
            scopes = self._scopes(expression)
            if join.input_name not in scopes or scopes - {join.input_name}:
                self._error(
                    join,
                    occurrence,
                    request,
                    f"temporal_one({field}=...) must read only the joined temporal input.",
                    f"Use a right-side validity field such as history.{field}.",
                )

    def _as_of(self, join, occurrence: int, request: StepAuthoringRequest) -> None:
        as_of = join.as_of
        assert as_of is not None
        if as_of.direction not in {AsOf.BACKWARD, AsOf.FORWARD}:
            self._error(
                join,
                occurrence,
                request,
                f"as_of_one(direction=...) policy {as_of.direction!r} is not supported.",
                'Use "backward", "forward", or omit direction=.',
            )
        if as_of.ties is not TiePolicy.ERROR:
            self._error(
                join,
                occurrence,
                request,
                f"as_of_one(ties=...) policy {as_of.ties!r} is not supported.",
                'Use "error" or omit ties=.',
            )
        if join.input_name in self._scopes(as_of.left_time):
            self._error(
                join,
                occurrence,
                request,
                "as_of_one(left_time=...) must not read the joined as-of input.",
                "Use a current-row event time such as trade.trade_time.",
            )
        scopes = self._scopes(as_of.right_time)
        if join.input_name not in scopes or scopes - {join.input_name}:
            self._error(
                join,
                occurrence,
                request,
                "as_of_one(right_time=...) must read only the joined as-of input.",
                "Use a right-side event time such as prices.price_time.",
            )
        if as_of.tolerance is not None and join.input_name in self._scopes(as_of.tolerance):
            self._error(
                join,
                occurrence,
                request,
                "as_of_one(tolerance=...) must not read the joined as-of input.",
                "Use a literal or current-row tolerance expression.",
            )

    def _dedupe(self, join, occurrence: int, request: StepAuthoringRequest) -> None:
        dedupe: JoinDedupe = join.dedupe
        if not isinstance(dedupe.order_by, Expression):
            self._error(
                join,
                occurrence,
                request,
                "lookup_join(dedupe=...) order_by must be a Structure expression.",
                "Use JoinDedupe.latest_by(customer.updated_at) or JoinDedupe.earliest_by(customer.updated_at).",
            )
        if dedupe.order_by.kind == "order":
            self._error(
                join,
                occurrence,
                request,
                "lookup_join(dedupe=...) order_by must be an unordered expression; the policy selects the direction.",
                "Pass the field expression directly, such as JoinDedupe.latest_by(customer.updated_at).",
            )
        if not self._orderable(dedupe.order_by.type):
            self._error(
                join,
                occurrence,
                request,
                "lookup_join(dedupe=...) order_by must be an orderable scalar expression.",
                "Use a Date, Timestamp, String, or numeric right-side expression.",
            )
        if not isinstance(dedupe.ties, TiePolicy):
            self._error(
                join,
                occurrence,
                request,
                f"lookup_join(dedupe=...) ties must be a TiePolicy value, not {type(dedupe.ties).__name__}.",
                'Use "error" or omit ties=.',
            )
        if dedupe.direction not in {"latest", "earliest"}:
            self._error(
                join,
                occurrence,
                request,
                f"lookup_join(dedupe=...) direction {dedupe.direction!r} is not supported.",
                "Use JoinDedupe.latest_by(...) or JoinDedupe.earliest_by(...).",
            )
        scopes = self._scopes(dedupe.order_by)
        if join.input_name not in scopes or scopes - {join.input_name}:
            self._error(
                join,
                occurrence,
                request,
                "lookup_join(dedupe=...) order_by must read only the joined input.",
                "Use a right-side field such as JoinDedupe.latest_by(customer.updated_at).",
            )

    def _conditions(self, join, occurrence: int, request: StepAuthoringRequest) -> list[Expression]:
        predicate = join.predicate
        if join.method is JoinMethod.ROWSET:
            if predicate.kind == "literal":
                return []
            scopes = self._scopes(predicate)
            if join.input_name not in scopes:
                self._error(
                    join,
                    occurrence,
                    request,
                    "rowset_join(...) predicate must reference the joined input.",
                    "Compare the joined input with the current row or another joined scope.",
                )
            if not scopes - {join.input_name}:
                self._error(
                    join,
                    occurrence,
                    request,
                    "rowset_join(...) predicate cannot reference only the joined input.",
                    "Compare the joined input with the current row or another joined scope.",
                )
            return self._equalities(predicate)
        if join.method is JoinMethod.EXISTS:
            return self._exists(join, occurrence, predicate, request)
        return self._keys(join, occurrence, predicate, request)

    def _keys(self, join, occurrence, predicate, request) -> list[Expression]:
        if predicate.kind == "and":
            return [item for arg in predicate.args for item in self._keys(join, occurrence, arg, request)]
        if predicate.kind in {"eq", "null_safe_eq"}:
            return [predicate]
        self._error(
            join,
            occurrence,
            request,
            "v1 joins support equality key pairs combined with AND.",
            "Replace OR, inequality, or arbitrary predicates with equality pairs, or move custom join logic into a hook.",
        )
        return []

    def _exists(self, join, occurrence, predicate, request) -> list[Expression]:
        if predicate.kind == "and":
            return [item for arg in predicate.args for item in self._exists(join, occurrence, arg, request)]
        if predicate.kind in {"eq", "null_safe_eq"}:
            return [predicate]
        if predicate.kind == "event_time_between" and join.input_name in self._scopes(predicate.args[0]) | self._scopes(
            predicate.args[1]
        ):
            return []
        self._error(
            join,
            occurrence,
            request,
            "exists(...) supports equality key pairs and event_time_between(...) constraints combined with AND.",
            "Use equality key pairs and, for a bounded stream-stream semi join, event_time_between(left_time, right_time, upper=...).",
        )
        return []

    def _equalities(self, predicate: Expression) -> list[Expression]:
        if predicate.kind == "and":
            return [item for arg in predicate.args for item in self._equalities(arg)]
        return [predicate] if predicate.kind in {"eq", "null_safe_eq"} else []

    def _pair(self, join, occurrence, left: Expression, right: Expression, request) -> None:
        left_scopes, right_scopes = self._scopes(left), self._scopes(right)
        left_input, right_input = join.input_name in left_scopes, join.input_name in right_scopes
        if left_input == right_input:
            self._error(
                join,
                occurrence,
                request,
                "Each join key pair must compare the joined input with the current row or an earlier joined scope.",
                "Put one joined-input expression on one side of == and one non-joined expression on the other side.",
            )
        if not (left_scopes | right_scopes) - {join.input_name}:
            self._error(
                join,
                occurrence,
                request,
                "Join key pairs cannot compare only fields from the joined input.",
                "Compare the joined input key to the current row or a previously joined scope.",
            )
        if not self._compatible(left.type, right.type):
            self._error(
                join,
                occurrence,
                request,
                f"Join key types are incompatible: {self._type(left.type)} and {self._type(right.type)}.",
                "Join fields with compatible types or use explicit expression helpers before comparing keys.",
            )

    def _scopes(self, expression: Expression) -> set[str]:
        scopes = set().union(*(self._scopes(arg) for arg in expression.args))
        if expression.kind == "field" and expression.data and "scope" in expression.data:
            scopes.add(str(expression.data["scope"]))
        return scopes

    def _compatible(self, left: StructureType | None, right: StructureType | None) -> bool:
        return (
            left is not None and right is not None and (self._assignable(left, right) or self._assignable(right, left))
        )

    def _assignable(self, actual: StructureType, target: StructureType) -> bool:
        if self._same_type(actual, target):
            return True
        if target.name == "long" and actual.name == "integer":
            return True
        if target.name == "double" and actual.name in {"integer", "long", "float"}:
            return True
        if isinstance(target, DecimalType):
            digits = target.precision - target.scale
            if actual.name == "integer":
                return digits >= 10
            if actual.name == "long":
                return digits >= 19
            if isinstance(actual, DecimalType):
                return target.scale >= actual.scale and digits >= actual.precision - actual.scale
        return False

    def _same_type(self, actual: StructureType, target: StructureType) -> bool:
        if actual.name != target.name:
            return False
        if isinstance(actual, DecimalType) and isinstance(target, DecimalType):
            return actual.precision == target.precision and actual.scale == target.scale
        if isinstance(actual, StructType) and isinstance(target, StructType):
            return actual.schema is target.schema
        return actual == target or actual.__class__.__name__.removesuffix("Type") == target.__class__.__name__

    def _orderable(self, type: StructureType | None) -> bool:
        return type is not None and type.name in {
            "date",
            "decimal",
            "double",
            "float",
            "integer",
            "long",
            "string",
            "timestamp",
        }

    def _type(self, type: StructureType | None) -> str:
        if type is None:
            return "untyped null"
        if isinstance(type, DecimalType):
            return f"Decimal({type.precision}, {type.scale})"
        if isinstance(type, StructType):
            return f"Struct({type.schema.__name__})"
        return f"{type.name}()"

    def _source(self, request: StepAuthoringRequest) -> str:
        origin = request.origin
        return f"{getattr(origin, 'module', '')}.{getattr(origin, 'class_name', 'Transform')}.{getattr(origin, 'member_name', request.name)}".lstrip(
            "."
        )

    def _error(self, join, occurrence: int, request: StepAuthoringRequest, problem: str, use: str) -> None:
        raise StructureCompileError(
            Diagnostic(
                entry=diagnostic_registry.get("JOIN-E0601"),
                problem=problem,
                use=use,
                context={"input": join.input_name, "occurrence": str(occurrence)},
                source=self._source(request),
            )
        )
