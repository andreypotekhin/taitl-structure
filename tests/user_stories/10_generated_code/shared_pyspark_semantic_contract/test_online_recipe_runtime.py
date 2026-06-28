import sys
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import Any, cast

import pytest

from structure import Join, JoinHint, SchemaMode, String, Structure, field
from structure.app.runtime.execution.online.commands.RunOnlinePySparkTransform import RunOnlinePySparkTransform
from structure.app.runtime.execution.online.logic.PySparkExpressionEvaluator import PySparkExpressionEvaluator
from structure.app.runtime.execution.online.logic.PySparkFrameValidator import PySparkFrameValidator
from structure.app.runtime.session.model.TransformResult import TransformResult
from structure.app.target.capabilities.model.BackendId import BackendId
from structure.app.target.pyspark.model.PySparkExecutionPlan import PySparkExecutionPlan
from structure.app.target.pyspark.model.PySparkExpressionRecipe import PySparkExpressionRecipe
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe
from structure.app.target.pyspark.model.PySparkInputRecipe import PySparkInputRecipe
from structure.app.target.pyspark.model.PySparkJoinRecipe import PySparkJoinRecipe
from structure.app.target.pyspark.model.PySparkOutputRecipe import PySparkOutputRecipe
from structure.app.target.pyspark.model.PySparkProjectionRecipe import PySparkProjectionRecipe
from structure.app.target.pyspark.model.PySparkStepRecipe import PySparkStepRecipe
from structure.app.target.pyspark.model.PySparkStepResultRecipe import PySparkStepResultRecipe
from structure.app.target.pyspark.model.PySparkValidationRecipe import PySparkValidationRecipe


class RawOrder(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)


class PublishedOrder(Structure):
    id = field(String(), nullable=False)
    status = field(String(), nullable=True)


class Customer(Structure):
    id = field(String(), nullable=False)
    segment = field(String(), nullable=True)


class PublishedOrderId(Structure):
    id = field(String(), nullable=False)


class PublishedOrderStatus(Structure):
    status = field(String(), nullable=True)


class PermissivePublishedOrder(PublishedOrder):
    pass


def test_online_expression_evaluator_preserves_pyspark_column_semantics() -> None:
    """I can rely on online execution and generated execution to consume the same PySpark semantic contract."""

    evaluator = PySparkExpressionEvaluator()
    functions = FakeFunctions("functions")
    aliases = {RawOrder.__name__: "orders"}

    cases = [
        (_is_null(_field(RawOrder, "status")), "col(orders.status).isNull()"),
        (
            _binary("and", _not_null(_field(RawOrder, "id")), _is_null(_field(RawOrder, "status"))),
            "(col(orders.id).isNotNull() AND col(orders.status).isNull())",
        ),
        (
            _binary("or", _is_null(_field(RawOrder, "status")), _not_null(_field(RawOrder, "id"))),
            "(col(orders.status).isNull() OR col(orders.id).isNotNull())",
        ),
        (_binary("eq", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) == lit('A-1'))"),
        (_binary("ne", _field(RawOrder, "status"), _literal("cancelled")), "(col(orders.status) != lit('cancelled'))"),
        (_binary("gt", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) > lit('A-1'))"),
        (_binary("sub", _field(RawOrder, "id"), _literal("A-1")), "(col(orders.id) - lit('A-1'))"),
        (
            _binary("null_safe_eq", _field(RawOrder, "status"), _literal(None)),
            "col(orders.status).eqNullSafe(lit(None))",
        ),
        (_not(_is_null(_field(RawOrder, "status"))), "~(col(orders.status).isNull())"),
        (_to_decimal(_field(RawOrder, "status"), precision=12, scale=2), "cast(col(orders.status) as decimal(12,2))"),
    ]

    assert [evaluator.evaluate(recipe, functions=functions, aliases=aliases).expression for recipe, _ in cases] == [
        expected for _, expected in cases
    ]


def test_online_runner_executes_lowered_pyspark_recipe(monkeypatch) -> None:
    """I can rely on online execution and generated execution to consume the same PySpark semantic contract."""

    functions = FakeFunctions("pyspark.sql.functions")
    _install_fake_pyspark(monkeypatch, functions)

    source = FakeFrame(
        "source",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField("status", FakeTypes.StringType(), True),
            )
        ),
    )
    result = RunOnlinePySparkTransform()(
        cast(Any, FakeInvocation(orders=source)),
        _online_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark=object(),
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert published.field_names == ["id", "status"]
    assert published.operations == (
        "alias:orders",
        "where:col(orders.status).isNotNull()",
        "select:id=cast(col(orders.id) as StringType()),"
        "status=cast(lower(trim(col(orders.status))) as StringType())",
        "alias:published",
    )


def test_online_runner_preserves_injected_multi_output_result_contract() -> None:
    """I can call run(session) on a transform invocation so StructureSession chooses the runtime runner."""

    result = TransformResult({"ids": "ids-frame", "statuses": "statuses-frame"})
    invocation = FakeInvocation(orders="orders-frame")
    session = SimpleNamespace(
        online_executor=lambda **_: result,
        spark=None,
        ctx=None,
        execution_mode="online",
        target_backend="pyspark",
    )

    assert RunOnlinePySparkTransform()(cast(Any, invocation), _multi_result_plan(), session=session) is result

    session.online_executor = lambda **_: "one-frame"
    with pytest.raises(TypeError, match="TransformResult for multi-output"):
        RunOnlinePySparkTransform()(cast(Any, invocation), _multi_result_plan(), session=session)


def test_online_runner_applies_step_hooks_and_step_and_output_joins(monkeypatch) -> None:
    """I can rely on online execution and generated execution to preserve the same transform semantics."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(
        orders=_frame("orders", RawOrder),
        customers=_frame("customers", Customer),
    )

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _join_and_hook_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx={"run": "online"},
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    published = cast(FakeFrame, result.published)

    assert invocation.hook_calls == [
        ("prepare_orders", "orders", "spark", {"run": "online"}),
        ("record_published", "orders", "spark", {"run": "online"}),
    ]
    assert published.operations == (
        "before-hook",
        "alias:orders",
        "join:customers:left:col(orders.id).eqNullSafe(col(customers.id))",
        "select:id=cast(col(orders.id) as StringType()),"
        "status=cast(coalesce(col(customers.segment),col(orders.status)) as StringType())",
        "after-hook",
        "select:id=cast(col(id) as StringType()),status=cast(col(status) as StringType())",
        "alias:published",
        "join:customers:inner:(col(published.id) == col(customers.id))",
        "where:col(published.id).isNotNull()",
        "select:id=cast(col(published.id) as StringType()),status=cast(col(published.status) as StringType())",
    )


def test_online_runner_materializes_multiple_step_results(monkeypatch) -> None:
    """I can run online/generated parity tests for every supported compiled operation."""

    _install_fake_pyspark(monkeypatch, FakeFunctions("pyspark.sql.functions"))
    invocation = FakeInvocation(orders=_frame("orders", RawOrder))

    result = RunOnlinePySparkTransform()(
        cast(Any, invocation),
        _multi_result_plan(),
        session=SimpleNamespace(
            online_executor=None,
            spark="spark",
            ctx=None,
            execution_mode="online",
            target_backend="pyspark",
        ),
    )

    ids = cast(FakeFrame, result.ids)
    statuses = cast(FakeFrame, result.statuses)

    assert invocation.hook_calls == [("record_ids", "orders", "spark", None)]
    assert ids.field_names == ["id"]
    assert ids.operations == (
        "alias:orders",
        "select:id=cast(col(orders.id) as StringType())",
        "ids-hook",
        "select:id=cast(col(id) as StringType())",
        "alias:ids",
    )
    assert statuses.field_names == ["status"]
    assert statuses.operations == (
        "alias:orders",
        "select:status=cast(col(orders.status) as StringType())",
        "alias:statuses",
    )


def test_online_schema_validation_projects_equivalent_spark_shapes() -> None:
    """Online execution exposes equivalent Spark schemas."""

    validator = PySparkFrameValidator()
    frame = FakeFrame(
        "published",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField("status", FakeTypes.StringType(), True),
                FakeField("debug", FakeTypes.StringType(), True),
            )
        ),
    )
    validation = PySparkValidationRecipe(
        target="published",
        schema=PermissivePublishedOrder,
        mode=SchemaMode.ALLOW_EXTRA_COLUMNS,
        project=True,
        reason="hook",
    )

    validator.validate(frame, validation, types=FakeTypes)
    projected = validator.project(frame, validation, types=FakeTypes, functions=FakeFunctions("functions"))

    assert projected.field_names == ["id", "status"]
    assert projected.operations == ("select:id=cast(col(id) as StringType()),status=cast(col(status) as StringType())",)


def test_online_schema_validation_rejects_strict_shape_drift() -> None:
    """I can rely on online execution and generated execution to preserve the same transform semantics."""

    validation = PySparkValidationRecipe(
        target="published",
        schema=PublishedOrder,
        mode=SchemaMode.STRICT,
        project=False,
        reason="output",
    )
    frame = FakeFrame(
        "published",
        FakeSchema(
            (
                FakeField("id", FakeTypes.StringType(), False),
                FakeField("status", FakeTypes.StringType(), True),
                FakeField("debug", FakeTypes.StringType(), True),
            )
        ),
    )

    with pytest.raises(ValueError, match="unexpected column\\(s\\): debug"):
        PySparkFrameValidator().validate(frame, validation, types=FakeTypes)


def _online_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe("published", PublishedOrder, SchemaMode.STRICT, False, "output")
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _call("lower", _call("trim", _field(RawOrder, "status"))),
        ),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(_not_null(_field(RawOrder, "status")),),
        joins=(),
        projection=projection,
        after_hooks=(),
        validations=(published_validation,),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("orders", RawOrder, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                filters=(),
                joins=(),
                projection=(),
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _join_and_hook_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, SchemaMode.STRICT, False, "input")
    customer_validation = PySparkValidationRecipe("customers", Customer, SchemaMode.STRICT, False, "input")
    published_validation = PySparkValidationRecipe("published", PublishedOrder, SchemaMode.STRICT, False, "output")
    projected_validation = PySparkValidationRecipe(
        "published", PublishedOrder, SchemaMode.STRICT, True, "hook_projected"
    )
    projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(RawOrder, "id")),
        PySparkProjectionRecipe(
            PublishedOrder._structure_fields["status"],
            _call("coalesce", _field_scope("customers", Customer, "segment"), _field(RawOrder, "status")),
        ),
    )
    output_projection = (
        PySparkProjectionRecipe(PublishedOrder._structure_fields["id"], _field(PublishedOrder, "id")),
        PySparkProjectionRecipe(PublishedOrder._structure_fields["status"], _field(PublishedOrder, "status")),
    )
    step = PySparkStepRecipe(
        name="publish",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(_hook("prepare_orders", lanes=("orders",), outputs=("orders",)),),
        filters=(),
        joins=(
            PySparkJoinRecipe(
                input_name="customers",
                source="customers",
                input_schema=Customer,
                left_alias="orders",
                right_alias="customers",
                how=Join.LEFT,
                hint=JoinHint.BROADCAST,
                predicate=_binary("null_safe_eq", _field(RawOrder, "id"), _field_scope("customers", Customer, "id")),
                occurrence=0,
            ),
        ),
        projection=projection,
        after_hooks=(_hook("record_published", lanes=("published",), outputs=("published",)),),
        validations=(projected_validation,),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrder,
                lane="published",
                frame="published",
                output_alias="published",
                projection=projection,
                ordinal=0,
                after_hooks=(),
                validations=(published_validation,),
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="PublishOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(
            PySparkInputRecipe("orders", RawOrder, 0, input_validation),
            PySparkInputRecipe("customers", Customer, 1, customer_validation),
        ),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="published",
                ordinal=0,
                source="published",
                source_scope="published",
                input_schema=PublishedOrder,
                output_schema=PublishedOrder,
                input_alias="published",
                output_alias="published",
                joins=(
                    PySparkJoinRecipe(
                        input_name="customers",
                        source="customers",
                        input_schema=Customer,
                        left_alias="published",
                        right_alias="customers",
                        how=Join.INNER,
                        hint=JoinHint.BROADCAST,
                        predicate=_binary(
                            "eq", _field(PublishedOrder, "id"), _field_scope("customers", Customer, "id")
                        ),
                        occurrence=0,
                    ),
                ),
                filters=(_not_null(_field(PublishedOrder, "id")),),
                projection=output_projection,
                validation=published_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _multi_result_plan() -> PySparkExecutionPlan:
    input_validation = PySparkValidationRecipe("orders", RawOrder, SchemaMode.STRICT, False, "input")
    id_validation = PySparkValidationRecipe("ids", PublishedOrderId, SchemaMode.STRICT, True, "output")
    status_validation = PySparkValidationRecipe("statuses", PublishedOrderStatus, SchemaMode.STRICT, False, "output")
    id_projection = (PySparkProjectionRecipe(PublishedOrderId._structure_fields["id"], _field(RawOrder, "id")),)
    status_projection = (
        PySparkProjectionRecipe(PublishedOrderStatus._structure_fields["status"], _field(RawOrder, "status")),
    )
    step = PySparkStepRecipe(
        name="split",
        ordinal=0,
        source="orders",
        source_scope="orders",
        input_schema=RawOrder,
        output_schema=PublishedOrder,
        input_alias="orders",
        output_alias="published",
        before_hooks=(),
        filters=(),
        joins=(),
        projection=(),
        after_hooks=(),
        validations=(),
        results=(
            PySparkStepResultRecipe(
                schema=PublishedOrderId,
                lane="ids",
                frame="ids",
                output_alias="ids",
                projection=id_projection,
                ordinal=0,
                after_hooks=(_hook("record_ids", lanes=("ids",), outputs=("ids",)),),
                validations=(id_validation,),
            ),
            PySparkStepResultRecipe(
                schema=PublishedOrderStatus,
                lane="statuses",
                frame="statuses",
                output_alias="statuses",
                projection=status_projection,
                ordinal=1,
                after_hooks=(),
                validations=(status_validation,),
            ),
        ),
    )
    return PySparkExecutionPlan(
        transform="SplitOrders",
        backend=BackendId("PySpark", "3.5", "pyspark"),
        inputs=(PySparkInputRecipe("orders", RawOrder, 0, input_validation),),
        steps=(step,),
        outputs=(
            PySparkOutputRecipe(
                name="ids",
                ordinal=0,
                source="ids",
                source_scope="ids",
                input_schema=PublishedOrderId,
                output_schema=PublishedOrderId,
                input_alias="ids",
                output_alias="ids",
                filters=(),
                joins=(),
                projection=(),
                validation=id_validation,
            ),
            PySparkOutputRecipe(
                name="statuses",
                ordinal=1,
                source="statuses",
                source_scope="statuses",
                input_schema=PublishedOrderStatus,
                output_schema=PublishedOrderStatus,
                input_alias="statuses",
                output_alias="statuses",
                filters=(),
                joins=(),
                projection=(),
                validation=status_validation,
            ),
        ),
        requires_hook_inputs=False,
    )


def _field(schema: type[Structure], name: str) -> PySparkExpressionRecipe:
    return _field_scope(schema.__name__, schema, name)


def _field_scope(scope: str, schema: type[Structure], name: str) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        kind="field",
        type=schema._structure_fields[name].type,
        nullable=schema._structure_fields[name].nullable,
        data={"scope": scope, "field": name},
    )


def _call(function: str, *args: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("call", args[0].type, args[0].nullable, {"function": function}, args)


def _to_decimal(expression: PySparkExpressionRecipe, *, precision: int, scale: int) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(
        "call",
        expression.type,
        expression.nullable,
        {"function": "to_decimal", "precision": precision, "scale": scale},
        (expression,),
    )


def _literal(value) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("literal", None, value is None, {"value": value})


def _not_null(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("is_not_null", None, False, {}, (expression,))


def _is_null(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("is_null", None, False, {}, (expression,))


def _not(expression: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe("not", None, False, {}, (expression,))


def _binary(kind: str, left: PySparkExpressionRecipe, right: PySparkExpressionRecipe) -> PySparkExpressionRecipe:
    return PySparkExpressionRecipe(kind, left.type, False, {}, (left, right))


def _hook(
    name: str,
    *,
    lanes: tuple[str, ...],
    outputs: tuple[str, ...],
) -> PySparkHookRecipe:
    return PySparkHookRecipe(
        name=name,
        phase="after",
        target=lanes[0],
        lanes=lanes,
        outputs=outputs,
        pass_inputs=False,
        schema_mode=SchemaMode.STRICT,
        project_output=False,
        streaming_safe=True,
    )


def _frame(name: str, schema: type[Structure]) -> "FakeFrame":
    return FakeFrame(
        name,
        FakeSchema(
            tuple(
                FakeField(schema_field.column, _type(schema_field.type), schema_field.nullable)
                for schema_field in schema._structure_fields.values()
            )
        ),
    )


def _type(value):
    if value.__class__.__name__ == "String":
        return FakeTypes.StringType()
    return FakeTypes.StringType()


def _install_fake_pyspark(monkeypatch, functions: ModuleType) -> None:
    pyspark = ModuleType("pyspark")
    sql = ModuleType("pyspark.sql")
    types = ModuleType("pyspark.sql.types")
    for name in (
        "StructType",
        "StructField",
        "StringType",
        "IntegerType",
        "LongType",
        "FloatType",
        "DoubleType",
        "BooleanType",
        "DateType",
        "TimestampType",
        "DecimalType",
        "ArrayType",
        "MapType",
    ):
        setattr(types, name, getattr(FakeTypes, name))

    setattr(pyspark, "sql", sql)
    setattr(sql, "functions", functions)
    setattr(sql, "types", types)
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types)


class FakeInvocation:

    def __init__(self, **inputs) -> None:
        self._structure_bound_inputs = inputs
        self.hook_calls: list[tuple[str, str, object, object]] = []

    def prepare_orders(self, *, orders, spark, ctx):
        self.hook_calls.append(("prepare_orders", orders.name, spark, ctx))
        return orders.with_operation("before-hook")

    def record_published(self, *, published, spark, ctx):
        self.hook_calls.append(("record_published", published.name, spark, ctx))
        return published.with_operation("after-hook")

    def record_ids(self, *, ids, spark, ctx):
        self.hook_calls.append(("record_ids", ids.name, spark, ctx))
        return ids.with_operation("ids-hook")


class FakeFunctions(ModuleType):

    def col(self, name: str):
        return FakeColumn(f"col({name})", source_name=name.rsplit(".", 1)[-1])

    def lit(self, value):
        return FakeColumn(f"lit({value!r})")

    def lower(self, column):
        return FakeColumn(f"lower({column.expression})", source_name=column.source_name)

    def trim(self, column):
        return FakeColumn(f"trim({column.expression})", source_name=column.source_name)

    def coalesce(self, *columns):
        return FakeColumn("coalesce(" + ",".join(column.expression for column in columns) + ")")

    def broadcast(self, frame):
        return frame.with_operation("broadcast")


@dataclass(frozen=True)
class FakeColumn:
    expression: str
    source_name: str | None = None
    output_name: str | None = None

    def alias(self, name: str):
        return FakeColumn(self.expression, self.source_name, name)

    def cast(self, target: str):
        return FakeColumn(f"cast({self.expression} as {target})", self.source_name)

    def isNotNull(self):
        return FakeColumn(f"{self.expression}.isNotNull()")

    def isNull(self):
        return FakeColumn(f"{self.expression}.isNull()")

    def eqNullSafe(self, other):
        return FakeColumn(f"{self.expression}.eqNullSafe({other.expression})")

    def __and__(self, other):
        return FakeColumn(f"({self.expression} AND {other.expression})")

    def __or__(self, other):
        return FakeColumn(f"({self.expression} OR {other.expression})")

    def __eq__(self, other):
        return FakeColumn(f"({self.expression} == {other.expression})")

    def __ne__(self, other):
        return FakeColumn(f"({self.expression} != {other.expression})")

    def __gt__(self, other):
        return FakeColumn(f"({self.expression} > {other.expression})")

    def __sub__(self, other):
        return FakeColumn(f"({self.expression} - {other.expression})", self.source_name)

    def __invert__(self):
        return FakeColumn(f"~({self.expression})")


@dataclass(frozen=True)
class FakeFrame:
    name: str
    schema: "FakeSchema"
    operations: tuple[str, ...] = ()

    @property
    def field_names(self) -> list[str]:
        return self.schema.fieldNames()

    def alias(self, name: str):
        return FakeFrame(name, self.schema, self.operations + (f"alias:{name}",))

    def where(self, predicate: FakeColumn):
        return self.with_operation(f"where:{predicate.expression}")

    def join(self, right, predicate: FakeColumn, how: str):
        return self.with_operation(f"join:{right.name}:{how}:{predicate.expression}")

    def select(self, *columns: FakeColumn):
        fields_by_name = {schema_field.name: schema_field for schema_field in self.schema}
        fields = []
        rendered = []
        for column in columns:
            name = column.output_name or column.source_name or column.expression
            source = fields_by_name.get(column.source_name or name)
            fields.append(
                FakeField(
                    name, source.dataType if source else FakeTypes.StringType(), source.nullable if source else True
                )
            )
            rendered.append(f"{name}={column.expression}")
        return FakeFrame(self.name, FakeSchema(tuple(fields)), self.operations + ("select:" + ",".join(rendered),))

    def with_operation(self, operation: str):
        return FakeFrame(self.name, self.schema, self.operations + (operation,))


@dataclass(frozen=True)
class FakeSchema:
    fields: tuple["FakeField", ...]

    def fieldNames(self) -> list[str]:
        return [schema_field.name for schema_field in self.fields]

    def __iter__(self):
        return iter(self.fields)

    def __getitem__(self, name: str):
        for schema_field in self.fields:
            if schema_field.name == name:
                return schema_field
        raise KeyError(name)


@dataclass(frozen=True)
class FakeField:
    name: str
    dataType: "FakeType"
    nullable: bool


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()

    def __str__(self) -> str:
        if not self.args:
            return f"{self.name}()"
        return f"{self.name}({', '.join(str(arg) for arg in self.args)})"


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return FakeSchema(tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeField(name, dataType, nullable)

    @staticmethod
    def StringType():
        return FakeType("StringType")

    @staticmethod
    def IntegerType():
        return FakeType("IntegerType")

    @staticmethod
    def LongType():
        return FakeType("LongType")

    @staticmethod
    def FloatType():
        return FakeType("FloatType")

    @staticmethod
    def DoubleType():
        return FakeType("DoubleType")

    @staticmethod
    def BooleanType():
        return FakeType("BooleanType")

    @staticmethod
    def DateType():
        return FakeType("DateType")

    @staticmethod
    def TimestampType():
        return FakeType("TimestampType")

    @staticmethod
    def DecimalType(precision, scale):
        return FakeType("DecimalType", (precision, scale))

    @staticmethod
    def ArrayType(element, *, containsNull):
        return FakeType("ArrayType", (element, containsNull))

    @staticmethod
    def MapType(key, value, *, valueContainsNull):
        return FakeType("MapType", (key, value, valueContainsNull))
