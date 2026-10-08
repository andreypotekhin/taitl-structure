from typing import Any, cast

from structure import Schema, Transform, output, variable
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import (
    delta_detail,
    delta_history,
    delta_input,
    delta_optimize,
    delta_restore,
    delta_table,
    delta_vacuum,
    long,
    string,
)


class Order(Schema):
    id = string(nullable=False)
    order_date = string(nullable=False)


class Commit(Schema):
    version = long()
    operation = string()


class Detail(Schema):
    location = string()


class Inspect(Transform):
    orders = delta_input(Order)
    limit = variable(int, default=5)
    commits = output(Commit)

    def history(self, order: Order) -> Commit:
        return delta_history(order, limit=self.limit)


class InspectDetail(Transform):
    orders = delta_input(Order)
    details = output(Detail)

    def detail(self, order: Order) -> Detail:
        return delta_detail(order)


class Restore(Transform):
    orders = delta_table(Order)
    version = variable(int)

    def restore(self, order: Order) -> Order:
        return delta_restore(order, version=self.version).execute()


class Maintain(Transform):
    orders = delta_table(Order)
    selected_date = variable(str)

    def compact(self, order: Order) -> None:
        delta_optimize(order).execute_compaction()

    def zorder(self, order: Order) -> None:
        delta_optimize(order, where=order.order_date == self.selected_date).execute_zorder(by=(order.id,))

    def recluster(self, order: Order) -> None:
        delta_optimize(order).full()

    def vacuum(self, order: Order) -> None:
        delta_vacuum(order).execute()


def test_delta_inspection_and_maintenance_are_compiler_visible() -> None:
    compile_transform = Compiler.frontend.compile()
    inspection = cast(Any, compile_transform(Inspect, materialize_schemas=False, plugin={"pyspark": {}}).lowered)
    detail = cast(Any, compile_transform(InspectDetail, materialize_schemas=False, plugin={"pyspark": {}}).lowered)
    restore = cast(Any, compile_transform(Restore, materialize_schemas=False, plugin={"pyspark": {}}).lowered)
    maintenance = cast(Any, compile_transform(Maintain, materialize_schemas=False, plugin={"pyspark": {}}).lowered)

    assert [step.delta_mutations[0].kind for step in inspection.steps] == ["delta_history"]
    assert detail.steps[0].delta_mutations[0].kind == "delta_detail"
    assert restore.steps[0].delta_mutations[0].kind == "restore"
    assert [step.delta_mutations[0].kind for step in maintenance.steps] == [
        "optimize",
        "optimize",
        "optimize",
        "vacuum",
    ]
    assert [step.delta_mutations[0].action for step in maintenance.steps[:2]] == ["compaction", "zorder"]
    recluster = maintenance.steps[2].delta_mutations[0]
    assert recluster.kind == "optimize"
    assert recluster.action == "full"
    assert maintenance.steps[2].effect
    assert maintenance.steps[1].delta_mutations[0].columns == ("id",)
    assert maintenance.steps[1].delta_mutations[0].predicate.args[1].kind == "variable"
    assert maintenance.steps[1].effect
