import sys

from structure.app.compiler.api import compiler
from structure.app.dsl.api import compile_transform
from structure.app.target.pyspark.api import pyspark


def _traceability():
    from testing.model.v1.orders.transforms.order import EnrichOrders

    return compiler.traceability.build()(
        pyspark.plan.lower()(compile_transform(EnrichOrders)),
        source_transform="testing.model.v1.orders.transforms.order.EnrichOrders",
        transform_module="testing.model.v1.structure_generated.orders.pyspark.transforms.order",
    )


def test_v1_compiler_traceability_is_spark_free_and_deterministic() -> None:
    before = {name for name in sys.modules if name.startswith("pyspark")}

    first = _traceability().to_dict()
    second = _traceability().to_dict()

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert first == second


def test_v1_compiler_traceability_maps_source_ir_and_generated_nodes() -> None:
    traceability = _traceability()
    records = {(record.source, record.ir, record.generated) for record in traceability.provenance}

    assert (
        "source:testing.model.v1.orders.transforms.order.EnrichOrders.add_customer",
        "ir:EnrichOrders.step.1.add_customer",
        "generated:testing.model.v1.structure_generated.orders.pyspark.transforms.order."
        "EnrichOrdersGenerated.run.step.1.add_customer",
    ) in records
    assert any(
        record.ir == "ir:EnrichOrders.step.1.add_customer.join.1.customers" for record in traceability.provenance
    )
    assert any(
        record.generated.endswith("EnrichOrdersGenerated.run.step.4.publish.select.has_promotion")
        for record in traceability.provenance
    )


def test_v1_compiler_traceability_reports_static_dataflow_and_opaque_hooks() -> None:
    traceability = _traceability()
    dependencies = {dependency.target: dependency for dependency in traceability.static_dataflow}

    assert dependencies["EnrichOrders"].sources == ("orders", "customers", "products", "promotions")
    assert dependencies["add_customer.join[1].customers"].operation == "join_one"
    assert dependencies["add_customer.join[1].customers"].detail["how"] == "left"
    assert dependencies["OrderNormalized.id"].sources
    assert dependencies["OrderWithCustomer.customer_name"].sources

    boundaries = {
        (boundary.step, boundary.hook, boundary.phase, boundary.reason) for boundary in traceability.opaque_boundaries
    }
    assert ("add_promotion", "note_lookup_inputs", "after", "arbitrary PySpark hook body") in boundaries
    assert ("publish", "add_quality_columns", "after", "arbitrary PySpark hook body") in boundaries
