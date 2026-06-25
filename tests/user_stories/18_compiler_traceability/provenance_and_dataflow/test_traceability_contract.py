import sys


def test_compiler_traceability_is_spark_free_and_deterministic(orders_traceability) -> None:
    """I can inspect compiler provenance."""

    before = {name for name in sys.modules if name.startswith("pyspark")}

    first = orders_traceability.to_dict()
    second = orders_traceability.to_dict()

    assert first == second
    assert {name for name in sys.modules if name.startswith("pyspark")} == before


def test_traceability_maps_source_ir_and_generated_nodes(orders_traceability) -> None:
    """I can trace a source node to its IR node and generated PySpark node."""

    records = {(record.source, record.ir, record.generated) for record in orders_traceability.provenance}

    assert (
        "source:testing.model.v1.orders.transforms.order.EnrichOrders.add_customer",
        "ir:EnrichOrders.step.1.add_customer",
        "generated:testing.model.v1.structure_generated.orders.pyspark.transforms.order."
        "EnrichOrdersGenerated.run.step.1.add_customer",
    ) in records
    assert any(
        record.ir == "ir:EnrichOrders.step.1.add_customer.join.1.customers" for record in orders_traceability.provenance
    )


def test_traceability_reports_static_dataflow_and_opaque_hook_boundaries(orders_traceability) -> None:
    """I can identify opaque hook boundaries in traceability reports."""

    dependencies = {dependency.target: dependency for dependency in orders_traceability.static_dataflow}
    boundaries = {
        (boundary.step, boundary.hook, boundary.phase, boundary.reason)
        for boundary in orders_traceability.opaque_boundaries
    }

    assert dependencies["EnrichOrders"].sources == ("orders", "customers", "products", "promotions")
    assert dependencies["add_customer.join[1].customers"].operation == "join_one"
    assert dependencies["OrderWithCustomer.customer_name"].sources
    assert ("add_promotion", "note_lookup_inputs", "after", "arbitrary PySpark hook body") in boundaries
    assert ("publish", "add_quality_columns", "after", "arbitrary PySpark hook body") in boundaries
