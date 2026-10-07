from __future__ import annotations

from structure import Transform
from structure.core.runtime.session.model.StructureSession import StructureSession
from structure.core.sources.model.SourceTransformAddress import SourceTransformAddress
from structure.plugin.pyspark.execution.logic.PlanBoundary import apply_plan_boundary, transfer_plan_boundaries


class Catalog:
    def __init__(self) -> None:
        self.views: set[str] = set()

    def dropTempView(self, name: str) -> bool:
        self.views.discard(name)
        return True


class Spark:
    def __init__(self) -> None:
        self.catalog = Catalog()
        self.stopped = False

    def table(self, name: str) -> str:
        return name

    def stop(self) -> None:
        self.stopped = True


class Frame:
    def __init__(self, spark: Spark, name: str) -> None:
        self.spark = spark
        self.name = name

    def createOrReplaceTempView(self, name: str) -> None:
        self.spark.catalog.views.add(name)


def test_spawn_copies_configuration_and_source_registrations() -> None:
    spark = Spark()
    parent = StructureSession(runtime=spark)
    address = SourceTransformAddress(module="tests.example", qualname="Example")
    parent._source_transforms[address] = [Transform]

    child = StructureSession(parent)
    spawned = parent.spawn()

    assert child.runtime is spark
    assert child.spark is parent.spark
    assert child.config is parent.config
    assert child.artifacts is parent.artifacts
    assert child._source_transforms == parent._source_transforms
    assert child._source_transforms is not parent._source_transforms
    assert child._source_transforms[address] is not parent._source_transforms[address]
    assert spawned.config is parent.config
    child.close()
    spawned.close()
    parent.close()
    assert not spark.stopped


def test_closing_child_drops_only_its_plan_boundary_views() -> None:
    spark = Spark()
    parent = StructureSession(runtime=spark)
    child = parent.spawn()

    parent_view = apply_plan_boundary(Frame(spark, "parent"), spark, owner=parent)
    child_view = apply_plan_boundary(Frame(spark, "child"), spark, owner=child)

    assert parent_view in spark.catalog.views
    assert child_view in spark.catalog.views
    child.close()

    assert parent_view in spark.catalog.views
    assert child_view not in spark.catalog.views
    parent.close()
    assert not spark.catalog.views
    assert not spark.stopped


def test_transferred_plan_boundary_views_follow_the_session_lifecycle() -> None:
    spark = Spark()
    session = StructureSession(runtime=spark)
    generated = object()
    view = apply_plan_boundary(Frame(spark, "generated"), spark, owner=generated)

    transfer_plan_boundaries(generated, session)
    session.close()

    assert view not in spark.catalog.views
    assert not spark.stopped


def test_invocation_run_batch_forwards_to_session() -> None:
    class Invocation(Transform):
        pass

    class Session:
        def run_batch(self, handoff, invocation):
            return handoff, invocation

    invocation = Invocation()
    handoff = object()

    assert invocation.run_batch(Session(), handoff) == (handoff, invocation)
