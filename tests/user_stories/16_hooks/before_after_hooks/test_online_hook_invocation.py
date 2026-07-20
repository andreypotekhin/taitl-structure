from types import SimpleNamespace
from typing import Any, cast

import pytest

from structure import *
from structure.core.runtime.session.model.StructureRuntimeError import StructureRuntimeError
from structure.platform.pyspark import *
from structure.platform.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe
from structure.platform.pyspark.execution.logic.PySparkHookInvoker import PySparkHookInvoker


def test_online_hooks_receive_selected_lane_spark_and_context() -> None:
    """I can write hook methods with a selected lane parameter."""

    invocation = RecordingHook()
    frames: dict[str, object] = {"orders": "orders-frame"}

    PySparkHookInvoker().apply(
        (_hook("decorate_orders", lanes=("orders",), outputs=("orders",)),),
        frames=frames,
        invocation=cast(Any, invocation),
        session=SimpleNamespace(spark="spark", ctx={"job": "nightly"}),
    )

    assert frames["orders"] == "decorated-orders-frame"
    assert invocation.calls == [
        {
            "orders": "orders-frame",
            "spark": "spark",
            "ctx": {"job": "nightly"},
        }
    ]


def test_online_hooks_receive_explicit_original_input_sources() -> None:
    """I can request an original input without a namespace object."""

    invocation = RecordingHook()

    PySparkHookInvoker().apply(
        (_hook("validate_lookup", lanes=("orders",), sources=("input:orders",), outputs=("orders",)),),
        frames={"orders": "orders-frame", "input:orders": "raw-orders"},
        invocation=cast(Any, invocation),
        session=SimpleNamespace(spark=None, ctx=None),
    )

    assert invocation.calls[0]["orders"] == "raw-orders"


def test_online_hooks_can_return_multiple_output_lanes() -> None:
    """I can use arbitrary PySpark DataFrame code inside hooks."""

    frames: dict[str, object] = {"orders": "orders-frame"}

    PySparkHookInvoker().apply(
        (_hook("split_orders", lanes=("orders",), outputs=("valid", "invalid")),),
        frames=frames,
        invocation=cast(Any, RecordingHook()),
        session=SimpleNamespace(spark=None, ctx=None),
    )

    assert frames["valid"] == "valid-orders-frame"
    assert frames["invalid"] == "invalid-orders-frame"


def test_spark_connect_hook_classic_only_failure_reports_boundary_diagnostic() -> None:
    """Spark Connect hook failures explain the unsupported API boundary."""

    with pytest.raises(StructureRuntimeError) as raised:
        PySparkHookInvoker().apply(
            (_hook("inspect_spark_context", lanes=("orders",), outputs=("orders",)),),
            frames={"orders": "orders-frame"},
            invocation=cast(Any, ClassicOnlyHook()),
            session=SimpleNamespace(
                spark="spark",
                ctx=None,
                execution_mode="online",
                target_backend="pyspark",
                target_profile=">=3.5,<4.1",
                target_variant="spark-connect",
            ),
        )

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "CONNECT-E2601"
    assert diagnostic.target_variant == "spark-connect"
    assert diagnostic.context["surface"] == "hook inspect_spark_context"
    assert "Spark Connect cannot access SparkContext" in diagnostic.problem
    assert 'target_variant = "ordinary"' in diagnostic.use


def _hook(
    name: str,
    *,
    lanes: tuple[str, ...],
    outputs: tuple[str, ...],
    sources: tuple[str, ...] | None = None,
) -> PySparkHookRecipe:
    return PySparkHookRecipe(
        name=name,
        phase="after",
        target=lanes[0],
        lanes=lanes,
        outputs=outputs,
        sources=sources or lanes,
        schema_mode=SchemaMode.STRICT,
        project_output=False,
        streaming_safe=True,
    )


class RecordingHook:

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def decorate_orders(self, *, orders, spark, ctx):
        self.calls.append({"orders": orders, "spark": spark, "ctx": ctx})
        return f"decorated-{orders}"

    def validate_lookup(self, *, orders, spark, ctx):
        self.calls.append({"orders": orders, "spark": spark, "ctx": ctx})
        return orders

    def split_orders(self, *, orders, spark, ctx):
        self.calls.append({"orders": orders, "spark": spark, "ctx": ctx})
        return f"valid-{orders}", f"invalid-{orders}"


class ClassicOnlyHook:

    def inspect_spark_context(self, *, orders, spark, ctx):
        raise RuntimeError("SparkContext is not supported in Spark Connect")
