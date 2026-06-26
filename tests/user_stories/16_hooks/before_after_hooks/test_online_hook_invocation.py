from types import SimpleNamespace
from typing import Any, cast

import pytest

from structure import SchemaMode
from structure.app.runtime.execution.online.logic.PySparkHookInvoker import HookInputs, PySparkHookInvoker
from structure.app.target.pyspark.model.PySparkHookRecipe import PySparkHookRecipe


def test_online_hooks_receive_selected_lane_spark_and_context() -> None:
    """I can write hook methods with a selected lane parameter."""

    invocation = RecordingHook()
    frames: dict[str, object] = {"orders": "orders-frame"}

    PySparkHookInvoker().apply(
        (_hook("decorate_orders", lanes=("orders",), outputs=("orders",)),),
        frames=frames,
        inputs=None,
        invocation=cast(Any, invocation),
        session=SimpleNamespace(spark="spark", ctx={"job": "nightly"}),
    )

    assert frames["orders"] == "decorated-orders-frame"
    assert invocation.calls == [
        {
            "orders": "orders-frame",
            "spark": "spark",
            "ctx": {"job": "nightly"},
            "inputs": None,
        }
    ]


def test_online_hooks_receive_read_only_named_inputs_when_requested() -> None:
    """I can opt a hook into original input access."""

    inputs = HookInputs(orders="raw-orders", customers="customers")
    invocation = RecordingHook()

    PySparkHookInvoker().apply(
        (_hook("validate_lookup", lanes=("orders",), outputs=("orders",), pass_inputs=True),),
        frames={"orders": "orders-frame"},
        inputs=inputs,
        invocation=cast(Any, invocation),
        session=SimpleNamespace(spark=None, ctx=None),
    )

    hook_inputs = cast(HookInputs, invocation.calls[0]["inputs"])
    assert getattr(hook_inputs, "customers") == "customers"
    with pytest.raises(AttributeError, match="read-only"):
        inputs.customers = "replacement"


def test_online_hooks_can_return_multiple_output_lanes() -> None:
    """I can use arbitrary PySpark DataFrame code inside hooks."""

    frames: dict[str, object] = {"orders": "orders-frame"}

    PySparkHookInvoker().apply(
        (_hook("split_orders", lanes=("orders",), outputs=("valid", "invalid")),),
        frames=frames,
        inputs=None,
        invocation=cast(Any, RecordingHook()),
        session=SimpleNamespace(spark=None, ctx=None),
    )

    assert frames["valid"] == "valid-orders-frame"
    assert frames["invalid"] == "invalid-orders-frame"


def _hook(
    name: str,
    *,
    lanes: tuple[str, ...],
    outputs: tuple[str, ...],
    pass_inputs: bool = False,
) -> PySparkHookRecipe:
    return PySparkHookRecipe(
        name=name,
        phase="after",
        target=lanes[0],
        lanes=lanes,
        outputs=outputs,
        pass_inputs=pass_inputs,
        schema_mode=SchemaMode.STRICT,
        project_output=False,
        streaming_safe=True,
    )


class RecordingHook:

    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def decorate_orders(self, *, orders, spark, ctx):
        self.calls.append({"orders": orders, "spark": spark, "ctx": ctx, "inputs": None})
        return f"decorated-{orders}"

    def validate_lookup(self, *, orders, spark, ctx, inputs):
        self.calls.append({"orders": orders, "spark": spark, "ctx": ctx, "inputs": inputs})
        return orders

    def split_orders(self, *, orders, spark, ctx):
        self.calls.append({"orders": orders, "spark": spark, "ctx": ctx, "inputs": None})
        return f"valid-{orders}", f"invalid-{orders}"
