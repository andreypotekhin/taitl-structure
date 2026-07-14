from typing import cast

import pytest

from structure import *
from structure.app.dsl.model.expr.Expression import Expression
from structure.app.dsl.model.transforms.operations import WindowFrame


@pytest.mark.parametrize(
    "call",
    [
        lambda: rolling_sum("amount", partition_by="tenant", order_by="ordered", preceding=1),
        lambda: rolling_avg("amount", partition_by="tenant", order_by="ordered", preceding=1),
        lambda: window_sum("amount", over=window(partition_by="tenant", order_by="ordered")),
        lambda: window_avg("amount", over=window(partition_by="tenant", order_by="ordered")),
    ],
)
def test_numeric_window_helpers_reject_non_numeric_arguments(call) -> None:
    with pytest.raises(TypeError, match="requires a numeric expression"):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: rolling_min(array("amount"), partition_by="tenant", order_by="ordered", preceding=1),
        lambda: rolling_max(array("amount"), partition_by="tenant", order_by="ordered", preceding=1),
        lambda: window_min(array("amount"), over=window(partition_by="tenant", order_by="ordered")),
        lambda: window_max(array("amount"), over=window(partition_by="tenant", order_by="ordered")),
    ],
)
def test_extrema_window_helpers_reject_non_orderable_arguments(call) -> None:
    with pytest.raises(TypeError, match="requires an orderable scalar expression"):
        call()


def test_window_rejects_invalid_frame_objects() -> None:
    with pytest.raises(TypeError, match=r"window\(frame=\.\.\.\) requires rows_between"):
        window(partition_by="tenant", order_by="ordered", frame=cast(WindowFrame, "current row"))


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (
            lambda: lag(1, partition_by="tenant", order_by="ordered", default="missing"),
            "default must be compatible",
        ),
        (
            lambda: lead(
                1,
                partition_by="tenant",
                order_by="ordered",
                default=Expression(kind="test_default", type=Long(), nullable=False),
            ),
            "default must be a Python scalar literal",
        ),
    ],
)
def test_lag_and_lead_reject_unlowerable_or_incompatible_defaults(call, message: str) -> None:
    with pytest.raises(TypeError, match=message):
        call()
