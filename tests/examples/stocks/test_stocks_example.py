import pytest

from examples.stocks.transforms.advanced import Advanced
from examples.stocks.transforms.momentum import Momentum
from examples.stocks.transforms.returns import PrepareReturns
from examples.stocks.transforms.trend import Trend
from examples.stocks.transforms.volatility import Volatility
from examples.stocks.transforms.volume import Volume
from structure.core.compiler.api import Compiler


@pytest.mark.parametrize(
    "transform",
    [PrepareReturns, Trend, Momentum, Volatility, Volume, Advanced],
)
def test_stock_indicator_transform_compiles(transform) -> None:
    Compiler.frontend.compile()(transform, materialize_schemas=False)
