from pathlib import Path

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

from examples.stocks.schemas.indicators import (
    AdvancedIndicator,
    MomentumIndicator,
    TrendIndicator,
    VolatilityIndicator,
    VolumeIndicator,
)
from examples.stocks.schemas.market import BenchmarkReturn, DailyReturn, MarketBar
from examples.stocks.transforms.advanced import Advanced
from examples.stocks.transforms.momentum import Momentum
from examples.stocks.transforms.returns import PrepareReturns
from examples.stocks.transforms.trend import Trend
from examples.stocks.transforms.volatility import Volatility
from examples.stocks.transforms.volume import Volume

pytestmark = pytest.mark.integration

PACKAGE = "integration_stocks_generated"
FIXTURES = Path(__file__).resolve().parents[4] / "examples" / "fixtures" / "stocks"
SCHEMA_MODULES = {
    "examples.stocks.schemas.indicators": [
        TrendIndicator,
        MomentumIndicator,
        VolatilityIndicator,
        VolumeIndicator,
        AdvancedIndicator,
    ],
    "examples.stocks.schemas.market": [MarketBar, DailyReturn, BenchmarkReturn],
}
TRANSFORMS = (
    (PrepareReturns, "examples.stocks.transforms.returns.PrepareReturns"),
    (Trend, "examples.stocks.transforms.trend.Trend"),
    (Momentum, "examples.stocks.transforms.momentum.Momentum"),
    (Volatility, "examples.stocks.transforms.volatility.Volatility"),
    (Volume, "examples.stocks.transforms.volume.Volume"),
    (Advanced, "examples.stocks.transforms.advanced.Advanced"),
)


def test_stock_fixtures_run_online_and_generated(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        from importlib import import_module

        schemas = import_module(f"{PACKAGE}.pyspark.schemas.market")
        bars = spark.read.schema(schemas.MARKET_BAR_SCHEMA).option("header", True).csv(str(FIXTURES / "bars.csv"))
        benchmarks = (
            spark.read.schema(schemas.BENCHMARK_RETURN_SCHEMA)
            .option("header", True)
            .csv(str(FIXTURES / "benchmark_returns.csv"))
        )
        online_returns = PrepareReturns(bars=bars).run(session(spark, execution_mode="online")).returns
        generated_returns = (
            PrepareReturns(bars=bars).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).returns
        )

        assert rows(online_returns, "symbol", "trade_date") == rows(generated_returns, "symbol", "trade_date")
        assert rows(generated_returns, "symbol", "trade_date")[1]["return_1d"] == pytest.approx(2.0 / 102.0)

        _assert_equivalent(Trend, bars=bars, spark=spark)
        _assert_equivalent(Momentum, returns=generated_returns, spark=spark)
        _assert_equivalent(Volatility, returns=generated_returns, spark=spark)
        _assert_equivalent(Volume, returns=generated_returns, spark=spark)
        _assert_equivalent(Advanced, returns=generated_returns, benchmarks=benchmarks, spark=spark)


def _assert_equivalent(transform, *, spark, **inputs) -> None:
    online = transform(**inputs).run(session(spark, execution_mode="online")).indicators
    generated = (
        transform(**inputs).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).indicators
    )
    assert rows(online, "symbol", "trade_date") == rows(generated, "symbol", "trade_date")
