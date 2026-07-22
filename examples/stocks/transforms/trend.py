from examples.stocks.schemas.indicators import TrendIndicator
from examples.stocks.schemas.market import MarketBar
from structure import *
from structure.plugin.pyspark import *


class Trend(Transform):
    """Moving averages and price channels for trend-following strategies."""

    bars = input(MarketBar)
    indicators = output(TrendIndicator)

    def calculate(self, bar: MarketBar) -> TrendIndicator:
        bar_number = row_number(partition_by=bar.symbol, order_by=bar.trade_date)
        sma_20 = rolling_avg(bar.close, partition_by=bar.symbol, order_by=bar.trade_date, preceding=19)
        sma_50 = rolling_avg(bar.close, partition_by=bar.symbol, order_by=bar.trade_date, preceding=49)
        return TrendIndicator.base(bar)(
            sma_20=when(bar_number >= 20, sma_20).otherwise(None),
            sma_50=when(bar_number >= 50, sma_50).otherwise(None),
            high_20=when(
                bar_number >= 20,
                rolling_max(bar.high, partition_by=bar.symbol, order_by=bar.trade_date, preceding=19),
            ).otherwise(None),
            low_20=when(
                bar_number >= 20,
                rolling_min(bar.low, partition_by=bar.symbol, order_by=bar.trade_date, preceding=19),
            ).otherwise(None),
            above_sma_50=when(bar_number >= 50, bar.close > sma_50).otherwise(None),
        )
