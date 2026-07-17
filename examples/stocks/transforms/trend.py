from examples.stocks.schemas.indicators import TrendIndicator
from examples.stocks.schemas.market import MarketBar
from structure import *


class Trend(Transform):
    """Moving averages and price channels for trend-following strategies."""

    bars = input(MarketBar)
    indicators = output(TrendIndicator)

    def calculate(self, bar: MarketBar) -> TrendIndicator:
        sma_50 = rolling_avg(bar.close, partition_by=bar.symbol, order_by=bar.trade_date, preceding=49)
        return TrendIndicator(
            symbol=bar.symbol,
            trade_date=bar.trade_date,
            close=bar.close,
            sma_20=rolling_avg(bar.close, partition_by=bar.symbol, order_by=bar.trade_date, preceding=19),
            sma_50=sma_50,
            high_20=rolling_max(bar.high, partition_by=bar.symbol, order_by=bar.trade_date, preceding=19),
            low_20=rolling_min(bar.low, partition_by=bar.symbol, order_by=bar.trade_date, preceding=19),
            above_sma_50=bar.close > sma_50,
        )
