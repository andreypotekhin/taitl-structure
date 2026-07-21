from examples.stocks.schemas.market import DailyReturn, MarketBar
from structure import *
from structure.plugin.pyspark import *


class PrepareReturns(Transform):
    """Derive one-day price changes once for downstream indicator families."""

    bars = input(MarketBar)
    returns = output(DailyReturn)

    def calculate(self, bar: MarketBar) -> DailyReturn:
        previous_close = lag(bar.close, partition_by=bar.symbol, order_by=bar.trade_date)
        price_change = bar.close - previous_close
        return DailyReturn(
            symbol=bar.symbol,
            trade_date=bar.trade_date,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            previous_close=previous_close,
            price_change=price_change,
            return_1d=when(previous_close > 0, price_change / previous_close).otherwise(None),
            gain=when(previous_close.is_null(), None).otherwise(when(price_change > 0, price_change).otherwise(0.0)),
            loss=when(previous_close.is_null(), None).otherwise(when(price_change < 0, -price_change).otherwise(0.0)),
            signed_volume=when(previous_close.is_null(), 0).otherwise(
                when(price_change > 0, bar.volume).otherwise(when(price_change < 0, -bar.volume).otherwise(0))
            ),
        )
