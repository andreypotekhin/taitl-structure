from examples.stocks.schemas.market import DailyReturn, MarketBar
from structure import *


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
            return_1d=price_change / previous_close,
            gain=when(price_change > 0, price_change).otherwise(0.0),
            loss=when(price_change < 0, -price_change).otherwise(0.0),
            signed_volume=when(price_change > 0, bar.volume).otherwise(-bar.volume),
        )
