from examples.stocks.schemas.indicators import MomentumIndicator
from examples.stocks.schemas.market import DailyReturn
from structure import *


class Momentum(Transform):
    """Rate of change, RSI, and stochastic oscillator from prepared returns."""

    returns = input(DailyReturn)
    indicators = output(MomentumIndicator)

    def calculate(self, row: DailyReturn) -> MomentumIndicator:
        rsi_window = window(
            partition_by=row.symbol,
            order_by=row.trade_date,
            frame=rows_between(preceding(13), current_row()),
        )
        average_gain = window_avg(row.gain, over=rsi_window)
        average_loss = window_avg(row.loss, over=rsi_window)
        low_14 = rolling_min(row.low, partition_by=row.symbol, order_by=row.trade_date, preceding=13)
        high_14 = rolling_max(row.high, partition_by=row.symbol, order_by=row.trade_date, preceding=13)
        return MomentumIndicator(
            symbol=row.symbol,
            trade_date=row.trade_date,
            return_1d=row.return_1d,
            roc_10=row.close / lag(row.close, partition_by=row.symbol, order_by=row.trade_date, offset=10) - 1.0,
            rsi_14=when(average_loss > 0, 100.0 - 100.0 / (1.0 + average_gain / average_loss)).otherwise(100.0),
            stochastic_k_14=(row.close - low_14) / (high_14 - low_14) * 100.0,
        )
