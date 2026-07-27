from examples.stocks.schemas.indicators import MomentumIndicator
from examples.stocks.schemas.market import DailyReturn
from structure import *
from structure.plugin.pyspark import *


class Momentum(Transform):
    """Rate of change, Cutler RSI, and stochastic oscillator from prepared returns."""

    returns = input(DailyReturn)
    indicators = output(MomentumIndicator)

    def calculate(self, row: DailyReturn) -> MomentumIndicator:
        bar_number = row_number(partition_by=row.symbol, order_by=row.trade_date)
        rsi_window = window(
            partition_by=row.symbol,
            order_by=row.trade_date,
            frame=rows_between(preceding(13), current_row()),
        )
        average_gain = window_avg(row.gain, over=rsi_window)
        average_loss = window_avg(row.loss, over=rsi_window)
        low_14 = rolling_min(row.low, partition_by=row.symbol, order_by=row.trade_date, preceding=13)
        high_14 = rolling_max(row.high, partition_by=row.symbol, order_by=row.trade_date, preceding=13)
        return MomentumIndicator.project(row)(
            roc_10=when(
                bar_number >= 11,
                row.close / lag(row.close, partition_by=row.symbol, order_by=row.trade_date, offset=10) - 1.0,
            ).otherwise(None),
            cutler_rsi_14=when(
                bar_number >= 15,
                when(average_loss > 0, 100.0 - 100.0 / (1.0 + average_gain / average_loss)).otherwise(100.0),
            ).otherwise(None),
            stochastic_k_14=when(bar_number >= 14, (row.close - low_14) / (high_14 - low_14) * 100.0).otherwise(None),
        )
