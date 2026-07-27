from examples.stocks.schemas.indicators import VolatilityIndicator
from examples.stocks.schemas.market import DailyReturn
from structure import *
from structure.plugin.pyspark import *


class Volatility(Transform):
    """Trading-range, realized-volatility, and Bollinger-band measures."""

    returns = input(DailyReturn)
    indicators = output(VolatilityIndicator)

    def calculate(self, row: DailyReturn) -> VolatilityIndicator:
        bar_number = row_number(partition_by=row.symbol, order_by=row.trade_date)
        window_20 = window(
            partition_by=row.symbol,
            order_by=row.trade_date,
            frame=rows_between(preceding(19), current_row()),
        )
        middle = rolling_avg(row.close, partition_by=row.symbol, order_by=row.trade_date, preceding=19)
        deviation = window_stddev(row.close, over=window_20)
        return VolatilityIndicator.project(row)(
            range_14=when(
                bar_number >= 14,
                rolling_avg(row.high - row.low, partition_by=row.symbol, order_by=row.trade_date, preceding=13),
            ).otherwise(None),
            daily_return_stddev_20=when(bar_number >= 21, window_stddev(row.return_1d, over=window_20)).otherwise(None),
            bollinger_middle=when(bar_number >= 20, middle).otherwise(None),
            bollinger_upper=when(bar_number >= 20, middle + 2.0 * deviation).otherwise(None),
            bollinger_lower=when(bar_number >= 20, middle - 2.0 * deviation).otherwise(None),
        )
