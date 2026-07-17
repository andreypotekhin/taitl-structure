from examples.stocks.schemas.indicators import VolatilityIndicator
from examples.stocks.schemas.market import DailyReturn
from structure import *


class Volatility(Transform):
    """Trading-range, realized-volatility, and Bollinger-band measures."""

    returns = input(DailyReturn)
    indicators = output(VolatilityIndicator)

    def calculate(self, row: DailyReturn) -> VolatilityIndicator:
        window_20 = window(
            partition_by=row.symbol,
            order_by=row.trade_date,
            frame=rows_between(preceding(19), current_row()),
        )
        middle = rolling_avg(row.close, partition_by=row.symbol, order_by=row.trade_date, preceding=19)
        deviation = window_stddev(row.close, over=window_20)
        return VolatilityIndicator(
            symbol=row.symbol,
            trade_date=row.trade_date,
            range_14=rolling_avg(
                row.high - row.low,
                partition_by=row.symbol,
                order_by=row.trade_date,
                preceding=13,
            ),
            return_stddev_20=window_stddev(row.return_1d, over=window_20),
            bollinger_middle=middle,
            bollinger_upper=middle + 2.0 * deviation,
            bollinger_lower=middle - 2.0 * deviation,
        )
