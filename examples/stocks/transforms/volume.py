from examples.stocks.schemas.indicators import VolumeIndicator
from examples.stocks.schemas.market import DailyReturn
from structure import *


class Volume(Transform):
    """Relative volume, on-balance volume, and a rolling VWAP."""

    returns = input(DailyReturn)
    indicators = output(VolumeIndicator)

    def calculate(self, row: DailyReturn) -> VolumeIndicator:
        window_20 = window(
            partition_by=row.symbol,
            order_by=row.trade_date,
            frame=rows_between(preceding(19), current_row()),
        )
        volume_sma = rolling_avg(row.volume, partition_by=row.symbol, order_by=row.trade_date, preceding=19)
        return VolumeIndicator(
            symbol=row.symbol,
            trade_date=row.trade_date,
            volume=row.volume,
            volume_sma_20=volume_sma,
            relative_volume=row.volume / volume_sma,
            on_balance_volume=window_sum(
                row.signed_volume,
                over=window(
                    partition_by=row.symbol,
                    order_by=row.trade_date,
                    frame=rows_between(unbounded_preceding(), current_row()),
                ),
            ),
            vwap_20=window_sum(row.close * row.volume, over=window_20)
            / window_sum(row.volume, over=window_20),  # type: ignore[operator]
        )
