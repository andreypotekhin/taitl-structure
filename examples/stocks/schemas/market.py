from structure import Schema
from structure.platform.pyspark.dsl.field import *


class MarketBar(Schema):
    """One adjusted daily OHLCV bar for a listed instrument."""

    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    open = double(nullable=False)
    high = double(nullable=False)
    low = double(nullable=False)
    close = double(nullable=False)
    volume = long(nullable=False)


class DailyReturn(MarketBar):
    previous_close = double(nullable=True)
    price_change = double(nullable=True)
    return_1d = double(nullable=True)
    gain = double(nullable=True)
    loss = double(nullable=True)
    signed_volume = long(nullable=False)


class BenchmarkReturn(Schema):
    benchmark = string(nullable=False)
    trade_date = date(nullable=False)
    return_1d = double(nullable=True)
