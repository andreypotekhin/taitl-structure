from structure import Schema
from structure.field import *


class TrendIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    close = double(nullable=False)
    sma_20 = double(nullable=True)
    sma_50 = double(nullable=True)
    high_20 = double(nullable=True)
    low_20 = double(nullable=True)
    above_sma_50 = boolean(nullable=True)


class MomentumIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    return_1d = double(nullable=True)
    roc_10 = double(nullable=True)
    cutler_rsi_14 = double(nullable=True)
    stochastic_k_14 = double(nullable=True)


class VolatilityIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    range_14 = double(nullable=True)
    daily_return_stddev_20 = double(nullable=True)
    bollinger_middle = double(nullable=True)
    bollinger_upper = double(nullable=True)
    bollinger_lower = double(nullable=True)


class VolumeIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    volume = long(nullable=False)
    volume_sma_20 = double(nullable=True)
    relative_volume = double(nullable=True)
    on_balance_volume = long(nullable=False)
    typical_price_vwap_20 = double(nullable=True)


class AdvancedIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    return_1d = double(nullable=True)
    benchmark_return = double(nullable=True)
    excess_return = double(nullable=True)
    daily_return_rank = long(nullable=True)
    drawdown_from_20d_high = double(nullable=True)
    daily_return_stddev_20 = double(nullable=True)
