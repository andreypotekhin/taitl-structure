from structure import Schema
from structure.field import *


class TrendIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    close = double(nullable=False)
    sma_20 = double(nullable=False)
    sma_50 = double(nullable=False)
    high_20 = double(nullable=False)
    low_20 = double(nullable=False)
    above_sma_50 = boolean(nullable=False)


class MomentumIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    return_1d = double(nullable=True)
    roc_10 = double(nullable=True)
    rsi_14 = double(nullable=True)
    stochastic_k_14 = double(nullable=True)


class VolatilityIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    range_14 = double(nullable=False)
    return_stddev_20 = double(nullable=True)
    bollinger_middle = double(nullable=False)
    bollinger_upper = double(nullable=True)
    bollinger_lower = double(nullable=True)


class VolumeIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    volume = long(nullable=False)
    volume_sma_20 = double(nullable=False)
    relative_volume = double(nullable=False)
    on_balance_volume = long(nullable=True)
    vwap_20 = double(nullable=False)


class AdvancedIndicator(Schema):
    symbol = string(nullable=False)
    trade_date = date(nullable=False)
    return_1d = double(nullable=True)
    benchmark_return = double(nullable=True)
    excess_return = double(nullable=True)
    daily_return_rank = long(nullable=False)
    drawdown = double(nullable=False)
    realized_volatility_20 = double(nullable=True)
