from examples.stocks.schemas.indicators import AdvancedIndicator
from examples.stocks.schemas.market import BenchmarkReturn, DailyReturn
from structure import *


class Advanced(Transform):
    """Benchmark-relative and cross-sectional analytics built from the same return series."""

    returns = input(DailyReturn)
    benchmarks = input(BenchmarkReturn)
    indicators = output(AdvancedIndicator)

    @step(input=[returns, benchmarks], output=indicators)
    def calculate(self, row: DailyReturn, benchmark: BenchmarkReturn) -> AdvancedIndicator:
        inner_join(on=row.trade_date == benchmark.trade_date)
        running_window = window(
            partition_by=row.symbol,
            order_by=row.trade_date,
            frame=rows_between(preceding(19), current_row()),
        )
        return AdvancedIndicator(
            symbol=row.symbol,
            trade_date=row.trade_date,
            return_1d=row.return_1d,
            benchmark_return=benchmark.return_1d,
            excess_return=row.return_1d - benchmark.return_1d,
            daily_return_rank=rank(partition_by=row.trade_date, order_by=row.return_1d, descending=True),
            drawdown=row.close / window_max(row.close, over=running_window) - 1.0,
            realized_volatility_20=window_stddev(row.return_1d, over=running_window),
        )
