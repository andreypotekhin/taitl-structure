# Stocks Example

This batch-only example shows Structure as a compact language for technical-analysis calculations over daily adjusted
OHLCV bars. It deliberately keeps the source data immutable: each transform returns a named analytical dataset that can
be materialized, tested, reused, or generated as ordinary optimizer-visible PySpark.

`PrepareReturns` is the shared preparation stage. It partitions by symbol, orders by `trade_date`, and adds the
previous close, daily return, gain/loss, and signed volume. The indicator families consume that dataset independently:

- `trend.py`: 20/50-period simple moving averages and a 20-period price channel.
- `momentum.py`: 10-period rate of change, 14-period RSI, and stochastic %K.
- `volatility.py`: 14-period average range, 20-period return standard deviation, and Bollinger bands.
- `volume.py`: relative volume, cumulative on-balance volume, and rolling VWAP.
- `advanced.py`: benchmark-relative excess return, daily cross-sectional ranking, rolling drawdown, and realized volatility.

The advanced transform accepts a separately supplied `BenchmarkReturn`. This makes the benchmark relationship explicit
and lets callers choose a broad index, sector index, or custom strategy benchmark without changing the indicator model.

All windows are row-based daily windows. Feed each transform complete batch data ordered by `trade_date`; Structure emits
the corresponding PySpark `Window` expressions. This app intentionally does not use Structured Streaming: technical
indicators often need historical recalculation after adjusted prices or late corrections.

Small, deterministic input fixtures live in `examples/fixtures/stocks/`: `bars.csv` holds two symbols' OHLCV bars and
`benchmark_returns.csv` supplies the index return used by the advanced transform. The integration test runs these inputs
through both Structure's online and generated execution modes.

```python
returns = PrepareReturns(bars=bars).run(session).returns
trend = Trend(bars=bars).run(session).indicators
momentum = Momentum(returns=returns).run(session).indicators
volatility = Volatility(returns=returns).run(session).indicators
volume = Volume(returns=returns).run(session).indicators
advanced = Advanced(returns=returns, benchmarks=benchmark_returns).run(session).indicators
```
