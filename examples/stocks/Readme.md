# Stocks Example

This batch-only example shows Structure as a compact language for technical-analysis calculations over daily adjusted
OHLCV bars. It deliberately keeps the source data immutable: each transform returns a named analytical dataset that can
be materialized, tested, reused, or generated as ordinary optimizer-visible PySpark.

`PrepareReturns` is the shared preparation stage. It partitions by symbol, orders by `trade_date`, and adds the
previous close, daily return, gain/loss, and signed volume. Returns are null when no positive previous close is available;
flat-price bars contribute zero to on-balance volume. The indicator families consume that dataset independently:

- `trend.py`: completed 20/50-bar simple moving averages and a 20-bar price channel.
- `momentum.py`: completed 10-bar rate of change, 14-change Cutler RSI, and 14-bar stochastic %K.
- `volatility.py`: completed 14-bar average high-low range, 20-bar daily-return sample standard deviation, and Bollinger bands.
- `volume.py`: completed 20-bar relative volume, cumulative on-balance volume, and typical-price VWAP approximation.
- `advanced.py`: benchmark-relative excess return, daily cross-sectional rank, 20-bar-high drawdown, and daily-return standard deviation.

The advanced transform accepts a separately supplied `BenchmarkReturn`. Supply exactly one selected benchmark return per
trade date; otherwise the join multiplies rows. This lets callers choose a broad index, sector index, or custom strategy
benchmark without changing the indicator model.

All windows are row-based daily windows. A period-labelled output is null until its complete lookback is available; this
avoids presenting a short warm-up window as a full-period indicator. The 20-bar return standard deviations are not
annualized. Feed each transform complete batch data ordered by `trade_date`; Structure emits the corresponding PySpark
`Window` expressions. This app intentionally does not use Structured Streaming: technical indicators often need
historical recalculation after adjusted prices or late corrections.

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
