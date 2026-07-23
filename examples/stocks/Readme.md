# Stocks Example App 

Stocks example app uses Structure as a compact language for technical analysis over daily OHLCV bars.
Each transform returns an analytical dataset that callers may materialize, test, reuse, or execute as ordinary
optimizer-visible PySpark. Source bars remain immutable.

| Concern | Transform | Result | Lookback |
| --- | --- | --- | --- |
| Preparation | `PrepareReturns` | `DailyReturn` rows | Previous close and one-day measures. |
| Trend | `Trend` | Moving averages and channels | 20- and 50-bar windows. |
| Momentum | `Momentum` | ROC, Cutler RSI, stochastic %K | 10- and 14-bar windows. |
| Volatility | `Volatility` | Range, realized volatility, Bollinger bands | 14- and 20-bar windows. |
| Volume | `Volume` | Relative volume, OBV, VWAP | 20-bar relative-volume window. |
| Advanced | `Advanced` | Benchmark, rank, drawdown, volatility | Daily and 20-bar measures. |

## Preparation

`PrepareReturns` partitions bars by symbol, orders them by `trade_date`, and calculates previous close, price change,
one-day return, gain, loss, and signed volume. A row without a positive previous close has a null return; a first row
has null gain and loss and contributes zero signed volume. Flat-price bars also contribute zero signed volume.

```python
returns = PrepareReturns(bars=bars).run(session).returns

# Reuse results downstream
returns.cache()
```

All downstream transforms consume either the immutable bars or this shared result. Feed each transform a complete batch
with one ordered daily series per symbol. The example deliberately does not use Structured Streaming:
adjusted prices and late corrections commonly require recalculating historical indicators.

## Calculate indicator families

```python
trend = Trend(bars=bars).run(session).indicators
momentum = Momentum(returns=returns).run(session).indicators
volatility = Volatility(returns=returns).run(session).indicators
volume = Volume(returns=returns).run(session).indicators
advanced = Advanced(returns=returns, benchmarks=benchmark_returns).run(session).indicators

# Feed a chosen indicator relation to a chart, table, or caller-owned persistence layer.
latest_trend = trend.orderBy("symbol", "trade_date")
```

### Trend and momentum

`Trend` emits completed 20- and 50-bar simple moving averages, a 20-bar high/low channel, and the 50-bar moving-
average comparison. `Momentum` emits completed 10-bar rate of change, 14-change Cutler RSI, and 14-bar stochastic
%K. A period-labelled measure is null until its full history is available, so a warm-up value is never presented as a
full-period indicator.

### Volatility and volume

`Volatility` emits a completed 14-bar average high-low range, 20-bar sample standard deviation of daily returns, and
20-bar Bollinger middle, upper, and lower bands. The return standard deviation is deliberately not annualized.

`Volume` emits 20-bar relative volume, cumulative on-balance volume, and a typical-price VWAP approximation. The
volume measures preserve the input bar grain; they do not invent intraday information unavailable in daily data.

### Advanced analysis

`Advanced` joins each return to a caller-supplied `BenchmarkReturn` on trade date. It emits benchmark-relative excess
return, daily cross-sectional return rank, 20-bar-high drawdown, and 20-bar daily-return standard deviation.

Supply exactly one selected benchmark return per trade date. Multiple benchmark rows multiply the join and therefore
the output; missing dates leave no matching advanced row. This allows callers to select a broad index, sector index,
or custom strategy benchmark without changing the indicator model.

## Fixtures and result handling

Small deterministic fixtures are in `examples/fixtures/stocks/`: `bars.csv` contains two symbols' OHLCV bars and
`benchmark_returns.csv` supplies the benchmark series. The integration test runs them through Structure's online and
generated execution modes. Callers own persistence, charts, trading policy, and interpretation: these descriptive
indicators are not investment advice or a trading signal by themselves.
