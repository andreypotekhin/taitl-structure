# Stocks app future

This document records market-data, research, and portfolio-analysis capabilities that could sensibly be admitted to the
Stocks example later. It is a design backlog, not financial advice and not a promise that every item will be implemented.
A future capability must define data provenance, time alignment, numerical conventions, leakage controls, and whether the
result is descriptive, evaluative, or an executable trading instruction.

The current Stocks application works over immutable daily OHLCV bars. It prepares one-day returns and exposes trend,
momentum, volatility, volume, benchmark-relative, rank, drawdown, and rolling risk indicators. It deliberately stays
batch-oriented because adjusted prices and late corrections can require historical recalculation. Callers own data
loading, persistence, charts, trading policy, and interpretation. The items below are not currently admitted.

## Market-data foundations

### Corporate actions and adjusted history

The current bar contract does not model splits, dividends, symbol changes, delistings, or adjustment provenance. A future
`ApplyCorporateActions` workflow could produce raw and adjusted price series with explicit effective dates, factors, source
priority, and restatement policy. It must make it possible to reproduce an earlier analysis from the same data snapshot.

### Instrument identity and listings

A future security master could map symbols to stable instrument identifiers, exchanges, currencies, share classes,
listing intervals, and delisting events. Joins by ticker text alone are insufficient for long-lived research because
symbols can be reused or change. The contract should expose ambiguous or missing identity matches rather than selecting
one silently.

### Intraday and multi-timeframe bars

Stocks currently consumes daily bars only. A future multi-timeframe slice could support minute, hourly, and daily bars with
session calendars, time zones, trading halts, partial sessions, and interval completeness. It must prevent accidental
mixing of bars from incompatible calendars and must state whether indicators use only completed intervals.

### Fundamentals and events

Earnings, balance sheets, analyst estimates, dividends, macroeconomic releases, and sector classifications could enrich
the research example. Each fact needs an availability timestamp distinct from its period or announcement date so a
backtest cannot use information before it was public.

## Research and signal evaluation

### Signal pipelines

A future `Signals` family could combine indicator relations into named, versioned signals with feature availability dates,
missing-input policy, normalization, and neutral defaults. Signal components should remain inspectable, and a signal must
not be presented as a recommendation or order instruction.

### Backtesting

A natural next step is a deterministic backtest over historical bars. It would need position state, entry and exit timing,
cash, transaction costs, slippage, commissions, position limits, corporate actions, and an explicit rule preventing
future data leakage. The backtest should publish trades and equity curves as well as summary metrics, and must distinguish
simulated fills from observed market transactions.

### Walk-forward and cross-validation

Future evaluation could train or select parameters on one time range and evaluate them on a later range. The split policy,
purge or embargo interval, parameter version, and candidate selection rule must be recorded. Random row splitting is not an
acceptable substitute for temporal validation.

### Strategy comparison and experiments

Stocks could compare named indicator strategies or parameter sets with common bars, costs, and evaluation windows. Results
should include drawdown, turnover, volatility, returns, and risk-adjusted measures with sample counts. The comparison is
descriptive unless a causal experiment design exists.

## Risk and portfolio analysis

### Portfolio construction

A future portfolio workflow could combine asset weights, cash, holdings, orders, constraints, and rebalance dates. It must
separate target weights, submitted orders, executed fills, and observed holdings. Optimization objectives, leverage,
shorting, concentration limits, and infeasible-constraint behavior must be explicit inputs.

### Risk measures

Stocks could publish portfolio volatility, beta, tracking error, value-at-risk, expected shortfall, factor exposure,
correlation, and drawdown decomposition. These measures need declared estimation windows, missing-data behavior, sample
versus population definitions, and minimum-history rules. Approximate or model-based risk must remain labeled as such.

### Multi-asset and currency support

Equities are the current focus. Future support for bonds, funds, futures, crypto, foreign exchange, and multiple currencies
would require instrument-specific price, quantity, calendar, valuation, and corporate-event contracts. It should be added
by asset family rather than through a permissive “any market row” schema.

## Trading and operations

### Order and execution analytics

A future execution branch could reconcile submitted orders, broker acknowledgements, fills, cancellations, and market bars.
It could report fill rate, slippage, latency, partial fills, and venue or broker performance. It must not submit orders or
manage credentials; execution remains an external system and caller-owned side effect.

### Data quality and survivorship controls

Long-lived research needs checks for missing sessions, duplicate bars, impossible OHLC relationships, stale prices,
delisted instruments, restated fundamentals, and survivorship bias. A future quality workflow should publish diagnostics
and data snapshots rather than silently repair or drop market facts.

### Streaming market monitoring

The example could eventually support live bar or quote monitoring, alerts, and rolling intraday indicators. This would need
explicit state, watermark, session-calendar, late-correction, restart, and alert-deduplication contracts. It must not turn
the Stocks example into a trading-job framework.

## Permanent boundaries

Unless a separate product decision changes the architecture, Stocks does not own:

- market-data acquisition, licensing, broker connections, or exchange connectivity;
- trade submission, order management, execution, settlement, or custody;
- investment advice, suitability decisions, or guaranteed performance claims;
- secret management, account permissions, or financial compliance workflows; and
- charting, dashboards, notebooks, or live monitoring lifecycle.

## Admission guidance

Admit Stocks capabilities in vertical, reproducible slices. Each slice should specify the time-information boundary,
provide a deterministic fixture, test leakage and warm-up behavior, preserve raw and derived facts separately, and show
online/generated parity where applicable. New indicators should remain independent transforms or clearly named pipelines;
portfolio and backtesting workflows should not quietly change the meaning of the existing daily indicator outputs.

## References

- Current Stocks application: `examples/stocks/Readme.md`
- Current API admission guidance: `docs/dev/future/API.future.md`
- Current streaming boundary: `docs/dev/future/Streaming.future.md`
