# Athena Data Platform (ADP)

**Local lakehouse + research factor platform for quantitative research.**

[![CI](https://github.com/chronexqs/quant-research-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/chronexqs/quant-research-platform/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)](https://www.python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--alpha-orange)]()

---

ADP ingests raw data from files or AWS Athena, enforces strict schemas, creates immutable versioned snapshots, computes deterministic analytical features, and exposes everything through a Python API and CLI. Built on [Polars](https://pola.rs/) for fast lazy evaluation and [DuckDB](https://duckdb.org/) for SQL-over-Parquet queries.

## Architecture

Data flows through four immutable stages:

```text
Source (CSV / Athena)
       |
       v
+--------------+     +---------------+     +---------------+
|     RAW      |---->|  NORMALIZED   |---->|   FEATURES    |
|   Parquet    |     |   Snapshot    |     |   Snapshot    |
|  data/raw/   |     | data/normal/  |     | data/features/|
+--------------+     +---------------+     +---------------+
  adp ingest        adp snapshot create   adp features build
```

Every stage is **immutable** — data is never overwritten. Full lineage is tracked in a SQLite metadata registry from features back to raw source files.

## Key Features

- **Immutable data pipeline** — raw ingestion, normalized snapshots, materialized features
- **11 built-in feature strategies** — SMA, EWMA, VWAP, Z-score, realized volatility, rolling min/max, returns, log returns, cross-sectional rank
- **Lazy evaluation** — all data loaded as Polars LazyFrames; single `.collect()` at execution
- **SQL-over-Parquet** — ad-hoc DuckDB queries against any snapshot or feature set
- **Full lineage tracking** — every feature traces back through snapshots to raw ingestions
- **Idempotent ingestion** — duplicate ingestions blocked unless `--force` is used
- **Backtest matrix builder** — features + configurable forward returns in one call
- **Deterministic mock data** — all examples reproducible from included CSV files

## Quick Start

```bash
# Install
git clone https://github.com/chronexqs/quant-research-platform.git
cd quant-research-platform
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run the pipeline
adp init
adp ingest ohlcv_btcusdt
adp snapshot create ohlcv_btcusdt
adp features build ohlcv_btcusdt candle_factors

# Query with SQL
adp features load ohlcv_btcusdt candle_factors --head 5
```

## Python API

```python
import adp

# Discover available data
print(adp.list_datasets())
print(adp.list_feature_sets("ohlcv_btcusdt"))

# Load features as a Polars LazyFrame
lf = adp.load_features("ohlcv_btcusdt", "candle_factors")
df = lf.collect()

# Run SQL over Parquet via DuckDB
result = adp.query_features(
    "ohlcv_btcusdt", "candle_factors",
    "SELECT timestamp, close, sma_10 FROM features LIMIT 5"
)

# Build a backtest matrix with forward returns
bt = adp.build_backtest_matrix(
    "ohlcv_btcusdt", "candle_factors",
    forward_return_periods=[1, 5, 10, 60],
)
```

## Documentation

| Document | Description |
| -------- | ----------- |
| [User Guide](docs/06-user-guide.md) | Installation, workflows for all user types, full API reference |
| [Design Document](docs/03-design-document.md) | Architecture, data model, technical decisions |
| [User Stories](docs/02-user-stories.md) | Persona-based requirements and use cases |
| [Test Strategy](docs/04-test-strategy.md) | Testing approach, coverage, and methodology |
| [Quant Infrastructure](docs/05-quant-infrastructure.md) | Domain-specific guidance for quant workflows |
| [Project Plan](docs/01-project-delivery-plan.md) | Roadmap and delivery milestones |

## Mock Data

All workflows use deterministic mock data included in `data/mock_data/`. Regenerate at any time:

```bash
python scripts/generate_mock_data.py
```

| File | Rows | Description |
| ---- | :--: | ----------- |
| `ohlcv_btcusdt_1s.csv` | 3,600 | 1-second BTCUSDT OHLCV candles (60 minutes) |
| `rfq_events.csv` | 60 | Pre-trade RFQ lifecycle (20 RFQs x 3 events each) |
| `trade_records.csv` | 14 | Executed trades from accepted RFQs |
| `funding_rates.csv` | 180 | 8-hour BTCUSDT funding rate snapshots (60 days) |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, code style, and testing guidelines.

## License

This project is licensed under the Apache License 2.0 — see [LICENSE](LICENSE) for details.
