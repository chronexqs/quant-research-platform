# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0-alpha] - 2026-03-02

Initial pre-alpha release of the Athena Data Platform.

### Added

- **Data ingestion pipeline** — ingest CSV files (or AWS Athena queries) into versioned Parquet storage with strict schema enforcement
- **Snapshot engine** — create immutable, normalized dataset snapshots with deduplication and timezone normalization
- **Feature materializer** — compute analytical features from snapshots using 11 built-in strategies:
  - `rolling_std`, `moving_average`, `ewma`, `rolling_min`, `rolling_max`
  - `vwap`, `returns`, `log_returns`, `z_score`, `realized_volatility`, `cross_sectional_rank`
- **Python API** — `load_dataset()`, `load_features()`, `query_dataset()`, `query_features()`, `list_datasets()`, `list_snapshots()`, `list_feature_sets()`, `build_backtest_matrix()`
- **CLI** (`adp`) — `init`, `ingest`, `snapshot create/list/show`, `features build/list/show/load`
- **SQLite metadata registry** — 7 tables tracking datasets, ingestions, snapshots, lineage, feature definitions, feature snapshots, and feature lineage
- **Full lineage tracking** — trace any feature snapshot back through dataset snapshots to raw ingestions
- **Idempotent ingestion** — duplicate ingestion blocked unless `--force` flag is used
- **DuckDB SQL queries** — run SQL directly over Parquet snapshots and feature sets
- **Backtest matrix builder** — combine features with configurable forward returns for research
- **Deterministic mock data** — 4 datasets (OHLCV, RFQ events, trade records, funding rates) with 5 feature sets
- **Comprehensive test suite** — 122 tests across unit, integration, and end-to-end tiers
- **Documentation** — design document, user stories, test strategy, quant infrastructure guide, and user guide with reproducible workflows

[0.1.0-alpha]: https://github.com/chronexqs/quant-research-platform/releases/tag/v0.1.0-alpha
