# Athena Data Platform (ADP) — User Guide

> **Version**: 1.0
> **Date**: 2026-03-02
> **Audience**: All platform users — Admins, Data Engineers, Quant Researchers, Data Scientists, Operations
> **Related**: [Design Document](./03-design-document.md) · [User Stories](./02-user-stories.md) · [Quant Infrastructure](./05-quant-infrastructure.md)

---

## Table of Contents

0. [Prerequisites & Installation](#0-prerequisites--installation)
1. [Platform Overview & Concepts](#1-platform-overview--concepts)
2. [Quick Start — Your First Pipeline](#2-quick-start--your-first-pipeline)
3. [Admin Workflows](#3-admin-workflows)
4. [Data Engineer Workflows](#4-data-engineer-workflows)
5. [Quant Researcher Workflows](#5-quant-researcher-workflows)
6. [Data Scientist Workflows](#6-data-scientist-workflows)
7. [Support & Operations Workflows](#7-support--operations-workflows)
8. [Reference](#8-reference)
9. [JupyterLab Research Environment](#9-jupyterlab-research-environment)

---

## 0. Prerequisites & Installation

### Requirements

- Python 3.11 or later (up to 3.13)
- pip (or any PEP 517 build tool)
- SQLite 3.x (included with Python)

### Installation

```bash
# Clone and install
cd quant-research-platform
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
pip install -e ".[dev]"

# Verify
adp --help
python -c "import adp; print(adp.__version__)"
```

### Project Layout

```text
quant-research-platform/
├── config/
│   ├── datasets.yaml          # Dataset definitions (schema, source, processing)
│   └── features.yaml          # Feature set definitions
├── data/
│   ├── mock_data/             # Deterministic sample data (checked in)
│   │   ├── ohlcv_btcusdt_1s.csv    # 3,600 1-second BTCUSDT candles
│   │   ├── rfq_events.csv          # 60 RFQ lifecycle events
│   │   ├── trade_records.csv       # 14 executed trades
│   │   └── funding_rates.csv       # 180 8-hour funding rate snapshots
│   ├── raw/                   # Ingested data (Parquet)
│   ├── normalized/            # Versioned snapshots (Parquet)
│   └── features/              # Materialized features (Parquet)
├── metadata/
│   └── adp_registry.db       # SQLite metadata registry
├── src/adp/                   # Platform source code
├── scripts/
│   └── generate_mock_data.py  # Regenerate mock data deterministically
├── notebooks/                 # Research notebooks
└── docs/                      # Documentation
```

### Mock Data

All workflows in this guide use the CSV files in `data/mock_data/`. These are generated deterministically by `scripts/generate_mock_data.py` — no randomness, fully reproducible. You can regenerate them at any time:

```bash
python scripts/generate_mock_data.py
```

| File | Rows | Description |
| ---- | :--: | ----------- |
| `ohlcv_btcusdt_1s.csv` | 3,600 | 1-second BTCUSDT OHLCV candles (60 minutes) |
| `rfq_events.csv` | 60 | Pre-trade RFQ lifecycle (20 RFQs x 3 events each) |
| `trade_records.csv` | 14 | Executed trades from accepted RFQs |
| `funding_rates.csv` | 180 | 8-hour funding rates for BTCUSDT (60 days) |

---

## 1. Platform Overview & Concepts

### What ADP Does

ADP is a **local lakehouse and research factor platform** for quantitative research. It ingests data from files or AWS Athena, enforces strict schemas, creates immutable versioned snapshots, computes deterministic analytical features, and exposes everything through a Python API and CLI.

### Data Flow

Data moves through four immutable stages:

```text
Source File / Athena
       │
       ▼
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   RAW       │────>│  NORMALIZED  │────>│   FEATURES   │
│  Parquet    │     │  Snapshot    │     │  Snapshot     │
│  data/raw/  │     │  data/norm/  │     │  data/feat/   │
└─────────────┘     └──────────────┘     └──────────────┘
  adp ingest       adp snapshot create   adp features build
```

Each stage is **immutable** — data is never overwritten. New ingestions create new raw files, new snapshots create new directories, and new feature builds create new feature snapshots.

### Key Concepts

| Concept | Description |
| ------- | ----------- |
| **Immutability** | Once written, data files are never modified. Old versions remain accessible. |
| **Snapshot** | A versioned, normalized copy of a dataset at a point in time. Identified by a unique ID like `ohlcv_btcusdt_snap_20260302_001`. |
| **Lineage** | Every feature snapshot traces back to a dataset snapshot, which traces back to raw ingestions. The full chain is stored in SQLite. |
| **Feature Set** | A named collection of computed analytical columns defined in `features.yaml` (e.g., `candle_factors`). |
| **Lazy Evaluation** | Data is loaded as Polars LazyFrames. No data is materialized until `.collect()` is called. |
| **Idempotency** | Re-ingesting the same source is blocked unless `--force` is used. Prevents accidental duplicates. |

### Two Interfaces

**CLI** — Pipeline orchestration from the terminal:

```bash
adp init                              # Initialize platform
adp ingest <dataset>                  # Ingest data
adp snapshot create <dataset>         # Create snapshot
adp features build <dataset> <set>    # Build features
```

**Python API** — Research and analysis from code or Jupyter:

```python
from adp import (
    load_dataset,           # Returns Polars LazyFrame
    load_features,          # Returns Polars LazyFrame
    query_dataset,          # Runs DuckDB SQL, returns DataFrame
    query_features,         # Runs DuckDB SQL, returns DataFrame
    list_datasets,          # Returns DataFrame of registered datasets
    list_snapshots,         # Returns DataFrame of snapshots
    list_feature_sets,      # Returns DataFrame of feature sets
    build_backtest_matrix,  # Features + forward returns
)
```

### Metadata Registry

ADP tracks everything in a SQLite database (`metadata/adp_registry.db`) with 7 tables:

| Table | Purpose |
| ----- | ------- |
| `datasets` | Registered datasets with current snapshot pointer |
| `raw_ingestions` | Every ingestion event (source, row count, timestamp) |
| `snapshots` | Normalized snapshots (schema hash, storage path) |
| `snapshot_lineage` | Links: snapshot ← ingestion(s) |
| `feature_definitions` | Feature set metadata (version, definition hash) |
| `feature_snapshots` | Materialized feature outputs |
| `feature_lineage` | Links: feature snapshot ← dataset snapshot |

---

## 2. Quick Start — Your First Pipeline

This walkthrough takes you from zero to querying features in under 5 minutes. It uses the OHLCV mock data.

### Step 1: Initialize the platform

```bash
adp init
```

This creates the directory structure (`data/raw/`, `data/normalized/`, `data/features/`) and initializes the SQLite metadata database.

### Step 2: Ingest data

```bash
adp ingest ohlcv_btcusdt
```

Output:

```text
Ingested 3600 rows -> ohlcv_btcusdt_ing_YYYYMMDD_001
```

This reads `data/mock_data/ohlcv_btcusdt_1s.csv`, converts it to Parquet, and stores it in `data/raw/ohlcv_btcusdt/`.

### Step 3: Create a normalized snapshot

```bash
adp snapshot create ohlcv_btcusdt
```

Output:

```text
Created snapshot: ohlcv_btcusdt_snap_YYYYMMDD_001
```

The normalization pipeline runs: schema validation, timezone normalization (to UTC), and deduplication (by `[timestamp, symbol]`, keep last).

### Step 4: List snapshots

```bash
adp snapshot list ohlcv_btcusdt
```

Output:

```text
  ohlcv_btcusdt_snap_YYYYMMDD_001  rows=3600  created=2026-... (current)
```

### Step 5: Build features

```bash
adp features build ohlcv_btcusdt candle_factors
```

Output:

```text
Built features: ohlcv_btcusdt_candle_factors_fsnap_YYYYMMDD_001
```

This computes 7 analytical features: rolling volatility, two SMAs, EWMA, returns, log returns, and VWAP.

### Step 6: Preview feature data

```bash
adp features load ohlcv_btcusdt candle_factors --head 5
```

This prints the first 5 rows of the feature snapshot — all original columns plus the 7 computed feature columns.

### Step 7: Use the Python API

```python
import adp

# What datasets exist?
print(adp.list_datasets())

# Load features as a LazyFrame
lf = adp.load_features("ohlcv_btcusdt", "candle_factors")
df = lf.collect()
print(f"Shape: {df.shape}")
print(df.head(5))
```

You now have a complete pipeline running. The sections below walk through specific workflows for each user type.

---

## 3. Admin Workflows

### 3.1 Platform Initialization & Health Check

**Goal:** Set up a fresh ADP platform and verify everything is in order.

```bash
# Initialize with default paths
adp init

# Verify directory structure
ls data/
# Expected: features/  mock_data/  normalized/  raw/  staged/

# Verify metadata database
ls metadata/
# Expected: adp_registry.db

# Check database tables (requires sqlite3)
sqlite3 metadata/adp_registry.db ".tables"
# Expected: datasets            feature_lineage     raw_ingestions
#           feature_definitions  feature_snapshots   snapshot_lineage   snapshots
```

You can also initialize with custom paths:

```bash
adp init --data-dir /custom/data --config-dir /custom/config --metadata-dir /custom/metadata
```

### 3.2 Metadata Inspection & Lineage Tracing

**Prerequisite:** Complete the Quick Start (Section 2) first.

**Goal:** Trace the full data lineage chain from features back to raw source files.

```bash
# Show snapshot details with lineage
adp snapshot show ohlcv_btcusdt_snap_YYYYMMDD_001 --lineage
```

Output:

```text
Snapshot: ohlcv_btcusdt_snap_YYYYMMDD_001
  Dataset:    ohlcv_btcusdt
  Rows:       3600
  Schema:     a3f8b2c1d4e5f6...
  Version:    1.0
  Path:       data/normalized/ohlcv_btcusdt/ohlcv_btcusdt_snap_YYYYMMDD_001
  Created:    2026-...
  Lineage:
    <- ohlcv_btcusdt_ing_YYYYMMDD_001
```

For a deeper lineage trace, use the Python API:

```python
from adp.metadata.registry import MetadataRegistry
from pathlib import Path

registry = MetadataRegistry(Path("metadata/adp_registry.db"))

# Trace: features -> snapshot -> ingestion
feat_snaps = registry.list_feature_snapshots("ohlcv_btcusdt", "candle_factors")
for fs in feat_snaps:
    print(f"Feature: {fs.feature_snapshot_id} (v{fs.feature_version}, {fs.row_count} rows)")

    # Which dataset snapshot was it built from?
    lineage = registry.get_feature_lineage(fs.feature_snapshot_id)
    for fl in lineage:
        snap = registry.get_snapshot(fl.dataset_snapshot_id)
        print(f"  <- Snapshot: {snap.snapshot_id} ({snap.row_count} rows)")

        # Which ingestions sourced that snapshot?
        snap_lineage = registry.get_snapshot_lineage(snap.snapshot_id)
        for sl in snap_lineage:
            ing = registry.get_ingestion(sl.ingestion_id)
            print(f"     <- Ingestion: {ing.ingestion_id} from {ing.source_location}")
```

### 3.3 Idempotency & Force Re-ingestion

**Goal:** Understand how ADP prevents accidental duplicate ingestions.

```bash
# First ingestion succeeds
adp ingest ohlcv_btcusdt
# Output: Ingested 3600 rows -> ohlcv_btcusdt_ing_YYYYMMDD_001

# Second attempt is blocked (idempotency guard)
adp ingest ohlcv_btcusdt
# Output: Error: Already ingested from 'data/mock_data/ohlcv_btcusdt_1s.csv'.
#         Use --force to re-ingest.

# Force re-ingestion when intentional
adp ingest ohlcv_btcusdt --force
# Output: Ingested 3600 rows -> ohlcv_btcusdt_ing_YYYYMMDD_002
```

Both ingestion records are preserved in the metadata registry. You can verify:

```python
from adp.metadata.registry import MetadataRegistry
from pathlib import Path

registry = MetadataRegistry(Path("metadata/adp_registry.db"))
for ing in registry.list_ingestions("ohlcv_btcusdt"):
    print(f"{ing.ingestion_id}  rows={ing.row_count}  source={ing.source_location}")
```

---

## 4. Data Engineer Workflows

### 4.1 Batch Ingestion of All Datasets

**Goal:** Ingest, snapshot, and build features for all configured datasets.

```bash
# Initialize
adp init

# Ingest all four datasets
adp ingest ohlcv_btcusdt
adp ingest rfq_events
adp ingest trade_records
adp ingest funding_rates

# Create snapshots for all
adp snapshot create ohlcv_btcusdt
adp snapshot create rfq_events
adp snapshot create trade_records
adp snapshot create funding_rates

# Build all feature sets
adp features build ohlcv_btcusdt candle_factors
adp features build ohlcv_btcusdt risk_factors
adp features build trade_records trade_factors
adp features build rfq_events rfq_analytics
adp features build funding_rates funding_factors
```

Verify everything is registered:

```python
import adp

print("=== Datasets ===")
print(adp.list_datasets())

print("\n=== OHLCV Feature Sets ===")
print(adp.list_feature_sets("ohlcv_btcusdt"))

print("\n=== Trade Feature Sets ===")
print(adp.list_feature_sets("trade_records"))

print("\n=== RFQ Feature Sets ===")
print(adp.list_feature_sets("rfq_events"))

print("\n=== Funding Feature Sets ===")
print(adp.list_feature_sets("funding_rates"))
```

### 4.2 Adding a Completely New Dataset

**Goal:** Walk through the full lifecycle of onboarding a new data source. This uses the `funding_rates` dataset as an example, but the same process applies to any new dataset.

#### Step 1: Prepare the data file

The mock data file at `data/mock_data/funding_rates.csv` contains 180 rows of 8-hourly funding rate snapshots for BTCUSDT (60 days x 3 snapshots/day):

```csv
timestamp,symbol,funding_rate,mark_price,index_price
2026-01-15 00:00:00.000,BTCUSDT,0.00010000,42000.00,42001.50
2026-01-15 08:00:00.000,BTCUSDT,0.00010349,42007.46,42006.86
2026-01-15 16:00:00.000,BTCUSDT,0.00010696,42014.78,42015.38
...
```

If you need to regenerate it:

```bash
python scripts/generate_mock_data.py
```

#### Step 2: Define the dataset schema in `config/datasets.yaml`

Add the following block under the `datasets:` key:

```yaml
  funding_rates:
    description: "8-hourly funding rate snapshots for BTCUSDT perpetual"
    source:
      type: file
      path: "data/mock_data/funding_rates.csv"
      format: csv
    schema:
      columns:
        - name: timestamp
          type: datetime
          nullable: false
          source_timezone: UTC
        - name: symbol
          type: str
          nullable: false
        - name: funding_rate
          type: float
          nullable: false
        - name: mark_price
          type: float
          nullable: false
        - name: index_price
          type: float
          nullable: false
    processing:
      dedup_keys: [timestamp, symbol]
      dedup_strategy: keep_last
      normalization_version: "1.0"
```

Key decisions:

- **dedup_keys**: `[timestamp, symbol]` ensures one record per symbol per timestamp
- **dedup_strategy**: `keep_last` retains the most recent duplicate
- **source_timezone**: `UTC` since our timestamps are already UTC

#### Step 3: Define features in `config/features.yaml`

```yaml
funding_rates:
  funding_factors:
    version: 1
    description: "Funding rate analytics"
    features:
      - name: funding_sma_10
        type: moving_average
        column: funding_rate
        window: 10
      - name: funding_ewma_20
        type: ewma
        column: funding_rate
        span: 20
      - name: mark_returns
        type: returns
        column: mark_price
      - name: mark_realized_vol_10
        type: realized_volatility
        column: mark_price
        window: 10
```

#### Step 4: Run the pipeline

```bash
adp ingest funding_rates
# Ingested 180 rows -> funding_rates_ing_YYYYMMDD_001

adp snapshot create funding_rates
# Created snapshot: funding_rates_snap_YYYYMMDD_001

adp features build funding_rates funding_factors
# Built features: funding_rates_funding_factors_fsnap_YYYYMMDD_001
```

#### Step 5: Verify

```python
import adp

df = adp.load_features("funding_rates", "funding_factors").collect()
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns}")
print(df.head(10))
```

Expected columns: `timestamp`, `symbol`, `funding_rate`, `mark_price`, `index_price`, `funding_sma_10`, `funding_ewma_20`, `mark_returns`, `mark_realized_vol_10`.

### 4.3 Source Path Override

**Goal:** Ingest from a file path different from what's configured in `datasets.yaml`.

This is useful when the same schema applies to multiple files (e.g., daily data drops):

```bash
# Override the source path
adp ingest ohlcv_btcusdt --path /path/to/other/ohlcv_file.csv --force

# Override both source type and path
adp ingest ohlcv_btcusdt --source file --path /tmp/new_data.csv --force
```

---

## 5. Quant Researcher Workflows

### 5.1 Dataset Discovery & Exploration

**Prerequisite:** Complete Section 4.1 (batch ingestion) first.

**Goal:** Discover what data is available and explore its contents.

```python
import adp
import polars as pl

# --- What datasets exist? ---
print("=== Registered Datasets ===")
print(adp.list_datasets())
# Shows: dataset_name, current_snapshot, schema_hash, created_at

# --- What snapshots exist for a dataset? ---
print("\n=== OHLCV Snapshots ===")
print(adp.list_snapshots("ohlcv_btcusdt"))
# Shows: snapshot_id, row_count, schema_hash, created_at

# --- What feature sets are available? ---
print("\n=== OHLCV Feature Sets ===")
print(adp.list_feature_sets("ohlcv_btcusdt"))
# Shows: feature_name, version, definition_hash, created_at

# --- Load and inspect a dataset ---
lf = adp.load_dataset("ohlcv_btcusdt")
print(f"\nSchema: {lf.collect_schema()}")
# Schema is available without collecting any data (lazy evaluation)

# Peek at first 5 rows
print(lf.head(5).collect())

# Descriptive statistics
print(lf.collect().describe())
```

### 5.2 SQL Analysis with DuckDB

**Goal:** Run SQL queries over snapshots and features without loading full datasets into memory.

```python
import adp

# --- OHLCV: 5-minute volume and price range analysis ---
result = adp.query_dataset(
    "ohlcv_btcusdt",
    """
    SELECT
        CAST(FLOOR(EXTRACT(MINUTE FROM timestamp) / 5) * 5 AS INTEGER) AS minute_group,
        COUNT(*) AS ticks,
        ROUND(AVG(volume), 4) AS avg_volume,
        ROUND(MAX(high) - MIN(low), 2) AS price_range,
        ROUND(AVG(close), 2) AS avg_close
    FROM dataset
    GROUP BY minute_group
    ORDER BY minute_group
    """
)
print("=== 5-Minute OHLCV Summary ===")
print(result)

# --- RFQ: Counterparty analysis ---
result = adp.query_dataset(
    "rfq_events",
    """
    SELECT
        counterparty,
        COUNT(DISTINCT rfq_id) AS total_rfqs,
        SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted,
        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected
    FROM dataset
    WHERE status IN ('accepted', 'rejected')
    GROUP BY counterparty
    ORDER BY total_rfqs DESC
    """
)
print("\n=== RFQ Counterparty Summary ===")
print(result)

# --- Feature query: filter for high-volatility moments ---
result = adp.query_features(
    "ohlcv_btcusdt",
    "candle_factors",
    """
    SELECT
        timestamp,
        close,
        rolling_vol_5,
        sma_10,
        close_returns
    FROM features
    WHERE rolling_vol_5 IS NOT NULL
    ORDER BY rolling_vol_5 DESC
    LIMIT 10
    """
)
print("\n=== Highest Volatility Moments ===")
print(result)
```

Note: in `query_dataset()`, the table is referenced as `dataset`. In `query_features()`, the table is referenced as `features`.

### 5.3 Backtest Matrix Construction

**Goal:** Build a feature matrix with forward returns for backtesting research.

```python
import adp
import polars as pl

# Build a backtest matrix with 1s, 5s, 10s, and 60s forward returns
backtest_lf = adp.build_backtest_matrix(
    dataset="ohlcv_btcusdt",
    feature_set="candle_factors",
    forward_return_periods=[1, 5, 10, 60],
    price_column="close",
    sort_column="timestamp",
)

bt_df = backtest_lf.collect()
print(f"Backtest matrix shape: {bt_df.shape}")
print(f"Columns: {bt_df.columns}")
# Expected: original 7 cols + 7 feature cols + 4 forward return cols = 18 columns

# Preview the matrix
print(bt_df.select([
    "timestamp", "close", "sma_10", "rolling_vol_5",
    "fwd_return_1", "fwd_return_5", "fwd_return_60",
]).head(10))
```

Forward returns are computed as:

```text
fwd_return_N = close[t + N] / close[t] - 1
```

The last N rows will have `null` forward returns (no future data available). This prevents look-ahead bias.

#### Factor-return correlation analysis

```python
# Drop nulls for clean correlation
clean = bt_df.drop_nulls(subset=[
    "rolling_vol_5", "sma_10", "close_returns", "fwd_return_5"
])

print("=== Factor-Return Correlations (fwd_return_5) ===")
for feature in ["rolling_vol_5", "sma_10", "ewma_20", "close_returns", "vwap"]:
    corr = clean.select(
        pl.corr(feature, "fwd_return_5").alias("correlation")
    )[0, 0]
    print(f"  {feature:20s} -> {corr:+.6f}")
```

### 5.4 Comparing Multiple Feature Sets

**Goal:** Build multiple feature sets from the same snapshot and compare them.

**Prerequisite:** Both `candle_factors` and `risk_factors` must be built (see Section 4.1).

```python
import adp
import polars as pl

# Load both feature sets
candle_df = adp.load_features("ohlcv_btcusdt", "candle_factors").collect()
risk_df = adp.load_features("ohlcv_btcusdt", "risk_factors").collect()

print(f"Candle factors: {candle_df.shape} — {candle_df.columns}")
print(f"Risk factors:   {risk_df.shape} — {risk_df.columns}")

# Join on timestamp to create a combined matrix
combined = candle_df.join(
    risk_df.select([
        "timestamp", "realized_vol_20", "close_zscore_30",
        "rolling_low_10", "rolling_high_10",
    ]),
    on="timestamp",
    how="inner",
)
print(f"\nCombined matrix: {combined.shape}")
print(combined.select([
    "timestamp", "close", "sma_10", "rolling_vol_5",
    "realized_vol_20", "close_zscore_30",
]).head(10))

# Compare volatility measures
print("\n=== Volatility Comparison ===")
vol_compare = combined.drop_nulls(
    subset=["rolling_vol_5", "realized_vol_20"]
).select([
    pl.col("rolling_vol_5").mean().alias("avg_rolling_vol_5"),
    pl.col("realized_vol_20").mean().alias("avg_realized_vol_20"),
    pl.corr("rolling_vol_5", "realized_vol_20").alias("correlation"),
])
print(vol_compare)
```

Both feature sets were built from the same underlying snapshot, so the join on `timestamp` is lossless. The `candle_factors` set focuses on trend signals (SMAs, EWMA, VWAP), while `risk_factors` focuses on risk metrics (realized volatility, z-score, rolling extremes).

---

## 6. Data Scientist Workflows

### 6.1 Cross-Dataset Analysis — RFQ-to-Trade Pipeline

**Prerequisite:** Ingest and snapshot both `rfq_events` and `trade_records` (Section 4.1).

**Goal:** Join RFQ events with trade records to analyze acceptance rates and execution slippage.

```python
import adp
import polars as pl

rfq_df = adp.load_dataset("rfq_events").collect()
trade_df = adp.load_dataset("trade_records").collect()

print(f"RFQ events:    {rfq_df.shape}")
print(f"Trade records: {trade_df.shape}")

# --- Analysis 1: Acceptance rate by counterparty ---
acceptance = (
    rfq_df
    .filter(pl.col("status").is_in(["accepted", "rejected"]))
    .group_by("counterparty")
    .agg([
        pl.col("rfq_id").n_unique().alias("total_rfqs"),
        (pl.col("status") == "accepted").sum().alias("accepted"),
        (pl.col("status") == "rejected").sum().alias("rejected"),
    ])
    .with_columns(
        (pl.col("accepted") / pl.col("total_rfqs") * 100)
        .round(1)
        .alias("acceptance_rate_pct")
    )
    .sort("acceptance_rate_pct", descending=True)
)
print("\n=== Acceptance Rate by Counterparty ===")
print(acceptance)

# --- Analysis 2: Execution slippage ---
# Join accepted RFQs with their trade executions
accepted_rfqs = rfq_df.filter(pl.col("status") == "accepted").select([
    "rfq_id", "quoted_price", "side", "counterparty", "instrument",
])

slippage = (
    trade_df
    .join(accepted_rfqs, on="rfq_id", suffix="_rfq")
    .with_columns(
        ((pl.col("price") - pl.col("quoted_price")) / pl.col("quoted_price") * 10000)
        .round(2)
        .alias("slippage_bps")
    )
)

print("\n=== Trade Execution Slippage ===")
print(slippage.select([
    "trade_id", "rfq_id", "instrument", "side",
    "quoted_price", "price", "slippage_bps",
]))

# Average slippage by side
print("\n=== Average Slippage by Side ===")
print(
    slippage
    .group_by("side")
    .agg([
        pl.col("slippage_bps").mean().round(2).alias("avg_slippage_bps"),
        pl.col("slippage_bps").std().round(2).alias("std_slippage_bps"),
        pl.col("slippage_bps").count().alias("trade_count"),
    ])
)
```

### 6.2 Complex SQL with DuckDB — Multi-Table Analysis

**Goal:** Use DuckDB SQL to query across multiple datasets simultaneously.

```python
import duckdb
from pathlib import Path
from adp.metadata.registry import MetadataRegistry

registry = MetadataRegistry(Path("metadata/adp_registry.db"))

# Get storage paths from metadata
rfq_snap = registry.get_snapshot(
    registry.get_dataset("rfq_events").current_snapshot
)
trade_snap = registry.get_snapshot(
    registry.get_dataset("trade_records").current_snapshot
)

# Create DuckDB views over the Parquet files
conn = duckdb.connect(":memory:")
conn.execute(
    f"CREATE VIEW rfq AS SELECT * FROM read_parquet('{rfq_snap.storage_path}/*.parquet')"
)
conn.execute(
    f"CREATE VIEW trades AS SELECT * FROM read_parquet('{trade_snap.storage_path}/*.parquet')"
)

# RFQ lifecycle timing analysis
result = conn.execute("""
    WITH rfq_lifecycle AS (
        SELECT
            rfq_id,
            instrument,
            counterparty,
            MIN(CASE WHEN status = 'requested' THEN timestamp END) AS requested_at,
            MIN(CASE WHEN status = 'quoted' THEN timestamp END) AS quoted_at,
            MIN(CASE WHEN status IN ('accepted', 'rejected') THEN timestamp END) AS decided_at,
            MAX(CASE WHEN status IN ('accepted', 'rejected') THEN status END) AS outcome
        FROM rfq
        GROUP BY rfq_id, instrument, counterparty
    )
    SELECT
        counterparty,
        COUNT(*) AS rfq_count,
        SUM(CASE WHEN outcome = 'accepted' THEN 1 ELSE 0 END) AS accepted,
        ROUND(AVG(EPOCH(quoted_at - requested_at)), 1) AS avg_quote_time_sec,
        ROUND(AVG(EPOCH(decided_at - quoted_at)), 1) AS avg_decision_time_sec
    FROM rfq_lifecycle
    GROUP BY counterparty
    ORDER BY rfq_count DESC
""").pl()

print("=== RFQ Lifecycle Timing by Counterparty ===")
print(result)

conn.close()
```

### 6.3 Building RFQ Analytics Features

**Prerequisite:** Ingest and snapshot `rfq_events`, then build `rfq_analytics` (Section 4.1).

**Goal:** Analyze pre-trade RFQ patterns using computed features.

```python
import adp
import polars as pl

# Load RFQ features
feat_df = adp.load_features("rfq_events", "rfq_analytics").collect()
print(f"Shape: {feat_df.shape}")
print(f"Columns: {feat_df.columns}")

# Preview features alongside raw data
print(feat_df.select([
    "event_id", "rfq_id", "status", "requested_qty",
    "qty_sma_5", "qty_rolling_vol_3",
]).head(15))

# Analyze quantity patterns by counterparty
print("\n=== Quantity Analytics by Counterparty ===")
summary = (
    feat_df
    .filter(pl.col("qty_sma_5").is_not_null())
    .group_by("counterparty")
    .agg([
        pl.col("requested_qty").mean().round(4).alias("avg_qty"),
        pl.col("qty_sma_5").mean().round(4).alias("avg_qty_sma"),
        pl.col("qty_rolling_vol_3").mean().round(4).alias("avg_qty_vol"),
    ])
)
print(summary)
```

---

## 7. Support & Operations Workflows

### 7.1 Platform Health Check

**Goal:** Audit the entire platform state — all datasets, ingestions, snapshots, and features.

```python
from adp.metadata.registry import MetadataRegistry
from pathlib import Path

registry = MetadataRegistry(Path("metadata/adp_registry.db"))

print("=" * 60)
print("ADP PLATFORM HEALTH CHECK")
print("=" * 60)

datasets = registry.list_datasets()
print(f"\nRegistered datasets: {len(datasets)}")

for ds in datasets:
    ingestions = registry.list_ingestions(ds.dataset_name)
    snapshots = registry.list_snapshots(ds.dataset_name)
    feat_defs = registry.list_feature_definitions(ds.dataset_name)
    has_current = "OK" if ds.current_snapshot else "MISSING"

    print(f"\n--- {ds.dataset_name} ---")
    print(f"  Status:        {has_current}")
    print(f"  Ingestions:     {len(ingestions)}")
    print(f"  Snapshots:      {len(snapshots)}")
    print(f"  Schema hash:    {ds.schema_hash[:16]}...")

    if ds.current_snapshot:
        snap = registry.get_snapshot(ds.current_snapshot)
        print(f"  Current snap:   {snap.snapshot_id} ({snap.row_count} rows)")

    # Feature sets
    seen = set()
    for fd in feat_defs:
        if fd.feature_name not in seen:
            seen.add(fd.feature_name)
            fsnaps = registry.list_feature_snapshots(ds.dataset_name, fd.feature_name)
            print(f"  Feature set:    {fd.feature_name} v{fd.version} ({len(fsnaps)} snapshot(s))")
```

### 7.2 Storage Integrity Audit

**Goal:** Verify that Parquet files on disk match what the metadata registry expects.

```python
from adp.metadata.registry import MetadataRegistry
from pathlib import Path
import json

registry = MetadataRegistry(Path("metadata/adp_registry.db"))

print("=== Storage Integrity Audit ===\n")
issues = 0

for ds in registry.list_datasets():
    # Check dataset snapshots
    for snap in registry.list_snapshots(ds.dataset_name):
        storage_path = Path(snap.storage_path)
        parquet_files = list(storage_path.glob("*.parquet")) if storage_path.exists() else []
        metadata_file = storage_path / "_metadata.json"

        if not parquet_files:
            print(f"MISSING  {snap.snapshot_id}  -> {storage_path}")
            issues += 1
            continue

        if metadata_file.exists():
            with open(metadata_file) as f:
                meta = json.load(f)
            if meta.get("row_count") != snap.row_count:
                print(f"MISMATCH {snap.snapshot_id}  row_count: meta={meta.get('row_count')} db={snap.row_count}")
                issues += 1
                continue

        print(f"OK       {snap.snapshot_id}  ({snap.row_count} rows)")

    # Check feature snapshots
    for fsnap in registry.list_feature_snapshots(ds.dataset_name):
        storage_path = Path(fsnap.storage_path)
        parquet_files = list(storage_path.glob("*.parquet")) if storage_path.exists() else []

        if not parquet_files:
            print(f"MISSING  {fsnap.feature_snapshot_id}  -> {storage_path}")
            issues += 1
        else:
            print(f"OK       {fsnap.feature_snapshot_id}  ({fsnap.row_count} rows)")

print(f"\nAudit complete. Issues found: {issues}")
```

### 7.3 Error Scenarios & Troubleshooting

#### Non-existent dataset

```bash
adp ingest nonexistent_dataset
# Error: Dataset 'nonexistent_dataset' not found in config.
# Available: ['funding_rates', 'ohlcv_btcusdt', 'rfq_events', 'trade_records']
```

**Resolution:** Check the dataset name matches a key in `config/datasets.yaml`.

#### No ingestions for a dataset

```bash
adp snapshot create some_dataset
# Error: No ingestions for 'some_dataset'. Run 'adp ingest' first.
```

**Resolution:** Run `adp ingest <dataset>` before creating a snapshot.

#### Non-existent feature set

```bash
adp features build ohlcv_btcusdt nonexistent_factors
# Error: Feature set 'nonexistent_factors' not found for 'ohlcv_btcusdt'.
# Available: ['candle_factors', 'risk_factors']
```

**Resolution:** Check the feature set name matches a key under the dataset in `config/features.yaml`.

#### Features not yet built

```bash
adp features load rfq_events rfq_analytics
# Error: No feature snapshots for 'rfq_analytics' on dataset 'rfq_events'
```

**Resolution:** Run `adp features build rfq_events rfq_analytics` first.

#### Schema validation errors

If a source file's columns don't match `datasets.yaml`, the snapshot creation will fail with a `SchemaValidationError`. To fix:

1. Check if the source data changed format
2. Update `config/datasets.yaml` to match the new schema
3. Bump `normalization_version` in the processing config
4. Re-ingest with `--force` and create a new snapshot

---

## 8. Reference

### 8.1 Feature Strategies

ADP includes 11 built-in feature computation strategies. All operate on Polars LazyFrames.

| Type Key | Strategy | Required Params | Formula |
| -------- | -------- | --------------- | ------- |
| `rolling_std` | Rolling Standard Deviation | `column`, `window` | `std(col, window)` |
| `moving_average` | Simple Moving Average (SMA) | `column`, `window` | `mean(col, window)` |
| `ewma` | Exponential Weighted Moving Average | `column`, `span` | `ewm_mean(col, span)` |
| `rolling_min` | Rolling Minimum | `column`, `window` | `min(col, window)` |
| `rolling_max` | Rolling Maximum | `column`, `window` | `max(col, window)` |
| `vwap` | Volume-Weighted Average Price | `price_column`, `volume_column`, optional `window` | `cumsum(P*V) / cumsum(V)` |
| `returns` | Simple Returns | `column` | `(P[t] - P[t-1]) / P[t-1]` |
| `log_returns` | Log Returns | `column` | `ln(P[t] / P[t-1])` |
| `z_score` | Rolling Z-Score | `column`, `window` | `(x - mean) / std` |
| `realized_volatility` | Realized Volatility | `column`, `window` | `sqrt(sum(returns^2, window))` |
| `cross_sectional_rank` | Cross-Sectional Rank | `column`, `group_by` | `rank(col).over(group)` |

**Notes:**

- The first `window - 1` rows of any rolling computation will be `null`.
- VWAP without a `window` parameter computes cumulative VWAP.
- `cross_sectional_rank` ranks values within groups (e.g., rank symbols at each timestamp).

### 8.2 Configuration Schema

#### `datasets.yaml`

```yaml
datasets:
  <dataset_name>:
    description: "Human-readable description"
    source:
      type: file | athena           # Data source type
      path: "path/to/file.csv"      # For file sources
      format: csv | json | parquet | txt  # File format
      # Athena-specific:
      # database: "db_name"
      # query: "SELECT ..."
      # s3_output: "s3://bucket/path"
    schema:
      columns:
        - name: column_name
          type: str | int | float | bool | datetime | date | Decimal
          nullable: true | false
          source_timezone: UTC       # For datetime columns (optional)
    processing:
      dedup_keys: [col1, col2]       # Columns to deduplicate on
      dedup_strategy: keep_first | keep_last
      normalization_version: "1.0"   # Bump when processing logic changes
```

#### `features.yaml`

```yaml
<dataset_name>:
  <feature_set_name>:
    version: 1                       # Integer, increment when features change
    description: "Human-readable description"
    features:
      - name: output_column_name     # Name of the computed column
        type: rolling_std            # Strategy key (see table above)
        column: input_column         # Source column
        window: 5                    # Strategy-specific parameter
```

### 8.3 CLI Command Reference

```text
adp init [--config-dir PATH] [--data-dir PATH] [--metadata-dir PATH]
    Initialize platform directory structure and metadata database.

adp ingest DATASET [--source TYPE] [--path PATH] [--force]
    Ingest data for a configured dataset.

adp snapshot create DATASET
    Create a normalized snapshot from all raw ingestions.

adp snapshot list DATASET
    List all snapshots for a dataset.

adp snapshot show SNAPSHOT_ID [--lineage]
    Show snapshot details. Use --lineage to see source ingestions.

adp features build DATASET FEATURE_SET [--snapshot ID]
    Materialize features. Uses current snapshot unless --snapshot is specified.

adp features list DATASET
    List all feature sets for a dataset.

adp features show DATASET FEATURE_SET
    Show all feature snapshots for a feature set.

adp features load DATASET FEATURE_SET [--head N]
    Load and display feature data (default: 10 rows).
```

All commands accept `--verbose` / `-v` for debug logging.

### 8.4 Python API Reference

```python
# Load data as Polars LazyFrame (lazy — no data loaded until .collect())
load_dataset(name, snapshot_id=None) -> pl.LazyFrame
load_features(dataset, feature_set, snapshot_id=None) -> pl.LazyFrame

# Run SQL over Parquet via DuckDB (returns eager DataFrame)
query_dataset(name, sql, snapshot_id=None) -> pl.DataFrame    # table: "dataset"
query_features(dataset, feature_set, sql, snapshot_id=None) -> pl.DataFrame  # table: "features"

# Discovery (returns eager DataFrames)
list_datasets() -> pl.DataFrame
list_snapshots(dataset) -> pl.DataFrame
list_feature_sets(dataset) -> pl.DataFrame

# Backtesting
build_backtest_matrix(
    dataset, feature_set,
    forward_return_periods=[1, 5, 10],
    price_column="close",
    sort_column="timestamp",
    group_column=None,          # For multi-asset data (e.g., "symbol")
) -> pl.LazyFrame
```

### 8.5 Snapshot Pinning for Reproducibility

To ensure reproducible research, pin specific snapshot IDs rather than relying on "current":

```python
import adp

# Pin to exact dataset snapshot
lf = adp.load_dataset("ohlcv_btcusdt", snapshot_id="ohlcv_btcusdt_snap_20260302_001")

# Pin to exact feature snapshot
feat_lf = adp.load_features(
    "ohlcv_btcusdt", "candle_factors",
    snapshot_id="ohlcv_btcusdt_candle_factors_fsnap_20260302_001",
)
```

This ensures your analysis produces identical results regardless of newer data ingestions or feature rebuilds. Document the snapshot IDs in your research notebooks for full reproducibility.

---

## 9. JupyterLab Research Environment

JupyterLab is the primary interactive environment for quant research on ADP. It provides a notebook-based workflow for data exploration, feature analysis, visualization, and backtesting — all powered by the ADP Python API.

### 9.1 Setting Up JupyterLab

Install JupyterLab alongside ADP using the optional `notebooks` dependency group:

```bash
pip install -e ".[notebooks]"
```

This installs `jupyterlab` and `matplotlib`. You can also install them manually: `pip install jupyterlab matplotlib`.

Launch from the project root (so ADP can find its config and data directories):

```bash
cd quant-research-platform
jupyter lab
```

JupyterLab opens in your browser. Navigate to the `notebooks/` folder to find the example notebooks.

### 9.2 Research Workflow in JupyterLab

The standard research workflow combines CLI pipeline commands with interactive Python analysis:

```text
Step 1: Pipeline Setup     →  Run adp init/ingest/snapshot/features from notebook
Step 2: Discover           →  list_datasets(), list_snapshots(), list_feature_sets()
Step 3: Load & Explore     →  load_dataset(), load_features() as LazyFrames
Step 4: Analyze            →  Polars transformations, DuckDB SQL, statistics
Step 5: Visualize          →  matplotlib charts and tables
Step 6: Backtest Matrix    →  build_backtest_matrix() with forward returns
Step 7: Iterate            →  Modify features.yaml, rebuild, compare
```

Each notebook is self-contained — it runs the pipeline setup in its first cells so you can start from a clean state.

### 9.3 Notebook Setup Pattern

Every research notebook should start with this standard pattern:

```python
# Cell 1: Pipeline setup
import subprocess
import os
from pathlib import Path

# Navigate to the project root (parent of notebooks/)
if os.path.basename(os.getcwd()) == "notebooks":
    os.chdir("..")
elif not Path("config/datasets.yaml").exists():
    root = Path.cwd()
    while not (root / "config" / "datasets.yaml").exists() and root != root.parent:
        root = root.parent
    os.chdir(root)

subprocess.run(["adp", "init"], capture_output=True, check=True)
subprocess.run(["adp", "ingest", "ohlcv_btcusdt", "--force"], capture_output=True, check=True)
subprocess.run(["adp", "snapshot", "create", "ohlcv_btcusdt"], capture_output=True, check=True)
subprocess.run(["adp", "features", "build", "ohlcv_btcusdt", "candle_factors"], capture_output=True, check=True)
```

```python
# Cell 2: Imports and display config
import polars as pl
import matplotlib.pyplot as plt
from adp import (
    load_dataset,
    load_features,
    query_dataset,
    query_features,
    list_datasets,
    list_snapshots,
    list_feature_sets,
    build_backtest_matrix,
)

pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)
```

### 9.4 Example Notebooks

Four example notebooks are provided in the `notebooks/` directory. Each demonstrates a practical, real-world quant research workflow using the platform's mock data.

| Notebook | Description | Datasets | Audience |
| -------- | ----------- | -------- | -------- |
| `01_platform_quickstart.ipynb` | End-to-end platform walkthrough — initialization, data discovery, feature loading, SQL queries, and visualization | `ohlcv_btcusdt` + `candle_factors` | New users, all roles |
| `02_factor_research_backtest.ipynb` | Factor research workflow — backtest matrices, factor-return correlations, volatility regime analysis, signal decay | `ohlcv_btcusdt` + `candle_factors` + `risk_factors` | Quant researchers |
| `03_rfq_trade_analysis.ipynb` | Cross-dataset analysis — RFQ lifecycle, counterparty acceptance rates, execution slippage, flow analytics | `rfq_events` + `trade_records` + `rfq_analytics` + `trade_factors` | Data scientists, desk analysts |
| `04_funding_rate_risk_monitor.ipynb` | Funding rate risk monitoring — regime detection, basis analysis, annualized cost estimation, aggregated risk metrics | `funding_rates` + `funding_factors` | Risk analysts, portfolio managers |

**How to use the notebooks:**

1. Launch JupyterLab: `jupyter lab`
2. Open a notebook from the `notebooks/` folder
3. Run all cells in order (Cell → Run All) — each notebook initializes its own pipeline
4. Modify and experiment — change parameters, add new analyses, pin snapshots

**Notebook 01 — Platform Quickstart** covers the basics that all other notebooks build on. Start here if you are new to the platform. It walks through initialization, data loading (lazy vs eager), DuckDB SQL queries, and matplotlib visualization.

**Notebook 02 — Factor Research & Backtesting** is the core quant workflow. It builds a backtest matrix with forward returns at multiple horizons (1s, 5s, 10s, 60s), computes factor-return correlations, combines candle and risk feature sets, and analyzes signal decay across horizons. It also demonstrates volatility regime analysis — splitting the data into high/low volatility regimes based on realized volatility.

**Notebook 03 — RFQ & Trade Analysis** demonstrates cross-dataset joins. It links RFQ lifecycle events with trade execution records to measure counterparty acceptance rates and execution slippage in basis points. This is a practical OTC desk analytics workflow.

**Notebook 04 — Funding Rate Risk Monitor** analyzes 60 days of 8-hourly perpetual funding rates. It detects funding rate regimes (positive/neutral/negative), computes the mark-index basis, estimates annualized funding costs, and produces daily aggregated risk metrics using DuckDB SQL.

### 9.5 Running CLI Commands from Notebooks

ADP CLI commands can be run from notebook cells using Python's `subprocess` module:

```python
import subprocess

# Run a single command
subprocess.run(["adp", "ingest", "ohlcv_btcusdt", "--force"], check=True)

# Run multiple pipeline steps
for dataset in ["rfq_events", "trade_records"]:
    subprocess.run(["adp", "ingest", dataset, "--force"], check=True)
    subprocess.run(["adp", "snapshot", "create", dataset], check=True)
```

Use `--force` on `adp ingest` to allow re-ingestion when re-running notebook cells. The `check=True` parameter raises an exception if any command fails.

### 9.6 Visualization Patterns

ADP notebooks use matplotlib for financial data visualization. Common patterns:

```python
import matplotlib.pyplot as plt

# Multi-panel chart (price + indicators + returns)
fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

# Panel 1: Price with moving averages
axes[0].plot(timestamps, close, label="Close", alpha=0.5)
axes[0].plot(timestamps, sma_10, label="SMA(10)")
axes[0].legend()
axes[0].set_title("Price & Moving Averages")
axes[0].grid(True, alpha=0.3)

# Panel 2: Volatility
axes[1].plot(timestamps, rolling_vol, color="orange")
axes[1].set_title("Rolling Volatility")

# Panel 3: Returns (green/red bars)
colors = ["green" if r >= 0 else "red" for r in returns]
axes[2].bar(range(len(returns)), returns, color=colors, alpha=0.6)
axes[2].set_title("Returns")

plt.tight_layout()
plt.show()
```

### 9.7 Best Practices

**Reproducibility:**

- Pin snapshot IDs in published research — use `snapshot_id=` parameters
- Record snapshot IDs at the top of each notebook in a markdown cell
- Use `list_snapshots()` and `list_feature_sets()` to document exact versions

**Performance:**

- Use lazy evaluation — call `.collect()` as late as possible
- Use `query_dataset()` / `query_features()` for SQL-based analysis (DuckDB pushes predicates to Parquet)
- Avoid loading full datasets when you only need a subset — filter with Polars or SQL

**Avoiding look-ahead bias:**

- Forward returns in `build_backtest_matrix()` produce `null` for the last N rows — do not fill these
- Features use only past and current values (rolling windows look backward)
- Do not build features on a snapshot that includes data from after the backtest period

**Organizing research:**

- Start with `01_platform_quickstart.ipynb` to verify your environment
- Create new notebooks in `notebooks/` for each research question
- Name notebooks descriptively: `05_my_research_topic.ipynb`
- Use the standard setup pattern (Section 9.3) in every new notebook

---

*Cross-references: [Design Document](./03-design-document.md) · [User Stories](./02-user-stories.md) · [Quant Infrastructure](./05-quant-infrastructure.md) · [Test Strategy](./04-test-strategy.md)*
