# Athena Data Platform (ADP) — Test Strategy

> **Version**: 1.0
> **Date**: 2026-03-02
> **Status**: Draft
> **Owner**: Tech Lead
> **Audience**: Platform Engineering, QA
> **Related**: [Design Document](./03-design-document.md) · [User Stories](./02-user-stories.md)

---

## Table of Contents

1. [Test Philosophy & Principles](#1-test-philosophy--principles)
2. [Test Pyramid](#2-test-pyramid)
3. [Coverage Targets](#3-coverage-targets)
4. [Test Infrastructure](#4-test-infrastructure)
5. [Unit Test Plan](#5-unit-test-plan)
6. [Integration Test Plan](#6-integration-test-plan)
7. [End-to-End Test Plan](#7-end-to-end-test-plan)
8. [Test Fixtures & Synthetic Data](#8-test-fixtures--synthetic-data)
9. [Mocking Strategy](#9-mocking-strategy)
10. [Test Matrix](#10-test-matrix)
11. [CI/CD Testing Pipeline](#11-cicd-testing-pipeline)
12. [Regression Testing](#12-regression-testing)
13. [Performance Testing](#13-performance-testing)
14. [Change Log](#14-change-log)

---

## 1. Test Philosophy & Principles

### 1.1 Core Philosophy

Testing in ADP is a first-class concern, not an afterthought. The platform deals with data lineage, immutability, and reproducibility — properties that are only trustworthy if verified by automated tests.

### 1.2 Principles

| Principle | Description |
|-----------|-------------|
| **Determinism** | Every test produces the same result on every run. No random data, no time-dependent assertions without mocking. |
| **Isolation** | Tests do not depend on each other. Each test creates its own state and cleans up after itself. |
| **Speed** | Unit tests run in milliseconds. Integration tests run in seconds. E2E tests are the only tests allowed to take longer. |
| **Clarity** | Test names describe the scenario and expected outcome. Failed tests produce actionable error messages. |
| **Realism** | Synthetic test data mirrors real data shapes (trade records, OHLCV candles) with realistic value distributions. |
| **Reproducibility as a test** | The platform's core promise is reproducibility. Tests explicitly verify that identical inputs produce identical outputs. |

---

## 2. Test Pyramid

```
                    ┌───────────┐
                    │   E2E     │   ~5% of tests
                    │  (3-5)    │   Full pipeline verification
                    ├───────────┤
                    │           │
                  ┌─┤Integration├─┐   ~25% of tests
                  │ │  (15-25)  │ │   Component interaction
                  │ ├───────────┤ │
                  │ │           │ │
                ┌─┤ │   Unit    │ ├─┐   ~70% of tests
                │ │ │  (50-80)  │ │ │   Individual functions/classes
                │ │ └───────────┘ │ │
                └─┘               └─┘
```

| Level | Count (est.) | Execution Time | Scope |
|-------|:------------:|:--------------:|-------|
| Unit | 50–80 | <10s total | Single function, single class |
| Integration | 15–25 | <60s total | Component pipelines, SQLite, file I/O |
| E2E | 3–5 | <120s total | Full pipeline, ingestion to feature loading |

---

## 3. Coverage Targets

### 3.1 Per-Layer Targets

| Layer | Line Coverage | Branch Coverage | Notes |
|-------|:------------:|:---------------:|-------|
| `config.py` | ≥95% | ≥90% | Config parsing is critical — must cover all error paths |
| `ingestion/` | ≥90% | ≥85% | Athena strategy harder to unit-test (mock-heavy) |
| `processing/` | ≥95% | ≥90% | Schema validation and normalization are core correctness |
| `storage/` | ≥90% | ≥85% | File I/O tested via integration tests |
| `metadata/` | ≥95% | ≥90% | Registry is the source of truth — must be bulletproof |
| `features/` | ≥95% | ≥90% | Feature determinism is a core guarantee |
| `api.py` | ≥90% | ≥85% | User-facing API must handle all error cases |
| `cli.py` | ≥80% | ≥75% | CLI is thin; tested via integration tests |

### 3.2 Global Target

- **Overall line coverage**: ≥90%
- **Overall branch coverage**: ≥85%
- **Enforced in CI**: Build fails below these thresholds

---

## 4. Test Infrastructure

### 4.1 Tools

| Tool | Purpose | Configuration |
|------|---------|---------------|
| **pytest** ~=8.3 | Test runner and framework | `pyproject.toml` `[tool.pytest.ini_options]` |
| **pytest-cov** ~=6.0 | Coverage reporting | `--cov=src/adp --cov-report=term-missing` |
| `tmp_path` (built-in) | Temporary file fixtures | Built-in pytest fixture |
| **moto** ~=5.0 (optional) | AWS service mocking | Mock Athena responses |
| **polars.testing** (built-in) | DataFrame assertion utilities | `assert_frame_equal` |
| **time-machine** ~=2.16 | Time mocking | Deterministic timestamp tests |
| **ruff** ~=0.9 | Linting | `pyproject.toml` `[tool.ruff]` |
| **mypy** ~=1.14 | Static type checking | `pyproject.toml` `[tool.mypy]` |

### 4.2 Directory Structure

```
tests/
├── conftest.py                    # Shared fixtures, synthetic data generators
├── unit/
│   ├── test_config.py             # Config loading and validation
│   ├── test_schema.py             # Pydantic model factory and schema hashing
│   ├── test_metadata.py           # MetadataRegistry CRUD operations
│   ├── test_ingestion.py          # Ingestion strategies (mocked I/O)
│   ├── test_normalizer.py         # Normalization pipeline steps
│   ├── test_dedup.py              # Deduplication engine
│   ├── test_snapshot.py           # Snapshot ID generation, hash computation
│   ├── test_feature_definitions.py # Feature YAML parsing
│   ├── test_feature_strategies.py  # Individual feature computations
│   └── test_feature_materializer.py # Materialiser logic (mocked storage)
├── integration/
│   ├── test_ingestion_pipeline.py  # Ingest → raw storage → metadata
│   ├── test_snapshot_pipeline.py   # Raw → normalize → snapshot → metadata
│   ├── test_feature_pipeline.py    # Snapshot → features → metadata
│   ├── test_cli.py                 # CLI command execution
│   └── test_duckdb_queries.py      # DuckDB SQL over Parquet files
└── e2e/
    ├── test_full_pipeline.py       # Ingest → normalize → snapshot → features → load
    ├── test_reproducibility.py     # Same input → same output verification
    └── test_snapshot_isolation.py  # New snapshots don't affect old ones
```

### 4.3 pytest Configuration

```toml
# pyproject.toml

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = [
    "-v",
    "--strict-markers",
    "--tb=short",
    "--cov=src/adp",
    "--cov-report=term-missing",
    "--cov-fail-under=90",
]
markers = [
    "unit: Unit tests (fast, isolated)",
    "integration: Integration tests (file I/O, SQLite)",
    "e2e: End-to-end tests (full pipeline)",
    "slow: Tests that take >5 seconds",
]
filterwarnings = [
    "error",
    "ignore::DeprecationWarning",
]
```

---

## 5. Unit Test Plan

### 5.1 Config Parsing — `test_config.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_load_datasets_config_valid` | Load a valid `datasets.yaml`, verify all fields parsed correctly | P0 |
| `test_load_datasets_config_missing_file` | Missing YAML raises `ConfigError` with path info | P0 |
| `test_load_datasets_config_malformed_yaml` | Invalid YAML syntax raises `ConfigError` | P0 |
| `test_load_datasets_config_missing_required_field` | Missing column name/type raises validation error | P0 |
| `test_load_datasets_config_invalid_type` | Unknown column type (e.g., "complex") raises error | P1 |
| `test_load_features_config_valid` | Load a valid `features.yaml`, verify structure | P0 |
| `test_load_features_config_unknown_feature_type` | Unknown feature type raises `ConfigError` | P1 |
| `test_config_path_override` | Config from custom path loads correctly | P1 |

### 5.2 Schema Validation — `test_schema.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_build_schema_model_creates_pydantic_model` | Generated model has correct fields and types | P0 |
| `test_schema_model_validates_good_data` | Valid record passes validation | P0 |
| `test_schema_model_rejects_wrong_type` | Wrong column type raises `ValidationError` | P0 |
| `test_schema_model_rejects_null_in_non_nullable` | Null in non-nullable column raises error | P0 |
| `test_schema_model_allows_null_in_nullable` | Null in nullable column passes | P0 |
| `test_schema_hash_deterministic` | Same schema → same hash on repeated calls | P0 |
| `test_schema_hash_changes_on_column_add` | Adding a column changes the hash | P0 |
| `test_schema_hash_changes_on_column_rename` | Renaming a column changes the hash | P0 |
| `test_schema_hash_changes_on_type_change` | Changing column type changes the hash | P0 |
| `test_schema_hash_stable_across_restarts` | Hash value matches a known constant | P1 |

### 5.3 Metadata Registry — `test_metadata.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_registry_init_creates_tables` | All 7 tables exist after initialization | P0 |
| `test_registry_init_idempotent` | Running init twice doesn't error or duplicate tables | P0 |
| `test_register_dataset` | Dataset record is inserted and retrievable | P0 |
| `test_register_dataset_duplicate` | Duplicate dataset name raises error | P0 |
| `test_log_ingestion` | Ingestion record is inserted and retrievable | P0 |
| `test_create_snapshot` | Snapshot record is inserted and retrievable | P0 |
| `test_link_snapshot_lineage` | Lineage record links snapshot to ingestion | P0 |
| `test_get_snapshot_lineage` | Returns all ingestions for a snapshot | P0 |
| `test_update_current_snapshot` | `current_snapshot` field updated correctly | P0 |
| `test_transaction_rollback_on_error` | Failed write leaves no partial records | P0 |
| `test_register_feature_definition` | Feature definition inserted and retrievable | P0 |
| `test_create_feature_snapshot` | Feature snapshot inserted and retrievable | P0 |
| `test_link_feature_lineage` | Feature lineage links feature snap to dataset snap | P0 |
| `test_list_datasets` | Returns all registered datasets | P1 |
| `test_list_snapshots` | Returns all snapshots for a dataset, ordered by date | P1 |
| `test_list_feature_snapshots` | Returns all feature snapshots for a feature | P1 |

### 5.4 Ingestion Strategies — `test_ingestion.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_athena_strategy_implements_protocol` | `AthenaIngestionStrategy` is `IngestionStrategy` | P0 |
| `test_file_strategy_implements_protocol` | `FileIngestionStrategy` is `IngestionStrategy` | P0 |
| `test_file_ingest_csv` | CSV file ingested, row count correct | P0 |
| `test_file_ingest_json` | JSON file ingested, row count correct | P0 |
| `test_file_ingest_parquet` | Parquet file ingested, row count correct | P0 |
| `test_file_ingest_txt_delimited` | TXT delimited file ingested | P1 |
| `test_ingestion_result_fields` | `IngestionResult` contains all required fields | P0 |
| `test_file_ingest_missing_file` | Missing file raises `IngestionError` | P0 |
| `test_file_ingest_empty_file` | Empty file raises `IngestionError` | P1 |

### 5.5 Normalization — `test_normalizer.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_timezone_normalize_utc_passthrough` | UTC timestamps unchanged | P0 |
| `test_timezone_normalize_eastern_to_utc` | US/Eastern timestamps converted to UTC | P0 |
| `test_timezone_normalize_naive_assumed_source` | Naive timestamps treated as source timezone | P0 |
| `test_timezone_preserves_microseconds` | Microsecond precision maintained | P1 |
| `test_pipeline_chains_steps` | Pipeline runs validate → tz → dedup in order | P0 |
| `test_pipeline_returns_lazyframe` | Pipeline output is LazyFrame, not DataFrame | P0 |
| `test_pipeline_logs_row_counts` | Each step logs input/output row counts | P1 |

### 5.6 Deduplication — `test_dedup.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_dedup_removes_duplicates` | Duplicate rows removed by key columns | P0 |
| `test_dedup_keep_last` | With `keep_last`, most recent duplicate retained | P0 |
| `test_dedup_keep_first` | With `keep_first`, earliest duplicate retained | P0 |
| `test_dedup_no_key_passthrough` | No dedup key configured → no rows removed | P0 |
| `test_dedup_returns_lazyframe` | Output is LazyFrame | P0 |
| `test_dedup_count_logged` | Number of removed rows is reported | P1 |
| `test_dedup_multi_column_key` | Dedup on composite key (symbol + trade_id) | P1 |

### 5.7 Snapshot — `test_snapshot.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_snapshot_id_format` | ID matches `{dataset}_snap_{YYYYMMDD}_{seq}` | P0 |
| `test_snapshot_id_sequence_increments` | Multiple snapshots same day get incrementing seq | P0 |
| `test_snapshot_id_no_collision` | Two different datasets can have snapshots on same day | P1 |
| `test_feature_snapshot_id_format` | ID matches `{dataset}_{fset}_fsnap_{YYYYMMDD}_{seq}` | P0 |

### 5.8 Feature Definitions — `test_feature_definitions.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_parse_feature_set` | Valid feature set parsed into typed objects | P0 |
| `test_parse_rolling_std` | Rolling std definition parsed with column and window | P0 |
| `test_parse_vwap` | VWAP definition parsed with price and volume columns | P0 |
| `test_parse_invalid_feature_type` | Unknown feature type raises error | P0 |
| `test_definition_hash_deterministic` | Same definition → same hash | P0 |
| `test_definition_hash_changes_on_param_change` | Changing window changes hash | P0 |
| `test_definition_hash_changes_on_add_feature` | Adding a feature changes hash | P1 |

### 5.9 Feature Strategies — `test_feature_strategies.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_rolling_std_computation` | Rolling std matches expected values on known data | P0 |
| `test_rolling_std_nan_at_boundary` | First `window-1` rows are NaN | P0 |
| `test_moving_average_computation` | SMA matches expected values | P0 |
| `test_ewma_computation` | EWMA matches expected values | P0 |
| `test_rolling_min_computation` | Rolling min matches expected values | P0 |
| `test_rolling_max_computation` | Rolling max matches expected values | P0 |
| `test_vwap_computation` | VWAP = sum(price*vol) / sum(vol) for known data | P0 |
| `test_returns_computation` | Simple returns match expected values | P0 |
| `test_log_returns_computation` | Log returns match expected values | P0 |
| `test_returns_first_row_nan` | First row is NaN (no prior price) | P0 |
| `test_strategy_returns_lazyframe` | All strategies return LazyFrame, not DataFrame | P0 |
| `test_strategy_appends_column` | Output has original columns + new feature column | P0 |

### 5.10 Feature Materialiser — `test_feature_materializer.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_materialiser_loads_snapshot` | Materialiser reads from the correct snapshot path | P0 |
| `test_materialiser_applies_all_features` | All features in set appear as columns | P0 |
| `test_materialiser_single_collect` | LazyFrame collected only once (verify via execution plan) | P1 |
| `test_materialiser_generates_snapshot_id` | Feature snapshot ID generated correctly | P0 |
| `test_materialiser_computes_definition_hash` | Definition hash matches expected value | P0 |

---

## 6. Integration Test Plan

### 6.1 Ingestion Pipeline — `test_ingestion_pipeline.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_ingest_csv_to_raw_storage` | CSV file → `data/raw/` Parquet + metadata record | P0 |
| `test_ingest_parquet_to_raw_storage` | Parquet file → `data/raw/` + metadata record | P0 |
| `test_ingest_logs_metadata` | After ingestion, `raw_ingestions` table has correct record | P0 |
| `test_ingest_failed_no_metadata` | Failed ingestion leaves no orphan metadata records | P0 |
| `test_ingest_idempotency_warning` | Re-ingesting same source shows warning | P2 |

### 6.2 Snapshot Pipeline — `test_snapshot_pipeline.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_raw_to_normalized_snapshot` | Raw ingestion → normalization → snapshot in `data/normalized/` | P0 |
| `test_snapshot_metadata_registered` | `snapshots` table has correct record after creation | P0 |
| `test_snapshot_lineage_linked` | `snapshot_lineage` links snapshot to source ingestion | P0 |
| `test_current_snapshot_updated` | `datasets.current_snapshot` points to new snapshot | P0 |
| `test_schema_enforcement_rejects_bad_data` | Malformed raw data → `SchemaValidationError` → no snapshot | P0 |
| `test_multiple_snapshots_coexist` | Creating a second snapshot doesn't affect the first | P0 |
| `test_snapshot_parquet_readable` | Snapshot Parquet files are valid and readable by Polars | P0 |

### 6.3 Feature Pipeline — `test_feature_pipeline.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_snapshot_to_features` | Dataset snapshot → feature materialisation → `data/features/` | P0 |
| `test_feature_metadata_registered` | `feature_definitions`, `feature_snapshots`, `feature_lineage` populated | P0 |
| `test_feature_parquet_readable` | Feature Parquet files are valid and readable by Polars | P0 |
| `test_feature_columns_present` | Feature DataFrame contains all defined feature columns | P0 |
| `test_feature_from_specific_snapshot` | Building features from a non-current snapshot works | P1 |
| `test_multiple_feature_sets` | Two different feature sets for the same dataset coexist | P1 |

### 6.4 CLI — `test_cli.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_cli_init` | `adp init` creates directories and database | P0 |
| `test_cli_init_idempotent` | `adp init` twice succeeds without errors | P0 |
| `test_cli_ingest` | `adp ingest <dataset>` produces raw data and metadata | P1 |
| `test_cli_snapshot_create` | `adp snapshot create <dataset>` creates snapshot | P1 |
| `test_cli_snapshot_list` | `adp snapshot list <dataset>` shows snapshots | P1 |
| `test_cli_features_build` | `adp features build <ds> <fset>` produces features | P1 |
| `test_cli_features_list` | `adp features list <dataset>` shows feature sets | P1 |
| `test_cli_error_formatting` | Invalid command produces user-friendly error (no traceback) | P1 |

### 6.5 DuckDB Queries — `test_duckdb_queries.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_query_dataset_basic_sql` | `SELECT * FROM dataset LIMIT 10` returns correct rows | P1 |
| `test_query_dataset_aggregation` | `SELECT date, AVG(price) FROM dataset GROUP BY date` works | P1 |
| `test_query_features_basic_sql` | `SELECT * FROM features LIMIT 10` returns feature columns | P1 |
| `test_query_returns_polars_dataframe` | DuckDB query results are Polars DataFrames | P1 |
| `test_query_nonexistent_dataset` | Query on missing dataset raises clear error | P1 |

---

## 7. End-to-End Test Plan

### 7.1 Full Pipeline — `test_full_pipeline.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_full_pipeline_csv_to_features` | CSV file → ingest → normalize → snapshot → feature build → load features → verify data | P0 |
| `test_full_pipeline_parquet_to_features` | Parquet file → ingest → normalize → snapshot → feature build → load features → verify data | P0 |
| `test_full_pipeline_metadata_integrity` | After full pipeline, all metadata tables contain correct linked records | P0 |

### 7.2 Reproducibility — `test_reproducibility.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_snapshot_reproducibility` | Ingest same data twice → normalize twice → compare snapshot Parquet file hashes → identical | P0 |
| `test_feature_reproducibility` | Build same features from same snapshot twice → compare feature Parquet file hashes → identical | P0 |
| `test_reproducibility_across_sessions` | Run full pipeline, close registry, reopen, run again → identical outputs | P1 |

### 7.3 Snapshot Isolation — `test_snapshot_isolation.py`

| Test | Description | Priority |
|------|-------------|:--------:|
| `test_new_snapshot_doesnt_modify_old` | Create snapshot A, create snapshot B → snapshot A files unchanged (byte-level) | P0 |
| `test_new_feature_doesnt_modify_old` | Build features v1, build features v2 → v1 files unchanged | P0 |
| `test_load_old_snapshot_after_new` | After creating snapshot B, `load_dataset(name, snapshot_id=A)` still returns A's data | P0 |

---

## 8. Test Fixtures & Synthetic Data

### 8.1 Shared Fixtures — `conftest.py`

```python
import pytest
import polars as pl
from pathlib import Path
from datetime import datetime, date
import tempfile


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary ADP data directory structure."""
    for subdir in ["raw", "staged", "normalized", "features"]:
        (tmp_path / "data" / subdir).mkdir(parents=True)
    (tmp_path / "metadata").mkdir(parents=True)
    (tmp_path / "config").mkdir(parents=True)
    return tmp_path


@pytest.fixture
def in_memory_registry():
    """SQLite in-memory metadata registry for isolated tests."""
    from adp.metadata.registry import MetadataRegistry
    return MetadataRegistry(db_path=":memory:")


@pytest.fixture
def sample_trades_df() -> pl.DataFrame:
    """Synthetic trade data with realistic values."""
    return pl.DataFrame({
        "trade_id": [f"T{i:06d}" for i in range(1, 101)],
        "symbol": ["BTCUSDT"] * 50 + ["ETHUSDT"] * 50,
        "price": [
            # BTC: ~40000 with noise
            *[40000.0 + (i * 17.3 % 500) - 250 for i in range(50)],
            # ETH: ~2500 with noise
            *[2500.0 + (i * 11.7 % 200) - 100 for i in range(50)],
        ],
        "quantity": [
            *[0.1 + (i * 0.037 % 2.0) for i in range(50)],
            *[1.0 + (i * 0.23 % 10.0) for i in range(50)],
        ],
        "timestamp": [
            datetime(2026, 1, 15, 10, 0, 0, i * 10000)
            for i in range(100)
        ],
        "side": ["buy" if i % 3 != 0 else "sell" for i in range(100)],
    })


@pytest.fixture
def sample_trades_with_duplicates(sample_trades_df: pl.DataFrame) -> pl.DataFrame:
    """Trade data with intentional duplicates for dedup testing."""
    duplicates = sample_trades_df.head(5)
    return pl.concat([sample_trades_df, duplicates])


@pytest.fixture
def sample_ohlcv_df() -> pl.DataFrame:
    """Synthetic OHLCV candle data."""
    dates = [date(2026, 1, d) for d in range(1, 31)]
    return pl.DataFrame({
        "symbol": ["BTCUSDT"] * 30,
        "date": dates,
        "open":  [40000.0 + i * 100 for i in range(30)],
        "high":  [40200.0 + i * 100 for i in range(30)],
        "low":   [39800.0 + i * 100 for i in range(30)],
        "close": [40100.0 + i * 100 for i in range(30)],
        "volume": [1000.0 + i * 50 for i in range(30)],
    })


@pytest.fixture
def sample_datasets_yaml(tmp_data_dir: Path) -> Path:
    """Write a valid datasets.yaml to the temp config directory."""
    yaml_content = '''
datasets:
  trades:
    description: "Test trade data"
    source:
      type: file
      format: csv
      path: /tmp/test_trades.csv
    schema:
      columns:
        - name: trade_id
          type: str
          nullable: false
        - name: symbol
          type: str
          nullable: false
        - name: price
          type: float
          nullable: false
        - name: quantity
          type: float
          nullable: false
        - name: timestamp
          type: datetime
          nullable: false
          source_timezone: UTC
        - name: side
          type: str
          nullable: false
    processing:
      dedup_keys: [trade_id]
      dedup_strategy: keep_last
      partition_by: null
      normalization_version: "1.0"
'''
    config_path = tmp_data_dir / "config" / "datasets.yaml"
    config_path.write_text(yaml_content)
    return config_path


@pytest.fixture
def sample_features_yaml(tmp_data_dir: Path) -> Path:
    """Write a valid features.yaml to the temp config directory."""
    yaml_content = '''
trades:
  basic_factors:
    version: 1
    description: "Test factors"
    features:
      - name: rolling_vol_5
        type: rolling_std
        column: price
        window: 5
      - name: sma_10
        type: moving_average
        column: price
        window: 10
      - name: simple_returns
        type: returns
        column: price
'''
    config_path = tmp_data_dir / "config" / "features.yaml"
    config_path.write_text(yaml_content)
    return config_path


@pytest.fixture
def sample_trades_parquet(tmp_data_dir: Path, sample_trades_df: pl.DataFrame) -> Path:
    """Write sample trade data as Parquet for ingestion tests."""
    parquet_path = tmp_data_dir / "test_trades.parquet"
    sample_trades_df.write_parquet(parquet_path)
    return parquet_path


@pytest.fixture
def sample_trades_csv(tmp_data_dir: Path, sample_trades_df: pl.DataFrame) -> Path:
    """Write sample trade data as CSV for ingestion tests."""
    csv_path = tmp_data_dir / "test_trades.csv"
    sample_trades_df.write_csv(csv_path)
    return csv_path
```

### 8.2 Synthetic Data Characteristics

| Dataset | Rows | Columns | Characteristics |
|---------|:----:|:-------:|-----------------|
| `sample_trades_df` | 100 | 6 | Two symbols, deterministic prices, mixed sides |
| `sample_trades_with_duplicates` | 105 | 6 | 100 unique + 5 duplicates on trade_id |
| `sample_ohlcv_df` | 30 | 7 | 30 daily candles, linear price trend |

**Design rules for synthetic data:**

- All values are deterministic (no `random()`)
- Prices follow simple arithmetic progressions (easy to verify)
- Timestamps are sequential and non-overlapping
- Includes edge cases: first/last rows, boundary values

---

## 9. Mocking Strategy

### 9.1 What to Mock

| Component | Mock Approach | Rationale |
|-----------|--------------|-----------|
| **AWS Athena** | `moto` library or manual `unittest.mock.patch` on `awswrangler.athena.read_sql_query` | No live AWS dependency in tests |
| **System clock** | `time-machine` ~=2.16 | Deterministic timestamps for snapshot IDs |
| **File system (unit tests)** | `tmp_path` pytest fixture | Isolated temp directories per test |

### 9.2 What NOT to Mock

| Component | Rationale |
|-----------|-----------|
| **SQLite** | Use in-memory SQLite (`:memory:`) — fast enough for tests, and tests the real SQL engine |
| **Polars** | Use real Polars operations — they're fast and deterministic; mocking would reduce confidence |
| **DuckDB** | Use real DuckDB with temp Parquet files — tests the actual query path |
| **Parquet I/O** | Use real Parquet reads/writes to `tmp_path` — tests the actual format |
| **Pydantic** | Use real validation — tests the actual schema enforcement |

### 9.3 Mock Examples

#### Mocking Athena with `moto`

```python
import pytest
from unittest.mock import patch, MagicMock
import polars as pl


@pytest.fixture
def mock_athena_response(sample_trades_df):
    """Mock awswrangler.athena.read_sql_query to return sample data."""
    # Convert Polars to Pandas (awswrangler returns Pandas)
    pandas_df = sample_trades_df.to_pandas()

    with patch("awswrangler.athena.read_sql_query") as mock_query:
        mock_query.return_value = pandas_df
        yield mock_query
```

#### Mocking Time

```python
import time_machine


@time_machine.travel("2026-03-02 14:30:00", tick=False)
def test_snapshot_id_uses_current_date():
    """Snapshot ID includes the mocked date."""
    snap_id = generate_snapshot_id("trades", sequence=1)
    assert snap_id == "trades_snap_20260302_001"
```

---

## 10. Test Matrix

### 10.1 Component × Test Level Matrix

| Component | Unit | Integration | E2E |
|-----------|:----:|:-----------:|:---:|
| Config loader | 8 tests | — | — |
| Schema enforcement | 10 tests | — | — |
| Metadata registry | 15 tests | — | — |
| Ingestion (file) | 6 tests | 3 tests | — |
| Ingestion (athena) | 3 tests | 2 tests | — |
| Normalization pipeline | 7 tests | — | — |
| Deduplication | 7 tests | — | — |
| Snapshot engine | 4 tests | 7 tests | — |
| Feature definitions | 7 tests | — | — |
| Feature strategies | 12 tests | — | — |
| Feature materialiser | 5 tests | 6 tests | — |
| CLI | — | 8 tests | — |
| DuckDB integration | — | 5 tests | — |
| Full pipeline | — | — | 3 tests |
| Reproducibility | — | — | 3 tests |
| Snapshot isolation | — | — | 3 tests |
| **Totals** | **~84** | **~31** | **~9** |

### 10.2 Priority Distribution

| Priority | Unit | Integration | E2E | Total |
|----------|:----:|:-----------:|:---:|:-----:|
| P0 | ~55 | ~18 | 6 | ~79 |
| P1 | ~25 | ~11 | 2 | ~38 |
| P2 | ~4 | ~2 | 1 | ~7 |

---

## 11. CI/CD Testing Pipeline

### 11.1 Pipeline Stages

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Lint    │───►│  Type    │───►│  Unit    │───►│  Integ   │
│  (ruff)  │    │  (mypy)  │    │  Tests   │    │  Tests   │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
                                                      │
                                                      ▼
                                                ┌──────────┐
                                                │   E2E    │
                                                │  Tests   │
                                                └──────────┘
                                                      │
                                                      ▼
                                                ┌──────────┐
                                                │ Coverage │
                                                │  Report  │
                                                └──────────┘
```

### 11.2 Stage Details

| Stage | Command | Pass Criteria | Timeout |
|-------|---------|---------------|:-------:|
| Lint | `ruff check src/ tests/` | Zero violations | 30s |
| Type Check | `mypy src/adp/` | Zero errors | 60s |
| Unit Tests | `pytest tests/unit/ -m unit` | 100% pass, ≥90% coverage | 30s |
| Integration Tests | `pytest tests/integration/ -m integration` | 100% pass | 120s |
| E2E Tests | `pytest tests/e2e/ -m e2e` | 100% pass | 300s |
| Coverage Report | `pytest --cov-report=html` | ≥90% line, ≥85% branch | — |

### 11.3 Local Test Commands

```bash
# Run all tests
pytest

# Run only unit tests (fast feedback)
pytest tests/unit/ -m unit

# Run integration tests
pytest tests/integration/ -m integration

# Run E2E tests
pytest tests/e2e/ -m e2e

# Run with coverage report
pytest --cov=src/adp --cov-report=html

# Run a specific test file
pytest tests/unit/test_feature_strategies.py -v

# Run tests matching a keyword
pytest -k "rolling_std" -v
```

---

## 12. Regression Testing

### 12.1 Regression Strategy

| Trigger | Action |
|---------|--------|
| New feature strategy added | Add unit tests for the strategy + integration test for materialisation |
| Schema change in datasets.yaml | Verify schema hash changes are detected; add test for migration path |
| SQLite schema change | Add migration test; verify existing data is preserved |
| Polars version upgrade | Run full E2E suite; verify reproducibility tests still pass |
| DuckDB version upgrade | Run DuckDB integration tests; verify query results unchanged |
| Bug fix | Add regression test that reproduces the bug before the fix |

### 12.2 Golden File Testing

For reproducibility-critical paths, maintain "golden files" — known-good outputs that tests compare against:

| Golden File | Purpose |
|-------------|---------|
| `tests/golden/trades_snapshot.parquet` | Known-good normalized snapshot from sample trades |
| `tests/golden/trades_basic_factors.parquet` | Known-good feature output from sample trades |
| `tests/golden/schema_hash.txt` | Known-good schema hash for sample trades schema |
| `tests/golden/definition_hash.txt` | Known-good definition hash for basic_factors |

**Golden file update process:**

1. Run `pytest --update-golden` (custom flag) to regenerate golden files
2. Review diffs in version control
3. Commit updated golden files with an explanation of why they changed

---

## 13. Performance Testing

### 13.1 Performance Benchmarks

| Scenario | Dataset Size | Target Time | Measured By |
|----------|:----------:|:-----------:|-------------|
| CSV ingestion | 1M rows | <5s | `test_perf_ingest_1m` |
| Normalization pipeline | 1M rows | <10s | `test_perf_normalize_1m` |
| Snapshot creation | 1M rows | <10s | `test_perf_snapshot_1m` |
| Feature materialisation (7 features) | 1M rows | <15s | `test_perf_features_1m` |
| `load_dataset()` (lazy scan) | 10M rows | <1s | `test_perf_load_10m` |
| DuckDB aggregation query | 10M rows | <3s | `test_perf_duckdb_10m` |

### 13.2 Performance Test Approach

- Performance tests are marked `@pytest.mark.slow` and excluded from CI by default
- Run manually or on-demand: `pytest -m slow`
- Use `pytest-benchmark` for precise timing
- Generate large synthetic datasets programmatically (not stored in repo)
- Performance targets are guidelines, not hard gates

### 13.3 Memory Testing

| Scenario | Max Memory | Verification |
|----------|:----------:|-------------|
| Normalize 10M rows | <2GB RSS | Monitor via `tracemalloc` or `memory_profiler` |
| Feature build 10M rows | <2GB RSS | Must use lazy evaluation throughout |
| Load dataset (lazy) | <100MB RSS | `scan_parquet` should not load data into memory |

---

## 14. Change Log

| Date | Version | Author | Change |
|------|---------|--------|--------|
| 2026-03-02 | 1.0 | — | Initial test strategy document |

---

*Cross-references: [Project Delivery Plan](./01-project-delivery-plan.md) · [User Stories](./02-user-stories.md) · [Design Document](./03-design-document.md) · [Quant Infrastructure](./05-quant-infrastructure.md)*
