# Athena Data Platform (ADP) — User Stories & Deliverables

> **Version**: 1.0
> **Date**: 2026-03-02
> **Status**: Draft
> **Owner**: Tech Lead
> **Audience**: Platform Engineering, Quant Research, Data Engineering
> **Related**: [Project Delivery Plan](./01-project-delivery-plan.md)

---

## Table of Contents

1. [Story Format & Conventions](#1-story-format--conventions)
2. [Definition of Done](#2-definition-of-done)
3. [Epic 1 — Foundation (WS1)](#3-epic-1--foundation-ws1)
4. [Epic 2 — Ingestion Layer (WS2)](#4-epic-2--ingestion-layer-ws2)
5. [Epic 3 — Normalization & Snapshot Engine (WS3)](#5-epic-3--normalization--snapshot-engine-ws3)
6. [Epic 4 — Feature Materialisation Engine (WS4)](#6-epic-4--feature-materialisation-engine-ws4)
7. [Epic 5 — Access Layer & Jupyter Integration (WS5)](#7-epic-5--access-layer--jupyter-integration-ws5)
8. [Story Dependency Map](#8-story-dependency-map)
9. [Priority Summary](#9-priority-summary)
10. [Change Log](#10-change-log)

---

## 1. Story Format & Conventions

Each story follows this format:

```
**[ID]** Title
As a [role], I want [capability] so that [benefit].
Priority: P0 | P1 | P2 | P3
Dependencies: [story IDs]
Acceptance Criteria:
- [ ] Criterion 1
- [ ] Criterion 2
```

### Priority Definitions

| Priority | Meaning |
|----------|---------|
| **P0** | Must-have — platform cannot function without this |
| **P1** | Should-have — critical for production quality |
| **P2** | Nice-to-have — improves ergonomics or resilience |
| **P3** | Future — documented for backlog, not in initial build |

### Roles

| Role | Description |
|------|-------------|
| **Platform Engineer** | Builds and maintains the ADP platform |
| **Quant Researcher** | Uses the platform for analytical research and factor development |
| **Data Engineer** | Manages data sources, ingestion configs, and schema definitions |

---

## 2. Definition of Done

A story is complete when ALL of the following are satisfied:

1. All acceptance criteria are met and verified
2. Code is written, reviewed, and merged
3. Unit tests cover the new functionality (≥90% line coverage for the story's code)
4. Integration tests pass where the story touches multiple components
5. No regressions in existing tests
6. Metadata schema changes (if any) are applied and tested
7. Configuration changes (if any) are documented in YAML with examples

---

## 3. Epic 1 — Foundation (WS1)

**Epic goal:** Establish the project skeleton, configuration system, and metadata infrastructure.

---

### F-001: Project Scaffolding

As a **Platform Engineer**, I want a properly structured Python project with `pyproject.toml`, dependency declarations, and the prescribed directory layout so that all team members can install and develop against a consistent environment.

**Priority:** P0
**Dependencies:** None

**Acceptance Criteria:**

- [ ] `pyproject.toml` exists with project name `adp`, Python >=3.11,<3.14, and pinned core dependencies (polars~=1.38, duckdb~=1.4, pydantic~=2.12, typer~=0.24, awswrangler~=3.15, pyyaml~=6.0)
- [ ] Directory structure matches the specification: `config/`, `data/{raw,staged,normalized,features}/`, `metadata/`, `src/adp/`, `notebooks/`
- [ ] `pip install -e .` succeeds in a clean virtual environment
- [ ] `import adp` works after installation
- [ ] `.gitignore` excludes `data/`, `metadata/*.db`, `__pycache__/`, `.venv/`

---

### F-002: Configuration Loader

As a **Platform Engineer**, I want a YAML-based configuration system that loads `datasets.yaml` and `features.yaml` into typed Python objects so that dataset and feature definitions are declarative and validated at startup.

**Priority:** P0
**Dependencies:** F-001

**Acceptance Criteria:**

- [ ] `config/datasets.yaml` schema is defined and documented
- [ ] `config/features.yaml` schema is defined and documented
- [ ] `src/adp/config.py` provides `load_datasets_config()` and `load_features_config()` functions
- [ ] Config objects are Pydantic models with strict validation
- [ ] Missing or malformed YAML raises clear error messages with file path and line number context
- [ ] Config paths are configurable (not hardcoded) via environment variable or parameter

---

### F-003: SQLite Metadata Registry — Schema Initialization

As a **Platform Engineer**, I want the SQLite metadata registry to auto-initialize all required tables on first run so that the platform is self-bootstrapping.

**Priority:** P0
**Dependencies:** F-001

**Acceptance Criteria:**

- [ ] `metadata/adp_registry.db` is created automatically if it does not exist
- [ ] All 7 tables are created: `datasets`, `raw_ingestions`, `snapshots`, `snapshot_lineage`, `feature_definitions`, `feature_snapshots`, `feature_lineage`
- [ ] SQLite is opened in WAL mode for concurrent read safety
- [ ] Table creation is idempotent (safe to run multiple times)
- [ ] Schema matches the DDL specification in the [Design Document](./03-design-document.md)

---

### F-004: Metadata Registry — Core CRUD Operations

As a **Platform Engineer**, I want a `MetadataRegistry` class that provides create/read/update operations for all metadata tables so that other components can register and query lineage data.

**Priority:** P0
**Dependencies:** F-003

**Acceptance Criteria:**

- [ ] `MetadataRegistry` class wraps SQLite connection with context-managed transactions
- [ ] Methods exist for: register dataset, log ingestion, create snapshot record, link snapshot lineage
- [ ] Methods exist for: register feature definition, create feature snapshot record, link feature lineage
- [ ] All writes are wrapped in transactions (commit on success, rollback on failure)
- [ ] Read methods return typed dataclasses or Pydantic models (not raw tuples)
- [ ] Unit tests cover all CRUD paths including error cases

---

### F-005: Snapshot ID Generation

As a **Platform Engineer**, I want a deterministic snapshot ID generation scheme so that each snapshot is uniquely identifiable and traceable.

**Priority:** P0
**Dependencies:** F-003

**Acceptance Criteria:**

- [ ] Snapshot IDs follow the format: `{dataset_name}_snap_{YYYYMMDD}_{sequence}`
- [ ] Sequence number auto-increments per dataset per day
- [ ] Feature snapshot IDs follow: `{dataset}_{feature_set}_fsnap_{YYYYMMDD}_{sequence}`
- [ ] ID generation is deterministic given the same inputs
- [ ] No collisions are possible for concurrent writes (sequence managed via SQLite)

---

### F-006: Schema Hash Generation

As a **Platform Engineer**, I want a schema hashing utility that produces a stable hash from a Pydantic model or column specification so that schema changes are detectable.

**Priority:** P1
**Dependencies:** F-002

**Acceptance Criteria:**

- [ ] `compute_schema_hash(model_or_columns)` returns a hex digest string
- [ ] Hash is deterministic (same schema → same hash, always)
- [ ] Hash changes when columns are added, removed, renamed, or retyped
- [ ] Hash is stable across Python restarts (no random seeds)
- [ ] Uses SHA-256 for collision resistance

---

### F-007: Platform Initialization CLI Command

As a **Platform Engineer**, I want an `adp init` CLI command that creates the directory structure and initializes the metadata database so that platform setup is a single command.

**Priority:** P1
**Dependencies:** F-001, F-003

**Acceptance Criteria:**

- [ ] `adp init` creates all required directories if they don't exist
- [ ] `adp init` initializes the SQLite registry with all tables
- [ ] `adp init` is idempotent (safe to run on an already-initialized platform)
- [ ] Output confirms what was created vs. what already existed
- [ ] `adp init --config-dir <path>` allows custom config location

---

## 4. Epic 2 — Ingestion Layer (WS2)

**Epic goal:** Implement reliable data extraction from Athena and local file sources with full metadata logging.

---

### I-001: Ingestion Strategy Protocol

As a **Platform Engineer**, I want an `IngestionStrategy` protocol that defines the contract for all data source ingestion so that new sources can be added without modifying existing code.

**Priority:** P0
**Dependencies:** F-001

**Acceptance Criteria:**

- [ ] `IngestionStrategy` is defined as a Python `Protocol` class
- [ ] Protocol requires: `ingest(config) -> IngestionResult` method
- [ ] `IngestionResult` contains: raw data path, row count, source metadata, ingestion timestamp
- [ ] Protocol is documented with type hints and docstring

---

### I-002: Athena Ingestion Strategy

As a **Data Engineer**, I want to extract data from AWS Athena tables using SQL queries so that analytical datasets are reliably sourced from the data warehouse.

**Priority:** P0
**Dependencies:** I-001, F-002

**Acceptance Criteria:**

- [ ] `AthenaIngestionStrategy` implements the `IngestionStrategy` protocol
- [ ] Accepts dataset config with: database, table, query, S3 output location
- [ ] Uses `awswrangler.athena.read_sql_query()` for extraction
- [ ] Writes raw output to `data/raw/{dataset_name}/{ingestion_id}.parquet`
- [ ] Handles Athena query timeouts gracefully with clear error messages
- [ ] Supports parameterized date ranges for incremental extraction

---

### I-003: File Ingestion Strategy

As a **Data Engineer**, I want to ingest data from local files in CSV, TXT, JSON, and Parquet formats so that datasets not in Athena can still enter the platform.

**Priority:** P0
**Dependencies:** I-001

**Acceptance Criteria:**

- [ ] `FileIngestionStrategy` implements the `IngestionStrategy` protocol
- [ ] Supports formats: CSV, TXT (delimited), JSON (records and lines), Parquet
- [ ] Auto-detects format from file extension (overridable via config)
- [ ] Copies raw file to `data/raw/{dataset_name}/{ingestion_id}.{ext}`
- [ ] Handles encoding issues (UTF-8 default, configurable)
- [ ] Returns accurate row count in `IngestionResult`

---

### I-004: Ingestion Metadata Logging

As a **Platform Engineer**, I want every ingestion event automatically logged to the metadata registry so that the lineage chain starts from the first touch of data.

**Priority:** P0
**Dependencies:** I-001, F-004

**Acceptance Criteria:**

- [ ] Every successful ingestion writes a record to `raw_ingestions` table
- [ ] Record includes: `ingestion_id`, `source_type` (athena/file), `source_location`, `ingestion_timestamp`
- [ ] Failed ingestions do not create partial metadata records (transactional)
- [ ] `ingestion_id` is returned to callers for lineage linking

---

### I-005: CLI — Ingest Command

As a **Data Engineer**, I want an `adp ingest <dataset>` CLI command so that I can trigger ingestion from the terminal without writing Python code.

**Priority:** P1
**Dependencies:** I-002, I-003, I-004

**Acceptance Criteria:**

- [ ] `adp ingest <dataset_name>` ingests the named dataset using its configured strategy
- [ ] `adp ingest <dataset_name> --source file --path /path/to/file` overrides with file ingestion
- [ ] Output shows: dataset name, source, row count, ingestion ID, duration
- [ ] Errors are reported clearly with actionable messages
- [ ] `adp ingest --list` shows all configured datasets

---

### I-006: Ingestion Idempotency Guard

As a **Platform Engineer**, I want ingestion to detect and warn about duplicate source data so that accidental re-ingestion doesn't create redundant raw copies.

**Priority:** P2
**Dependencies:** I-004

**Acceptance Criteria:**

- [ ] Before ingesting, check if the same source location was already ingested (via metadata query)
- [ ] If duplicate detected, warn the user and require `--force` flag to proceed
- [ ] Duplicate detection is based on source_location + source_type combination
- [ ] Forced re-ingestion creates a new ingestion record (doesn't overwrite)

---

## 5. Epic 3 — Normalization & Snapshot Engine (WS3)

**Epic goal:** Enforce schemas, normalize data, and produce immutable versioned snapshots with full lineage.

---

### N-001: Pydantic Schema Enforcement

As a **Platform Engineer**, I want a dynamic Pydantic model factory that generates validation models from dataset configuration so that schema enforcement is declarative and strict.

**Priority:** P0
**Dependencies:** F-002, F-006

**Acceptance Criteria:**

- [ ] `build_schema_model(dataset_config)` returns a Pydantic model class
- [ ] Model enforces column names, types, and nullability as declared in `datasets.yaml`
- [ ] Validation fails fast on first schema violation with a clear error message
- [ ] Schema hash is computed from the generated model
- [ ] Supported types: `str`, `int`, `float`, `bool`, `datetime`, `date`, `Decimal`

---

### N-002: Timezone Normalization

As a **Quant Researcher**, I want all timestamp columns normalized to UTC so that cross-source data analysis is timezone-consistent.

**Priority:** P0
**Dependencies:** N-001

**Acceptance Criteria:**

- [ ] All columns typed as `datetime` in the schema are converted to UTC
- [ ] Source timezone is configurable per dataset in `datasets.yaml`
- [ ] Timezone-naive datetimes are treated as the configured source timezone (not assumed UTC)
- [ ] Conversion preserves microsecond precision
- [ ] Normalization is logged (source tz → UTC)

---

### N-003: Deduplication Engine

As a **Platform Engineer**, I want configurable deduplication logic so that normalized datasets contain no duplicate records.

**Priority:** P1
**Dependencies:** N-001

**Acceptance Criteria:**

- [ ] Dedup key columns are configurable per dataset in `datasets.yaml`
- [ ] Default strategy: keep last occurrence (configurable: first, last)
- [ ] Dedup count is logged (N records removed)
- [ ] If no dedup key is configured, skip dedup (passthrough)
- [ ] Dedup operates on Polars LazyFrame (no eager collection)

---

### N-004: Normalization Pipeline

As a **Platform Engineer**, I want a composable normalization pipeline that chains schema validation, timezone normalization, and deduplication so that processing order is deterministic and configurable.

**Priority:** P0
**Dependencies:** N-001, N-002, N-003

**Acceptance Criteria:**

- [ ] `NormalizationPipeline` accepts a list of processing steps
- [ ] Default chain: validate schema → normalize timezones → deduplicate
- [ ] Pipeline operates entirely on Polars LazyFrames
- [ ] Each step is independently testable
- [ ] Pipeline logs processing stats per step (row count in, row count out)

---

### N-005: Snapshot Creation

As a **Platform Engineer**, I want the snapshot engine to write normalized data as immutable Parquet files with unique snapshot IDs so that every dataset version is permanently preserved.

**Priority:** P0
**Dependencies:** N-004, F-005

**Acceptance Criteria:**

- [ ] Normalized data is written to `data/normalized/{dataset_name}/{snapshot_id}/`
- [ ] Parquet files are partitioned by date column (if configured) or written as a single file
- [ ] Snapshot ID is generated per the ID scheme in F-005
- [ ] Snapshot metadata is registered in the `snapshots` table: ID, dataset, schema hash, normalization version, row count, storage path, timestamp
- [ ] Existing snapshots are never overwritten or modified

---

### N-006: Snapshot Lineage Tracking

As a **Quant Researcher**, I want each snapshot to record which raw ingestions it was built from so that I can trace any data point back to its source.

**Priority:** P0
**Dependencies:** N-005, I-004

**Acceptance Criteria:**

- [ ] `snapshot_lineage` table links each snapshot to its source ingestion(s)
- [ ] A snapshot can be built from one or more ingestions (e.g., multi-day merge)
- [ ] Lineage is queryable: given a snapshot ID, return all contributing ingestions
- [ ] Lineage is written transactionally with the snapshot record

---

### N-007: Dataset Registration and Current Snapshot Tracking

As a **Quant Researcher**, I want the `datasets` table to track the current (latest) snapshot for each dataset so that loading a dataset without specifying a version gives me the most recent data.

**Priority:** P0
**Dependencies:** N-005

**Acceptance Criteria:**

- [ ] `datasets` table has a `current_snapshot` field updated on each new snapshot
- [ ] `current_snapshot` points to the latest snapshot ID for the dataset
- [ ] Schema hash is stored and compared on each new snapshot (drift detection)
- [ ] If schema hash changes, a warning is emitted but the snapshot is still created

---

### N-008: CLI — Normalize and Snapshot Commands

As a **Data Engineer**, I want `adp normalize <dataset>` and `adp snapshot <dataset>` CLI commands so that I can run normalization and snapshotting from the terminal.

**Priority:** P1
**Dependencies:** N-004, N-005

**Acceptance Criteria:**

- [ ] `adp normalize <dataset>` runs the normalization pipeline on the latest raw ingestion
- [ ] `adp snapshot <dataset>` creates a snapshot from normalized data
- [ ] `adp snapshot list <dataset>` shows all snapshots for a dataset (ID, date, row count, schema hash)
- [ ] `adp snapshot show <snapshot_id>` shows snapshot details including lineage
- [ ] Output is human-readable with summary statistics

---

### N-009: Snapshot Reproducibility Guarantee

As a **Quant Researcher**, I want the guarantee that re-running normalization on the same raw data with the same config produces a bit-identical snapshot so that my research is reproducible.

**Priority:** P1
**Dependencies:** N-005

**Acceptance Criteria:**

- [ ] Given identical raw input and identical config, the normalization pipeline produces the same Parquet bytes
- [ ] Polars operations used are deterministic (no random seeds, no non-deterministic joins)
- [ ] Parquet writer settings are fixed (compression, row group size) to ensure binary reproducibility
- [ ] A test verifies this property: ingest → normalize twice → compare file hashes

---

## 6. Epic 4 — Feature Materialisation Engine (WS4)

**Epic goal:** Build a deterministic, versioned feature computation layer that produces reusable analytical factors from normalized snapshots.

---

### FE-001: Feature Definition Parser

As a **Platform Engineer**, I want a parser that reads `features.yaml` and produces typed feature definition objects so that feature specifications are declarative and validated.

**Priority:** P0
**Dependencies:** F-002

**Acceptance Criteria:**

- [ ] Parses the `features.yaml` structure: dataset → feature_set → version → features list
- [ ] Each feature definition includes: name, type, column, window (where applicable), and any type-specific parameters
- [ ] Supported feature types: `rolling_std`, `moving_average`, `vwap`, `ewma`, `rolling_min`, `rolling_max`, `returns`, `log_returns`
- [ ] Invalid definitions raise clear validation errors
- [ ] Definition hash is computed from the serialized definition (for version tracking)

---

### FE-002: Feature Strategy Protocol

As a **Platform Engineer**, I want a `FeatureStrategy` protocol that defines the contract for feature computation so that new feature types can be added without modifying the materialiser.

**Priority:** P0
**Dependencies:** FE-001

**Acceptance Criteria:**

- [ ] `FeatureStrategy` is a Python `Protocol` class
- [ ] Protocol requires: `compute(lazy_frame, feature_def) -> pl.LazyFrame`
- [ ] Each feature type has a corresponding strategy implementation
- [ ] Strategies operate on Polars LazyFrames exclusively
- [ ] Strategy registry maps feature type strings to strategy classes

---

### FE-003: Core Feature Strategies — Rolling Computations

As a **Quant Researcher**, I want rolling statistical features (volatility, moving averages, min, max) computed over configurable windows so that I can build standard factor signals.

**Priority:** P0
**Dependencies:** FE-002

**Acceptance Criteria:**

- [ ] `rolling_std` computes rolling standard deviation over a window
- [ ] `moving_average` computes simple moving average (SMA) over a window
- [ ] `rolling_min` / `rolling_max` compute rolling extremes
- [ ] `ewma` computes exponentially weighted moving average with configurable span
- [ ] All computations handle NaN at window boundaries correctly (NaN-filled, not dropped)
- [ ] All computations use Polars lazy expressions (no eager collection)

---

### FE-004: Core Feature Strategies — Market Microstructure

As a **Quant Researcher**, I want VWAP, returns, and log-returns features so that I can build price-based analytical factors.

**Priority:** P0
**Dependencies:** FE-002

**Acceptance Criteria:**

- [ ] `vwap` computes volume-weighted average price from price and volume columns
- [ ] `returns` computes simple returns: `(price[t] - price[t-1]) / price[t-1]`
- [ ] `log_returns` computes log returns: `ln(price[t] / price[t-1])`
- [ ] Column names for price and volume are configurable in the feature definition
- [ ] All computations are deterministic and handle edge cases (first row, zero volume)

---

### FE-005: Feature Materialiser

As a **Platform Engineer**, I want a `FeatureMaterialiser` that takes a dataset snapshot and feature set definition and produces a versioned feature snapshot so that feature computation is automated and reproducible.

**Priority:** P0
**Dependencies:** FE-001, FE-002, FE-003, FE-004, N-005

**Acceptance Criteria:**

- [ ] `materialise(dataset_name, feature_set_name, snapshot_id=None)` is the entry point
- [ ] If `snapshot_id` is None, uses the current snapshot for the dataset
- [ ] Loads the dataset snapshot as a Polars LazyFrame
- [ ] Applies each feature in the feature set sequentially using the appropriate strategy
- [ ] Collects the LazyFrame only once at the end (single materialization pass)
- [ ] Computes the feature definition hash and generates a feature snapshot ID

---

### FE-006: Feature Snapshot Storage

As a **Platform Engineer**, I want feature outputs stored as immutable Parquet files in a structured directory layout so that feature data is organized and versioned.

**Priority:** P0
**Dependencies:** FE-005

**Acceptance Criteria:**

- [ ] Feature snapshots are written to `data/features/{dataset}/{feature_set}/{feature_snapshot_id}/`
- [ ] Parquet files are partitioned by date (if the dataset has a date column)
- [ ] Existing feature snapshots are never overwritten
- [ ] Storage path is recorded in the `feature_snapshots` metadata table

---

### FE-007: Feature Metadata Registration

As a **Platform Engineer**, I want feature definitions and snapshots registered in the metadata registry so that feature lineage is fully tracked.

**Priority:** P0
**Dependencies:** FE-005, FE-006, F-004

**Acceptance Criteria:**

- [ ] Feature definitions are registered in `feature_definitions` table with: name, dataset, version, definition hash, timestamp
- [ ] Feature snapshots are registered in `feature_snapshots` table with: snapshot ID, feature name, dataset snapshot ID, version, row count, storage path, timestamp
- [ ] Feature lineage is recorded in `feature_lineage` table linking feature snapshot to dataset snapshot
- [ ] All metadata writes are transactional

---

### FE-008: Feature Determinism Guarantee

As a **Quant Researcher**, I want the guarantee that materialising the same feature set from the same dataset snapshot always produces identical output so that my factor research is reproducible.

**Priority:** P1
**Dependencies:** FE-005

**Acceptance Criteria:**

- [ ] Same dataset snapshot + same feature definition → bit-identical Parquet output
- [ ] Polars operations used are fully deterministic (no random, no non-deterministic sorts)
- [ ] Feature definition hash detects any change in the definition
- [ ] If definition hash has changed, materialiser warns and creates a new version (not overwrite)
- [ ] A test verifies this: materialise twice from same snapshot → compare file hashes

---

### FE-009: CLI — Feature Commands

As a **Quant Researcher**, I want CLI commands to build, list, show, and load features so that I can manage the feature lifecycle from the terminal.

**Priority:** P1
**Dependencies:** FE-005, FE-006, FE-007

**Acceptance Criteria:**

- [ ] `adp features build <dataset> <feature_set>` materialises features from the current snapshot
- [ ] `adp features build <dataset> <feature_set> --snapshot <id>` materialises from a specific snapshot
- [ ] `adp features list <dataset>` lists all feature sets and their latest snapshots
- [ ] `adp features show <dataset> <feature_set>` shows feature set details, definitions, and snapshot history
- [ ] `adp features load <dataset> <feature_set>` prints summary statistics of the latest feature snapshot
- [ ] All commands produce clear, formatted output

---

### FE-010: Feature Definition Versioning

As a **Quant Researcher**, I want feature definitions to be versioned so that I can track how factor computations evolved over time.

**Priority:** P1
**Dependencies:** FE-001, FE-007

**Acceptance Criteria:**

- [ ] `features.yaml` includes a `version` field per feature set
- [ ] Definition hash is recomputed on each build and compared to the registered version
- [ ] If the hash changed but version didn't increment, materialiser raises a warning
- [ ] Version history is queryable via metadata: given a feature name, return all versions and their hashes
- [ ] Old feature snapshots remain accessible even after definition version changes

---

## 7. Epic 5 — Access Layer & Jupyter Integration (WS5)

**Epic goal:** Provide ergonomic data access for research workflows through Python API, DuckDB SQL, and JupyterLab notebooks.

---

### A-001: Load Dataset API

As a **Quant Researcher**, I want a `load_dataset()` function that returns any snapshot as a Polars LazyFrame so that I can access data with one line of code.

**Priority:** P0
**Dependencies:** N-005, N-007

**Acceptance Criteria:**

- [ ] `load_dataset(name)` returns the current (latest) snapshot as `pl.LazyFrame`
- [ ] `load_dataset(name, snapshot_id="snap_id")` returns a specific snapshot
- [ ] Raises clear error if dataset or snapshot doesn't exist
- [ ] Returns LazyFrame (not DataFrame) — caller decides when to collect
- [ ] Metadata (snapshot ID, row count, schema hash) is accessible via the returned object or a companion function

---

### A-002: Load Features API

As a **Quant Researcher**, I want a `load_features()` function that returns feature data with optional snapshot pinning so that I can access factors with one line of code.

**Priority:** P0
**Dependencies:** FE-006, FE-007

**Acceptance Criteria:**

- [ ] `load_features(dataset, feature_set)` returns the latest feature snapshot as `pl.LazyFrame`
- [ ] `load_features(dataset, feature_set, snapshot_id="fsnap_id")` returns a specific feature snapshot
- [ ] `load_features("trades", "basic_factors@v2026_03_02_001")` syntax is supported for snapshot pinning
- [ ] Raises clear error if feature set or snapshot doesn't exist
- [ ] Returns LazyFrame (not DataFrame)

---

### A-003: DuckDB Integration

As a **Quant Researcher**, I want to run SQL queries over dataset snapshots and feature stores using DuckDB so that I can use SQL for ad-hoc analysis alongside Polars.

**Priority:** P1
**Dependencies:** A-001, A-002

**Acceptance Criteria:**

- [ ] `query_dataset(name, sql)` executes a DuckDB SQL query over a dataset snapshot
- [ ] `query_features(dataset, feature_set, sql)` executes SQL over a feature snapshot
- [ ] DuckDB reads Parquet files directly (zero-copy where possible)
- [ ] SQL can reference the table as `dataset` or `features` (standardized alias)
- [ ] Results are returned as Polars DataFrames
- [ ] Connection is managed internally (no DuckDB boilerplate for the user)

---

### A-004: Convenience API Module

As a **Quant Researcher**, I want a clean top-level import (`from adp import load_dataset, load_features`) so that API usage is minimal-boilerplate.

**Priority:** P1
**Dependencies:** A-001, A-002

**Acceptance Criteria:**

- [ ] `src/adp/api.py` exports: `load_dataset`, `load_features`, `query_dataset`, `query_features`
- [ ] `src/adp/__init__.py` re-exports these for `from adp import ...` usage
- [ ] API functions handle initialization internally (auto-init registry if needed)
- [ ] Clear docstrings with usage examples on each function

---

### A-005: Research Demo Notebook

As a **Quant Researcher**, I want an example Jupyter notebook that demonstrates the full workflow — load data, build features, query with DuckDB, construct a backtest-ready matrix — so that I have a reference for research patterns.

**Priority:** P1
**Dependencies:** A-001, A-002, A-003

**Acceptance Criteria:**

- [ ] `notebooks/research_demo.ipynb` contains a complete walkthrough
- [ ] Demonstrates: `load_dataset()` → inspect schema → `adp features build` → `load_features()`
- [ ] Shows DuckDB SQL query over the feature store
- [ ] Constructs a backtest-ready feature matrix (features joined with forward returns)
- [ ] Includes visualizations (matplotlib/plotly) of at least one factor signal
- [ ] Markdown cells explain each step for educational value

---

### A-006: Dataset Discovery API

As a **Quant Researcher**, I want to list available datasets, their snapshots, and available feature sets from Python so that I can discover data without leaving Jupyter.

**Priority:** P2
**Dependencies:** F-004, A-004

**Acceptance Criteria:**

- [ ] `list_datasets()` returns all registered datasets with current snapshot info
- [ ] `list_snapshots(dataset)` returns all snapshots for a dataset
- [ ] `list_feature_sets(dataset)` returns all feature sets and their latest snapshot info
- [ ] Output is a Polars DataFrame (or list of typed dataclasses) suitable for display in Jupyter
- [ ] Includes metadata: row counts, schema hashes, timestamps

---

### A-007: Backtest Feature Matrix Builder

As a **Quant Researcher**, I want a utility to construct a point-in-time correct feature matrix by joining features with forward returns so that I have a ready-to-use input for backtesting models.

**Priority:** P2
**Dependencies:** A-001, A-002

**Acceptance Criteria:**

- [ ] `build_backtest_matrix(dataset, feature_set, forward_return_periods=[1, 5, 10])` is available
- [ ] Joins feature snapshot with forward returns computed from the price column
- [ ] Ensures point-in-time correctness (no look-ahead bias)
- [ ] Returns a Polars LazyFrame with features + forward return columns
- [ ] Forward return periods are configurable

---

## 8. Story Dependency Map

```
F-001 ──→ F-002 ──→ F-006
  │         │
  │         ├──→ N-001 ──→ N-002 ──→ N-004 ──→ N-005 ──→ N-006
  │         │                                     │         │
  │         │              N-003 ────────────→ N-004       N-007
  │         │                                     │
  │         ├──→ FE-001 ──→ FE-002 ──→ FE-003    │
  │         │                  │         │        │
  │         │                  └──→ FE-004        │
  │         │                  │                  │
  │         │                  └──→ FE-005 ←──────┘
  │         │                         │
  │         │                         ├──→ FE-006 ──→ FE-007
  │         │                         │                 │
  │         │                         ├──→ FE-008      FE-009
  │         │                         └──→ FE-010
  │         │
  ├──→ F-003 ──→ F-004
  │         │
  ├──→ F-005
  │
  ├──→ F-007
  │
  ├──→ I-001 ──→ I-002
  │      │        │
  │      ├──→ I-003
  │      │
  │      └──→ I-004 ──→ I-005
  │              │
  │              └──→ I-006
  │
  └──→ A-001 ──→ A-002 ──→ A-003 ──→ A-005
         │         │                    │
         ├──→ A-004                    A-006
         └──→ A-007
```

---

## 9. Priority Summary

| Priority | Count | Stories |
|----------|:-----:|---------|
| **P0** | 20 | F-001, F-002, F-003, F-004, F-005, I-001, I-002, I-003, I-004, N-001, N-002, N-004, N-005, N-006, N-007, FE-001, FE-002, FE-003, FE-004, FE-005, FE-006, FE-007, A-001, A-002 |
| **P1** | 13 | F-006, F-007, N-003, N-008, N-009, FE-008, FE-009, FE-010, A-003, A-004, A-005, I-005 |
| **P2** | 3 | I-006, A-006, A-007 |
| **P3** | 0 | — |

---

## 10. Change Log

| Date | Version | Author | Change |
|------|---------|--------|--------|
| 2026-03-02 | 1.0 | — | Initial user stories creation |

---

*Cross-references: [Project Delivery Plan](./01-project-delivery-plan.md) · [Design Document](./03-design-document.md) · [Test Strategy](./04-test-strategy.md) · [Quant Infrastructure](./05-quant-infrastructure.md)*
