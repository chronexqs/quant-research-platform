# Athena Data Platform (ADP) — Project Delivery Plan

> **Version**: 1.0
> **Date**: 2026-03-02
> **Status**: Draft
> **Owner**: Tech Lead
> **Audience**: Platform Engineering, Quant Research, Data Engineering
> **Review Cadence**: Weekly stand-up + milestone gate reviews

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Platform Overview & Objectives](#2-platform-overview--objectives)
3. [Scope & Boundaries](#3-scope--boundaries)
4. [Workstream Definitions](#4-workstream-definitions)
5. [Milestone Schedule](#5-milestone-schedule)
6. [Phase Gate Reviews](#6-phase-gate-reviews)
7. [Dependency Map](#7-dependency-map)
8. [RACI Matrix](#8-raci-matrix)
9. [Delivery Cadence & Ceremonies](#9-delivery-cadence--ceremonies)
10. [Risk Register](#10-risk-register)
11. [Constraints & Assumptions](#11-constraints--assumptions)
12. [Architectural Decision Records](#12-architectural-decision-records)
13. [Change Log](#13-change-log)

---

## 1. Executive Summary

The Athena Data Platform (ADP) is a production-quality, local Python data platform designed for quantitative and analytical research. It provides a unified workflow for extracting data from AWS Athena (with file-based fallback), enforcing strict schema normalization, storing data in immutable versioned columnar formats, and producing reproducible dataset snapshots. A feature materialisation layer enables reusable, deterministic analytical factors.

The platform combines a local lakehouse architecture with a snapshot engine, feature store, and research-ready access layer — forming a scaled-down internal quant research infrastructure that integrates cleanly with JupyterLab for advanced analysis.

**Key outcomes:**

- Immutable, versioned datasets with full lineage tracking
- Reproducible snapshots tied to schema hashes and normalization versions
- Deterministic feature materialisation with versioned outputs
- Fast local querying via Polars lazy frames and DuckDB SQL
- CLI-driven workflows and programmatic Python API

---

## 2. Platform Overview & Objectives

### 2.1 What ADP Is

A mini lakehouse + research factor platform that operates under a strict functional data engineering paradigm. Data flows through well-defined stages:

```
AWS Athena / Files  →  Raw  →  Staged  →  Normalized  →  Snapshots  →  Features  →  Research
```

### 2.2 Strategic Objectives

| ID | Objective | Success Metric |
|----|-----------|----------------|
| O1 | Reliable data ingestion from Athena and local files | Ingestion completes without data loss; metadata logged |
| O2 | Strict schema enforcement on all datasets | Pydantic validation rejects malformed records at ingestion |
| O3 | Immutable, versioned dataset snapshots | Every snapshot has a unique ID, schema hash, and lineage |
| O4 | Reproducible feature materialisation | Same snapshot + same definition = identical output (bit-level) |
| O5 | Fast local analysis | Sub-second queries on datasets up to 100M rows via DuckDB |
| O6 | Research-friendly access | One-line dataset/feature loading in Jupyter notebooks |

### 2.3 Core Architecture Principles

1. **Immutability** — Raw data is never modified. Normalized datasets are stored as immutable snapshots. Feature outputs are versioned and never overwritten.
2. **Reproducibility** — Each dataset and feature state is tied to: schema hash, raw ingestion lineage, normalization version, and feature definition version.
3. **Separation of Concerns** — Clear boundaries between ingestion, normalization, storage, snapshot management, and feature materialisation.
4. **Lazy Evaluation** — Polars lazy frames used for all large dataset transformations to prevent OOM issues.
5. **Data-as-Code** — Datasets and features behave like version-controlled artifacts.

---

## 3. Scope & Boundaries

### 3.1 In Scope

| Area | Details |
|------|---------|
| Data Sources | AWS Athena (via awswrangler), local files (CSV, TXT, JSON, Parquet) |
| Schema Enforcement | Pydantic-based dynamic model factory with fail-fast validation |
| Storage | Parquet columnar format, partitioned by date where applicable |
| Metadata | SQLite registry (WAL mode) for datasets, snapshots, features, lineage |
| Snapshot Engine | Immutable versioned snapshots with lineage tracking |
| Feature Layer | Deterministic feature materialisation from YAML definitions |
| Access Layer | Python API (`load_dataset`, `load_features`), DuckDB SQL, CLI |
| Research Integration | JupyterLab notebooks with example workflows |
| CLI | Typer-based CLI for all platform operations |

### 3.2 Out of Scope

| Area | Rationale |
|------|-----------|
| Distributed computing (Spark, Dask) | Local-only platform; Polars handles scale |
| Workflow orchestration (Airflow, Prefect) | CLI-driven; no DAG scheduler needed |
| Cloud deployment | Local development platform only |
| Real-time streaming | Batch-oriented analytical workflows |
| User authentication / multi-tenancy | Single-user local tool |
| Web UI / dashboards | Jupyter notebooks serve as the interactive layer |

---

## 4. Workstream Definitions

### WS1 — Foundation

**Goal:** Establish the project skeleton, configuration system, and metadata infrastructure.

| Deliverable | Description |
|-------------|-------------|
| Project scaffolding | `pyproject.toml`, directory structure, dependencies |
| Config loader | YAML-based configuration for datasets and features |
| SQLite schema | Registry database with all 7 metadata tables |
| Snapshot metadata system | Core metadata CRUD operations for datasets and snapshots |

**Key files:**

- `pyproject.toml`
- `src/adp/config.py`
- `src/adp/metadata/`
- `config/datasets.yaml`
- `config/features.yaml`

### WS2 — Ingestion Layer

**Goal:** Implement reliable data extraction from Athena and local file sources.

| Deliverable | Description |
|-------------|-------------|
| Athena ingestion | awswrangler-based extraction with query parameterization |
| File ingestion | CSV, TXT, JSON, Parquet file readers |
| Raw storage | Write raw data to `data/raw/` in source format |
| Ingestion metadata | Log every ingestion event with source, timestamp, location |

**Key files:**

- `src/adp/ingestion/strategies.py`
- `src/adp/ingestion/athena.py`
- `src/adp/ingestion/file.py`

### WS3 — Normalization & Snapshot Engine

**Goal:** Enforce schemas, normalize data, and produce immutable versioned snapshots.

| Deliverable | Description |
|-------------|-------------|
| Schema enforcement | Pydantic dynamic model factory for runtime validation |
| Timezone normalization | UTC conversion for all timestamp columns |
| Deduplication | Configurable dedup logic per dataset |
| Snapshot creation | Immutable Parquet snapshots with unique IDs |
| Snapshot lineage | Track which raw ingestions feed each snapshot |

**Key files:**

- `src/adp/processing/normalizer.py`
- `src/adp/processing/schema.py`
- `src/adp/storage/snapshot.py`
- `src/adp/storage/writer.py`

### WS4 — Feature Materialisation Engine

**Goal:** Build a deterministic, versioned feature computation layer.

| Deliverable | Description |
|-------------|-------------|
| Feature definition parser | Parse `features.yaml` into executable transformation specs |
| Feature transformation pipeline | Polars lazy + DuckDB SQL deterministic transformations |
| Feature snapshot versioning | Unique feature snapshot IDs with definition hashes |
| Feature lineage metadata | Track feature → dataset snapshot dependencies |
| Feature loading API | `load_features()` with optional snapshot pinning |
| CLI commands | `adp features build/list/show/load` |

**Key files:**

- `src/adp/features/definitions.py`
- `src/adp/features/materializer.py`
- `src/adp/features/registry.py`
- `src/adp/features/feature_store.py`
- `config/features.yaml`

### WS5 — Access Layer & Jupyter Integration

**Goal:** Provide ergonomic data access for research workflows.

| Deliverable | Description |
|-------------|-------------|
| `load_dataset()` API | Load any snapshot as a Polars LazyFrame |
| `load_features()` API | Load any feature set with optional snapshot pinning |
| DuckDB integration | SQL queries over Parquet snapshots and features |
| Example notebook | End-to-end demo: load → build features → query → backtest matrix |

**Key files:**

- `src/adp/api.py`
- `notebooks/research_demo.ipynb`

---

## 5. Milestone Schedule

| Milestone | Title | Workstream | Entry Criteria | Exit Criteria |
|-----------|-------|------------|----------------|---------------|
| **M1** | Scaffolding & Metadata | WS1 | Repo created, requirements agreed | Project builds, SQLite schema initialized, config loads, all unit tests pass |
| **M2** | Ingestion Layer | WS2 | M1 complete | Athena + file ingestion works, raw data stored, metadata logged, integration tests pass |
| **M3** | Snapshot Engine | WS3 | M2 complete | Normalized snapshots created, schema enforced, lineage tracked, reproducibility verified |
| **M4** | Feature Layer | WS4 | M3 complete | Features build from YAML, snapshots versioned, lineage recorded, determinism verified |
| **M5** | Access Layer | WS5 | M4 complete | APIs work, DuckDB queries execute, notebook demo runs end-to-end, all tests pass |

### Milestone Dependency Chain

```
M1 (Foundation)
 └──→ M2 (Ingestion)
       └──→ M3 (Snapshot Engine)
             └──→ M4 (Feature Layer)
                   └──→ M5 (Access Layer)
```

Each milestone is strictly sequential — the output of each phase is required input for the next.

---

## 6. Phase Gate Reviews

Each milestone concludes with a gate review. The build does not proceed until all gate criteria are met.

### Gate 1 — Foundation Complete

| Criterion | Evidence |
|-----------|----------|
| Project installs cleanly | `pip install -e .` succeeds |
| SQLite schema is initialized | All 7 tables exist with correct DDL |
| Config files parse without error | `datasets.yaml` and `features.yaml` load successfully |
| Unit tests pass | `pytest tests/unit/` — 100% pass rate |
| Directory structure matches spec | All directories exist per architecture |

### Gate 2 — Ingestion Complete

| Criterion | Evidence |
|-----------|----------|
| Athena ingestion executes | Query returns data, stored to `data/raw/` |
| File ingestion works for all formats | CSV, TXT, JSON, Parquet files ingested |
| Metadata is logged | `raw_ingestions` table populated with correct records |
| Integration tests pass | `pytest tests/integration/ingestion/` — 100% pass rate |

### Gate 3 — Snapshot Engine Complete

| Criterion | Evidence |
|-----------|----------|
| Schema validation rejects bad data | Malformed records raise `ValidationError` |
| Timestamps normalized to UTC | All datetime columns in UTC after normalization |
| Duplicates removed | Dedup logic verified on test data |
| Snapshot created as immutable Parquet | Snapshot file exists in `data/normalized/` |
| Lineage tracked | `snapshots` and `snapshot_lineage` tables populated |
| Reproducibility verified | Re-running normalization on same input produces identical snapshot |

### Gate 4 — Feature Layer Complete

| Criterion | Evidence |
|-----------|----------|
| Feature YAML parses correctly | `features.yaml` loaded into typed definitions |
| Features build deterministically | Same snapshot + definition = identical output |
| Feature snapshots versioned | Unique IDs with definition hashes in metadata |
| Feature lineage recorded | `feature_snapshots` and `feature_lineage` tables populated |
| CLI commands work | `adp features build/list/show/load` execute correctly |

### Gate 5 — Access Layer Complete

| Criterion | Evidence |
|-----------|----------|
| `load_dataset()` returns LazyFrame | Correct data loaded, schema matches |
| `load_features()` returns LazyFrame | Feature data loaded with optional snapshot pinning |
| DuckDB queries execute | SQL over Parquet returns correct results |
| Notebook runs end-to-end | Demo notebook executes without errors |
| All tests pass | Full test suite — 100% pass rate |

---

## 7. Dependency Map

```
┌─────────────────────────────────────────────────────────────┐
│                    DEPENDENCY MATRIX                         │
├────────────┬──────┬──────┬──────┬──────┬──────┐            │
│            │ WS1  │ WS2  │ WS3  │ WS4  │ WS5  │            │
├────────────┼──────┼──────┼──────┼──────┼──────┤            │
│ WS1        │  —   │      │      │      │      │            │
│ WS2        │  ●   │  —   │      │      │      │            │
│ WS3        │  ●   │  ●   │  —   │      │      │            │
│ WS4        │  ●   │      │  ●   │  —   │      │            │
│ WS5        │  ●   │      │  ●   │  ●   │  —   │            │
└────────────┴──────┴──────┴──────┴──────┴──────┘            │
│  ● = depends on                                             │
└─────────────────────────────────────────────────────────────┘
```

**Critical path:** WS1 → WS2 → WS3 → WS4 → WS5

**Key dependencies:**

- WS2 requires WS1 (config loader, metadata registry, storage paths)
- WS3 requires WS2 (raw ingested data as input to normalization)
- WS4 requires WS3 (normalized snapshots as input to feature materialisation)
- WS5 requires WS3 + WS4 (both snapshots and features must exist to load)

---

## 8. RACI Matrix

| Activity | Platform Engineer | Quant Researcher | Data Engineer | Tech Lead |
|----------|:-----------------:|:----------------:|:-------------:|:---------:|
| Project scaffolding (WS1) | R | — | C | A |
| Config schema design | R | C | C | A |
| SQLite metadata schema | R | — | C | A |
| Athena ingestion (WS2) | R | — | C | A |
| File ingestion (WS2) | R | — | R | A |
| Schema enforcement (WS3) | R | C | C | A |
| Normalization pipeline (WS3) | R | C | — | A |
| Snapshot engine (WS3) | R | — | C | A |
| Feature definitions (WS4) | C | R | — | A |
| Feature materialiser (WS4) | R | C | — | A |
| Feature store (WS4) | R | — | C | A |
| Python API (WS5) | R | C | — | A |
| DuckDB integration (WS5) | R | C | C | A |
| Jupyter notebooks (WS5) | C | R | — | A |
| Test strategy & execution | R | C | C | A |
| Code review | C | — | C | R |
| Gate reviews | I | C | C | R |

> **R** = Responsible, **A** = Accountable, **C** = Consulted, **I** = Informed

---

## 9. Delivery Cadence & Ceremonies

| Ceremony | Frequency | Participants | Purpose |
|----------|-----------|--------------|---------|
| Daily standup | Daily | All | Progress updates, blocker identification |
| Milestone planning | Per milestone start | All | Scope confirmation, task breakdown |
| Gate review | Per milestone end | All | Acceptance criteria verification |
| Design review | As needed | Platform Eng + Tech Lead | Architecture decisions, protocol design |
| Research sync | Weekly | Quant Researcher + Platform Eng | Feature requirements, API ergonomics |
| Retrospective | Per milestone end | All | Process improvement |

### Definition of Done (Global)

A workstream deliverable is "done" when:

1. All code is written and follows project conventions
2. Unit tests pass with ≥90% coverage for the deliverable
3. Integration tests pass where applicable
4. Code is reviewed and approved
5. Metadata schema changes are applied
6. Documentation is updated (docstrings, YAML examples)
7. Gate review criteria are met

---

## 10. Risk Register

### Risk Scoring

**Score = Probability × Impact**

| Score Range | Action Required |
|-------------|-----------------|
| 1–3 | Monitor — no immediate action |
| 4–6 | Mitigate — implement controls |
| 7–9 | Escalate — requires design change or scope adjustment |

### Risks

| ID | Risk | Probability (1–3) | Impact (1–3) | Score | Mitigation |
|----|------|:------------------:|:------------:|:-----:|------------|
| R1 | **Storage growth** — immutable data accumulates without bound | 2 | 3 | 6 | Implement retention policy; archive raw data after N snapshots; document storage budget |
| R2 | **OOM on large datasets** — eager evaluation on large normalizations | 2 | 3 | 6 | Enforce Polars lazy pipelines; add memory guards; chunk file ingestion |
| R3 | **Schema drift** — Athena table schema changes without notice | 2 | 2 | 4 | Pydantic fail-fast validation catches at ingestion; alert on schema hash mismatch |
| R4 | **Metadata corruption** — crash during SQLite write | 1 | 3 | 3 | SQLite WAL mode; wrap all writes in transactions; test crash recovery |
| R5 | **Feature drift** — feature definitions change silently | 2 | 3 | 6 | Hash feature definitions; version control YAML; compare hashes before materialisation |
| R6 | **Athena availability** — AWS service outages block ingestion | 1 | 2 | 2 | File fallback ingestion; cache last-known-good raw data |
| R7 | **DuckDB compatibility** — Parquet schema mismatches | 1 | 2 | 2 | Standardize Parquet writer settings; test DuckDB reads in integration suite |
| R8 | **Polars API changes** — upstream breaking changes | 1 | 2 | 2 | Pin Polars version in `pyproject.toml`; test on upgrade |

---

## 11. Constraints & Assumptions

### Constraints

| ID | Constraint |
|----|------------|
| C1 | No Spark, Airflow, or distributed cluster dependencies |
| C2 | All data must be stored locally in Parquet format |
| C3 | SQLite is the only metadata store (no Postgres, no cloud DB) |
| C4 | Python >=3.11, <3.14 required (for Polars, DuckDB, and modern type hints) |
| C5 | Platform must run on a single machine without network services |

### Assumptions

| ID | Assumption |
|----|------------|
| A1 | AWS credentials are pre-configured for Athena access |
| A2 | Datasets fit in local disk (not exceeding available storage) |
| A3 | Users have JupyterLab installed or can install it |
| A4 | Feature definitions are maintained in version-controlled YAML |
| A5 | A single normalization version applies per dataset at any point in time |

---

## 12. Architectural Decision Records

| ADR | Decision Area | Selected Technology | Version | Rationale |
|-----|---------------|--------------------:|---------|-----------|
| ADR-001 | DataFrame Engine | Polars | ~=1.38 | Fast columnar engine with native lazy execution; avoids Pandas memory overhead |
| ADR-002 | Local Analytical DB | DuckDB | ~=1.4 | SQL-over-Parquet with zero-copy reads; embedded, no server process |
| ADR-003 | Metadata Registry | SQLite (WAL mode) | stdlib | Lightweight, embedded, transactional; perfect for single-user lineage tracking |
| ADR-004 | File Format | Parquet | — | Columnar, partition-friendly, ecosystem-standard for analytical workloads |
| ADR-005 | Feature Execution | Polars Lazy + DuckDB SQL | — | Deterministic lazy evaluation for transforms; SQL for ad-hoc analysis |
| ADR-006 | Orchestration | Typer CLI | ~=0.24 | Simple, typed CLI framework; no DAG overhead for research-driven workflows |
| ADR-007 | Validation | Pydantic | ~=2.12 | Strict runtime schema enforcement with clear error messages |
| ADR-008 | Athena Client | awswrangler | ~=3.15 | Mature library for Athena queries; handles pagination and result caching |

---

## 13. Change Log

| Date | Version | Author | Change |
|------|---------|--------|--------|
| 2026-03-02 | 1.0 | — | Initial document creation |

---

*Cross-references: [User Stories](./02-user-stories.md) · [Design Document](./03-design-document.md) · [Test Strategy](./04-test-strategy.md) · [Quant Infrastructure](./05-quant-infrastructure.md)*
