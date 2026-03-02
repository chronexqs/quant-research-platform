"""SQLite DDL and connection helpers for the ADP metadata registry."""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_DDL = """\
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS datasets (
    dataset_name    TEXT PRIMARY KEY,
    description     TEXT DEFAULT '',
    current_snapshot TEXT,
    schema_hash     TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_ingestions (
    ingestion_id        TEXT PRIMARY KEY,
    dataset_name        TEXT NOT NULL REFERENCES datasets(dataset_name),
    source_type         TEXT NOT NULL,
    source_location     TEXT NOT NULL,
    row_count           INTEGER NOT NULL,
    ingestion_timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id            TEXT PRIMARY KEY,
    dataset_name           TEXT NOT NULL REFERENCES datasets(dataset_name),
    schema_hash            TEXT NOT NULL,
    normalization_version  TEXT NOT NULL,
    row_count              INTEGER NOT NULL,
    storage_path           TEXT NOT NULL,
    created_at             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_lineage (
    snapshot_id  TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    ingestion_id TEXT NOT NULL REFERENCES raw_ingestions(ingestion_id),
    PRIMARY KEY (snapshot_id, ingestion_id)
);

CREATE TABLE IF NOT EXISTS feature_definitions (
    feature_name    TEXT NOT NULL,
    dataset_name    TEXT NOT NULL REFERENCES datasets(dataset_name),
    version         INTEGER NOT NULL,
    definition_hash TEXT NOT NULL,
    definition_yaml TEXT,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (feature_name, dataset_name, version)
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
    feature_snapshot_id TEXT PRIMARY KEY,
    feature_name        TEXT NOT NULL,
    dataset_name        TEXT NOT NULL,
    feature_version     INTEGER NOT NULL,
    definition_hash     TEXT NOT NULL,
    row_count           INTEGER NOT NULL,
    storage_path        TEXT NOT NULL,
    created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS feature_lineage (
    feature_snapshot_id  TEXT NOT NULL REFERENCES feature_snapshots(feature_snapshot_id),
    dataset_snapshot_id  TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    PRIMARY KEY (feature_snapshot_id, dataset_snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_ingestions_dataset ON raw_ingestions(dataset_name);
CREATE INDEX IF NOT EXISTS idx_snapshots_dataset ON snapshots(dataset_name);
CREATE INDEX IF NOT EXISTS idx_feat_snaps_ds
    ON feature_snapshots(dataset_name, feature_name);
CREATE INDEX IF NOT EXISTS idx_feature_defs_dataset ON feature_definitions(dataset_name);
"""


def get_connection(db_path: Path | str) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and foreign keys enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    """Execute the DDL to create all tables and indexes."""
    conn.executescript(SCHEMA_DDL)
