"""SQLite DDL and connection helpers for the ADP metadata registry."""

from __future__ import annotations

import sqlite3
from pathlib import Path

# DDL for the 7-table metadata schema.  The design follows a star-like layout:
# - `datasets` is the central dimension table.
# - `raw_ingestions` and `snapshots` track the ingest-then-normalize lifecycle.
# - `snapshot_lineage` is a many-to-many bridge (snapshot <-> ingestions).
# - `feature_definitions`, `feature_snapshots`, and `feature_lineage` mirror
#   the same pattern for the feature computation layer.
# All timestamps are stored as ISO-8601 TEXT for portability.
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
    """Open a SQLite connection with WAL mode and foreign keys enabled.

    Args:
        db_path: Filesystem path to the SQLite database file.  The file is
            created automatically by SQLite if it does not exist.

    Returns:
        A ``sqlite3.Connection`` configured with WAL journaling and
        foreign-key enforcement.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def initialize_schema(conn: sqlite3.Connection) -> None:
    """Execute the DDL to create all metadata tables and indexes.

    This is idempotent -- every statement uses ``CREATE TABLE IF NOT EXISTS``
    and ``CREATE INDEX IF NOT EXISTS``, so it is safe to call on every startup.

    Args:
        conn: An open SQLite connection (typically obtained from
            :func:`get_connection`).
    """
    conn.executescript(SCHEMA_DDL)
