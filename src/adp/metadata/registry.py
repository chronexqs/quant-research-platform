"""MetadataRegistry — Central SQLite-backed metadata store."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from adp.exceptions import MetadataError
from adp.metadata.models import (
    DatasetRecord,
    FeatureDefinitionRecord,
    FeatureLineageRecord,
    FeatureSnapshotRecord,
    IngestionRecord,
    SnapshotLineageRecord,
    SnapshotRecord,
)
from adp.metadata.schema import get_connection, initialize_schema


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def _parse_dt(s: str) -> datetime:
    """Parse an ISO-8601 string into a timezone-aware ``datetime``."""
    return datetime.fromisoformat(s)


# ── SQL constants ─────────────────────────────────────────────

_SQL_INSERT_DATASET = """\
INSERT INTO datasets
    (dataset_name, description, current_snapshot, schema_hash,
     created_at, updated_at)
VALUES (?, ?, NULL, ?, ?, ?)"""

_SQL_SELECT_DATASET = """\
SELECT dataset_name, description, current_snapshot,
       schema_hash, created_at, updated_at
FROM datasets WHERE dataset_name = ?"""

_SQL_LIST_DATASETS = """\
SELECT dataset_name, description, current_snapshot,
       schema_hash, created_at, updated_at
FROM datasets ORDER BY dataset_name"""

_SQL_INSERT_INGESTION = """\
INSERT INTO raw_ingestions
    (ingestion_id, dataset_name, source_type, source_location,
     row_count, ingestion_timestamp)
VALUES (?, ?, ?, ?, ?, ?)"""

_SQL_SELECT_INGESTION = """\
SELECT ingestion_id, dataset_name, source_type, source_location,
       row_count, ingestion_timestamp
FROM raw_ingestions WHERE ingestion_id = ?"""

_SQL_LIST_INGESTIONS = """\
SELECT ingestion_id, dataset_name, source_type, source_location,
       row_count, ingestion_timestamp
FROM raw_ingestions
WHERE dataset_name = ? ORDER BY ingestion_timestamp DESC"""

_SQL_FIND_INGESTION_BY_SRC = """\
SELECT ingestion_id, dataset_name, source_type, source_location,
       row_count, ingestion_timestamp
FROM raw_ingestions
WHERE dataset_name = ? AND source_location = ?
ORDER BY ingestion_timestamp DESC LIMIT 1"""

_SQL_INSERT_SNAPSHOT = """\
INSERT INTO snapshots
    (snapshot_id, dataset_name, schema_hash, normalization_version,
     row_count, storage_path, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)"""

_SQL_SELECT_SNAPSHOT = """\
SELECT snapshot_id, dataset_name, schema_hash,
       normalization_version, row_count, storage_path, created_at
FROM snapshots WHERE snapshot_id = ?"""

_SQL_LIST_SNAPSHOTS = """\
SELECT snapshot_id, dataset_name, schema_hash,
       normalization_version, row_count, storage_path, created_at
FROM snapshots WHERE dataset_name = ? ORDER BY created_at DESC"""

_SQL_INSERT_FEAT_DEF = """\
INSERT OR IGNORE INTO feature_definitions
    (feature_name, dataset_name, version, definition_hash,
     definition_yaml, created_at)
VALUES (?, ?, ?, ?, ?, ?)"""

_SQL_SELECT_FEAT_DEF = """\
SELECT feature_name, dataset_name, version, definition_hash,
       definition_yaml, created_at
FROM feature_definitions
WHERE feature_name = ? AND dataset_name = ? AND version = ?"""

_SQL_SELECT_FEAT_DEF_LATEST = """\
SELECT feature_name, dataset_name, version, definition_hash,
       definition_yaml, created_at
FROM feature_definitions
WHERE feature_name = ? AND dataset_name = ?
ORDER BY version DESC LIMIT 1"""

_SQL_LIST_FEAT_DEFS = """\
SELECT feature_name, dataset_name, version, definition_hash,
       definition_yaml, created_at
FROM feature_definitions
WHERE dataset_name = ? ORDER BY feature_name, version DESC"""

_SQL_INSERT_FEAT_SNAP = """\
INSERT INTO feature_snapshots
    (feature_snapshot_id, feature_name, dataset_name,
     feature_version, definition_hash, row_count,
     storage_path, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""

_SQL_SELECT_FEAT_SNAP = """\
SELECT feature_snapshot_id, feature_name, dataset_name,
       feature_version, definition_hash, row_count,
       storage_path, created_at
FROM feature_snapshots WHERE feature_snapshot_id = ?"""

_SQL_SELECT_FEAT_SNAP_LATEST = """\
SELECT feature_snapshot_id, feature_name, dataset_name,
       feature_version, definition_hash, row_count,
       storage_path, created_at
FROM feature_snapshots
WHERE feature_name = ? AND dataset_name = ?
ORDER BY created_at DESC LIMIT 1"""

_SQL_LIST_FEAT_SNAPS_ALL = """\
SELECT feature_snapshot_id, feature_name, dataset_name,
       feature_version, definition_hash, row_count,
       storage_path, created_at
FROM feature_snapshots
WHERE dataset_name = ?
ORDER BY created_at DESC"""

_SQL_LIST_FEAT_SNAPS_BY_NAME = """\
SELECT feature_snapshot_id, feature_name, dataset_name,
       feature_version, definition_hash, row_count,
       storage_path, created_at
FROM feature_snapshots
WHERE dataset_name = ? AND feature_name = ?
ORDER BY created_at DESC"""

_SQL_NEXT_SEQ = """\
SELECT MAX(CAST(SUBSTR(id, LENGTH(?) + 1) AS INTEGER))
FROM (
    SELECT ingestion_id AS id FROM raw_ingestions
        WHERE ingestion_id LIKE ?
    UNION ALL
    SELECT snapshot_id AS id FROM snapshots
        WHERE snapshot_id LIKE ?
    UNION ALL
    SELECT feature_snapshot_id AS id FROM feature_snapshots
        WHERE feature_snapshot_id LIKE ?
)"""


def _to_dataset(row: tuple[Any, ...]) -> DatasetRecord:
    """Convert a raw SQLite row tuple into a :class:`DatasetRecord`."""
    return DatasetRecord(
        row[0],
        row[1],
        row[2],
        row[3],
        _parse_dt(row[4]),
        _parse_dt(row[5]),
    )


def _to_ingestion(row: tuple[Any, ...]) -> IngestionRecord:
    """Convert a raw SQLite row tuple into an :class:`IngestionRecord`."""
    return IngestionRecord(row[0], row[1], row[2], row[3], row[4], _parse_dt(row[5]))


def _to_snapshot(row: tuple[Any, ...]) -> SnapshotRecord:
    """Convert a raw SQLite row tuple into a :class:`SnapshotRecord`."""
    return SnapshotRecord(
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        _parse_dt(row[6]),
    )


def _to_feat_def(row: tuple[Any, ...]) -> FeatureDefinitionRecord:
    """Convert a raw SQLite row tuple into a :class:`FeatureDefinitionRecord`."""
    return FeatureDefinitionRecord(row[0], row[1], row[2], row[3], row[4], _parse_dt(row[5]))


def _to_feat_snap(row: tuple[Any, ...]) -> FeatureSnapshotRecord:
    """Convert a raw SQLite row tuple into a :class:`FeatureSnapshotRecord`."""
    return FeatureSnapshotRecord(
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        _parse_dt(row[7]),
    )


class MetadataRegistry:
    """Single-user metadata registry backed by SQLite (WAL mode).

    Provides a transactional API for managing the full ADP metadata lifecycle:
    dataset registration, raw-ingestion logging, snapshot creation, feature
    definition tracking, feature snapshot materialisation, and lineage linking.

    The registry is designed for single-process use.  Concurrent writes from
    multiple processes are **not** supported.

    Attributes:
        db_path: Filesystem path to the underlying SQLite database.
    """

    def __init__(self, db_path: Path | str) -> None:
        self.db_path = db_path
        self._conn = get_connection(db_path)
        initialize_schema(self._conn)

    @property
    def conn(self) -> sqlite3.Connection:
        return self._conn

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Cursor]:
        """Commits on success, rolls back on error."""
        cursor = self._conn.cursor()
        try:
            cursor.execute("BEGIN")
            yield cursor
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> MetadataRegistry:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    # ── Datasets ──────────────────────────────────────────

    def register_dataset(
        self,
        dataset_name: str,
        schema_hash: str,
        description: str = "",
    ) -> DatasetRecord:
        """Register a new dataset in the metadata store.

        Args:
            dataset_name: Unique name for the dataset.
            schema_hash: Hash of the dataset's column schema definition.
            description: Optional human-readable description.

        Returns:
            The newly created :class:`DatasetRecord`.

        Raises:
            MetadataError: If a dataset with the same name already exists.
        """
        now = _now_iso()
        try:
            with self.transaction() as cur:
                cur.execute(
                    _SQL_INSERT_DATASET,
                    (dataset_name, description, schema_hash, now, now),
                )
        except sqlite3.IntegrityError as e:
            raise MetadataError(f"Dataset '{dataset_name}' is already registered") from e
        return DatasetRecord(
            dataset_name=dataset_name,
            description=description,
            current_snapshot=None,
            schema_hash=schema_hash,
            created_at=_parse_dt(now),
            updated_at=_parse_dt(now),
        )

    def get_dataset(self, dataset_name: str) -> DatasetRecord | None:
        """Retrieve a dataset record by name.

        Args:
            dataset_name: The dataset to look up.

        Returns:
            The matching :class:`DatasetRecord`, or ``None`` if not found.
        """
        row = self._conn.execute(_SQL_SELECT_DATASET, (dataset_name,)).fetchone()
        return _to_dataset(row) if row else None

    def list_datasets(self) -> list[DatasetRecord]:
        """Return all registered datasets, ordered alphabetically by name."""
        rows = self._conn.execute(_SQL_LIST_DATASETS).fetchall()
        return [_to_dataset(r) for r in rows]

    def update_current_snapshot(self, dataset_name: str, snapshot_id: str) -> None:
        """Point a dataset's ``current_snapshot`` to the given snapshot ID.

        Args:
            dataset_name: Dataset to update.
            snapshot_id: Snapshot ID to set as current.

        Raises:
            MetadataError: If the dataset does not exist.
        """
        now = _now_iso()
        with self.transaction() as cur:
            cur.execute(
                "UPDATE datasets SET current_snapshot = ?, updated_at = ? WHERE dataset_name = ?",
                (snapshot_id, now, dataset_name),
            )
            if cur.rowcount == 0:
                raise MetadataError(f"Dataset '{dataset_name}' not found")

    def update_schema_hash(self, dataset_name: str, schema_hash: str) -> None:
        """Update the stored schema hash for a dataset.

        Args:
            dataset_name: Dataset whose schema hash should be updated.
            schema_hash: New schema hash value.
        """
        now = _now_iso()
        with self.transaction() as cur:
            cur.execute(
                "UPDATE datasets SET schema_hash = ?, updated_at = ? WHERE dataset_name = ?",
                (schema_hash, now, dataset_name),
            )

    # ── Ingestions ─────────────────────────────────────────

    def log_ingestion(
        self,
        ingestion_id: str,
        dataset_name: str,
        source_type: str,
        source_location: str,
        row_count: int,
    ) -> IngestionRecord:
        """Record a raw data ingestion event.

        Args:
            ingestion_id: Pre-generated unique ingestion identifier.
            dataset_name: Dataset that was ingested into.
            source_type: Type of source (e.g. ``"csv"``, ``"api"``).
            source_location: URI or path to the original source data.
            row_count: Number of rows ingested.

        Returns:
            The newly created :class:`IngestionRecord`.
        """
        now = _now_iso()
        with self.transaction() as cur:
            cur.execute(
                _SQL_INSERT_INGESTION,
                (
                    ingestion_id,
                    dataset_name,
                    source_type,
                    source_location,
                    row_count,
                    now,
                ),
            )
        return IngestionRecord(
            ingestion_id=ingestion_id,
            dataset_name=dataset_name,
            source_type=source_type,
            source_location=source_location,
            row_count=row_count,
            ingestion_timestamp=_parse_dt(now),
        )

    def get_ingestion(self, ingestion_id: str) -> IngestionRecord | None:
        """Retrieve an ingestion record by its ID.

        Args:
            ingestion_id: The ingestion to look up.

        Returns:
            The matching :class:`IngestionRecord`, or ``None`` if not found.
        """
        row = self._conn.execute(_SQL_SELECT_INGESTION, (ingestion_id,)).fetchone()
        return _to_ingestion(row) if row else None

    def list_ingestions(self, dataset_name: str) -> list[IngestionRecord]:
        """List all ingestion records for a dataset, most recent first.

        Args:
            dataset_name: Dataset to query.

        Returns:
            List of :class:`IngestionRecord` ordered by timestamp descending.
        """
        rows = self._conn.execute(_SQL_LIST_INGESTIONS, (dataset_name,)).fetchall()
        return [_to_ingestion(r) for r in rows]

    def find_ingestion_by_source(
        self, dataset_name: str, source_location: str
    ) -> IngestionRecord | None:
        """Find the most recent ingestion for a given source location.

        Args:
            dataset_name: Dataset to search within.
            source_location: URI or path of the original source to match.

        Returns:
            The most recent matching :class:`IngestionRecord`, or ``None``.
        """
        row = self._conn.execute(
            _SQL_FIND_INGESTION_BY_SRC,
            (dataset_name, source_location),
        ).fetchone()
        return _to_ingestion(row) if row else None

    # ── Snapshots ─────────────────────────────────────────

    def create_snapshot(
        self,
        snapshot_id: str,
        dataset_name: str,
        schema_hash: str,
        normalization_version: str,
        row_count: int,
        storage_path: str,
    ) -> SnapshotRecord:
        """Record a new normalized dataset snapshot.

        Args:
            snapshot_id: Pre-generated unique snapshot identifier.
            dataset_name: Dataset this snapshot belongs to.
            schema_hash: Hash of the column schema at creation time.
            normalization_version: Version string of the normalization
                pipeline used.
            row_count: Number of rows in the snapshot.
            storage_path: Filesystem path where the Parquet data is stored.

        Returns:
            The newly created :class:`SnapshotRecord`.
        """
        now = _now_iso()
        with self.transaction() as cur:
            cur.execute(
                _SQL_INSERT_SNAPSHOT,
                (
                    snapshot_id,
                    dataset_name,
                    schema_hash,
                    normalization_version,
                    row_count,
                    storage_path,
                    now,
                ),
            )
        return SnapshotRecord(
            snapshot_id=snapshot_id,
            dataset_name=dataset_name,
            schema_hash=schema_hash,
            normalization_version=normalization_version,
            row_count=row_count,
            storage_path=storage_path,
            created_at=_parse_dt(now),
        )

    def get_snapshot(self, snapshot_id: str) -> SnapshotRecord | None:
        """Retrieve a snapshot record by its ID.

        Args:
            snapshot_id: The snapshot to look up.

        Returns:
            The matching :class:`SnapshotRecord`, or ``None`` if not found.
        """
        row = self._conn.execute(_SQL_SELECT_SNAPSHOT, (snapshot_id,)).fetchone()
        return _to_snapshot(row) if row else None

    def list_snapshots(self, dataset_name: str) -> list[SnapshotRecord]:
        """List all snapshots for a dataset, most recent first.

        Args:
            dataset_name: Dataset to query.

        Returns:
            List of :class:`SnapshotRecord` ordered by creation time descending.
        """
        rows = self._conn.execute(_SQL_LIST_SNAPSHOTS, (dataset_name,)).fetchall()
        return [_to_snapshot(r) for r in rows]

    def link_snapshot_lineage(self, snapshot_id: str, ingestion_id: str) -> SnapshotLineageRecord:
        """Create a lineage link between a snapshot and a source ingestion.

        Args:
            snapshot_id: The snapshot that was derived from the ingestion.
            ingestion_id: The raw ingestion that contributed to the snapshot.

        Returns:
            The newly created :class:`SnapshotLineageRecord`.
        """
        with self.transaction() as cur:
            cur.execute(
                "INSERT INTO snapshot_lineage (snapshot_id, ingestion_id) VALUES (?, ?)",
                (snapshot_id, ingestion_id),
            )
        return SnapshotLineageRecord(
            snapshot_id=snapshot_id,
            ingestion_id=ingestion_id,
        )

    def get_snapshot_lineage(self, snapshot_id: str) -> list[SnapshotLineageRecord]:
        """Retrieve all ingestion lineage links for a snapshot.

        Args:
            snapshot_id: The snapshot to query lineage for.

        Returns:
            List of :class:`SnapshotLineageRecord` linking to source ingestions.
        """
        rows = self._conn.execute(
            "SELECT snapshot_id, ingestion_id FROM snapshot_lineage WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchall()
        return [SnapshotLineageRecord(r[0], r[1]) for r in rows]

    # ── Features ──────────────────────────────────────────

    def register_feature_definition(
        self,
        feature_name: str,
        dataset_name: str,
        version: int,
        definition_hash: str,
        definition_yaml: str | None = None,
    ) -> FeatureDefinitionRecord:
        """Register a versioned feature definition (idempotent).

        If the exact ``(feature_name, dataset_name, version)`` triple already
        exists with the same hash, this is a no-op.  If the triple exists but
        the hash differs, a :class:`MetadataError` is raised to prevent
        silent definition drift.

        Args:
            feature_name: Logical name of the feature set.
            dataset_name: Target dataset name.
            version: Monotonically increasing version number.
            definition_hash: SHA-256 hash of the feature definitions.
            definition_yaml: Optional raw YAML text for auditability.

        Returns:
            The :class:`FeatureDefinitionRecord` (new or existing).

        Raises:
            MetadataError: If the same version already exists with a
                different definition hash.
        """
        # Check for hash mismatch on same version (definition changed
        # without version bump)
        existing = self.get_feature_definition(feature_name, dataset_name, version)
        if existing and existing.definition_hash != definition_hash:
            raise MetadataError(
                f"Definition hash mismatch for '{feature_name}' v{version}. "
                "Bump the version number when changing feature definitions."
            )

        now = _now_iso()
        with self.transaction() as cur:
            cur.execute(
                _SQL_INSERT_FEAT_DEF,
                (
                    feature_name,
                    dataset_name,
                    version,
                    definition_hash,
                    definition_yaml,
                    now,
                ),
            )
        return FeatureDefinitionRecord(
            feature_name=feature_name,
            dataset_name=dataset_name,
            version=version,
            definition_hash=definition_hash,
            definition_yaml=definition_yaml,
            created_at=_parse_dt(now),
        )

    def get_feature_definition(
        self,
        feature_name: str,
        dataset_name: str,
        version: int | None = None,
    ) -> FeatureDefinitionRecord | None:
        """Retrieve a feature definition record.

        Args:
            feature_name: Logical name of the feature set.
            dataset_name: Target dataset name.
            version: Specific version to fetch.  When ``None`` the latest
                version is returned.

        Returns:
            The matching :class:`FeatureDefinitionRecord`, or ``None``.
        """
        if version is not None:
            row = self._conn.execute(
                _SQL_SELECT_FEAT_DEF,
                (feature_name, dataset_name, version),
            ).fetchone()
        else:
            row = self._conn.execute(
                _SQL_SELECT_FEAT_DEF_LATEST,
                (feature_name, dataset_name),
            ).fetchone()
        return _to_feat_def(row) if row else None

    def list_feature_definitions(self, dataset_name: str) -> list[FeatureDefinitionRecord]:
        """List all feature definitions for a dataset, ordered by name and version.

        Args:
            dataset_name: Dataset to query.

        Returns:
            List of :class:`FeatureDefinitionRecord` sorted by feature name
            then version descending.
        """
        rows = self._conn.execute(_SQL_LIST_FEAT_DEFS, (dataset_name,)).fetchall()
        return [_to_feat_def(r) for r in rows]

    def create_feature_snapshot(
        self,
        feature_snapshot_id: str,
        feature_name: str,
        dataset_name: str,
        feature_version: int,
        definition_hash: str,
        row_count: int,
        storage_path: str,
    ) -> FeatureSnapshotRecord:
        """Record a new materialised feature snapshot.

        Args:
            feature_snapshot_id: Pre-generated unique identifier.
            feature_name: Logical name of the feature set.
            dataset_name: Source dataset name.
            feature_version: Version of the feature definition used.
            definition_hash: SHA-256 hash of the feature definitions.
            row_count: Number of rows in the snapshot.
            storage_path: Filesystem path where the Parquet data is stored.

        Returns:
            The newly created :class:`FeatureSnapshotRecord`.
        """
        now = _now_iso()
        with self.transaction() as cur:
            cur.execute(
                _SQL_INSERT_FEAT_SNAP,
                (
                    feature_snapshot_id,
                    feature_name,
                    dataset_name,
                    feature_version,
                    definition_hash,
                    row_count,
                    storage_path,
                    now,
                ),
            )
        return FeatureSnapshotRecord(
            feature_snapshot_id=feature_snapshot_id,
            feature_name=feature_name,
            dataset_name=dataset_name,
            feature_version=feature_version,
            definition_hash=definition_hash,
            row_count=row_count,
            storage_path=storage_path,
            created_at=_parse_dt(now),
        )

    def get_feature_snapshot(self, feature_snapshot_id: str) -> FeatureSnapshotRecord | None:
        """Retrieve a feature snapshot record by its ID.

        Args:
            feature_snapshot_id: The feature snapshot to look up.

        Returns:
            The matching :class:`FeatureSnapshotRecord`, or ``None``.
        """
        row = self._conn.execute(_SQL_SELECT_FEAT_SNAP, (feature_snapshot_id,)).fetchone()
        return _to_feat_snap(row) if row else None

    def get_latest_feature_snapshot(
        self, feature_name: str, dataset_name: str
    ) -> FeatureSnapshotRecord | None:
        """Retrieve the most recently created feature snapshot for a feature set.

        Args:
            feature_name: Logical name of the feature set.
            dataset_name: Source dataset name.

        Returns:
            The latest :class:`FeatureSnapshotRecord`, or ``None`` if no
            snapshots exist.
        """
        row = self._conn.execute(
            _SQL_SELECT_FEAT_SNAP_LATEST,
            (feature_name, dataset_name),
        ).fetchone()
        return _to_feat_snap(row) if row else None

    def list_feature_snapshots(
        self,
        dataset_name: str,
        feature_name: str | None = None,
    ) -> list[FeatureSnapshotRecord]:
        """List feature snapshots for a dataset, most recent first.

        Args:
            dataset_name: Dataset to query.
            feature_name: Optional filter to restrict results to a single
                feature set.  When ``None`` all feature snapshots for the
                dataset are returned.

        Returns:
            List of :class:`FeatureSnapshotRecord` ordered by creation time
            descending.
        """
        if feature_name:
            rows = self._conn.execute(
                _SQL_LIST_FEAT_SNAPS_BY_NAME,
                (dataset_name, feature_name),
            ).fetchall()
        else:
            rows = self._conn.execute(_SQL_LIST_FEAT_SNAPS_ALL, (dataset_name,)).fetchall()
        return [_to_feat_snap(r) for r in rows]

    def link_feature_lineage(
        self,
        feature_snapshot_id: str,
        dataset_snapshot_id: str,
    ) -> FeatureLineageRecord:
        """Create a lineage link between a feature snapshot and its source dataset snapshot.

        Args:
            feature_snapshot_id: The feature snapshot that was derived.
            dataset_snapshot_id: The dataset snapshot used as input.

        Returns:
            The newly created :class:`FeatureLineageRecord`.
        """
        with self.transaction() as cur:
            cur.execute(
                "INSERT INTO feature_lineage "
                "(feature_snapshot_id, dataset_snapshot_id) "
                "VALUES (?, ?)",
                (feature_snapshot_id, dataset_snapshot_id),
            )
        return FeatureLineageRecord(
            feature_snapshot_id=feature_snapshot_id,
            dataset_snapshot_id=dataset_snapshot_id,
        )

    def get_feature_lineage(self, feature_snapshot_id: str) -> list[FeatureLineageRecord]:
        """Retrieve all dataset-snapshot lineage links for a feature snapshot.

        Args:
            feature_snapshot_id: The feature snapshot to query lineage for.

        Returns:
            List of :class:`FeatureLineageRecord` linking to source dataset
            snapshots.
        """
        rows = self._conn.execute(
            "SELECT feature_snapshot_id, dataset_snapshot_id "
            "FROM feature_lineage "
            "WHERE feature_snapshot_id = ?",
            (feature_snapshot_id,),
        ).fetchall()
        return [FeatureLineageRecord(r[0], r[1]) for r in rows]

    # ── ID sequence helpers ───────────────────────────────

    def get_next_sequence(self, prefix: str, date_str: str) -> int:
        """Next sequence for IDs like {prefix}_{date}_{seq}."""
        pattern = f"{prefix}_{date_str}_%"
        row = self._conn.execute(
            _SQL_NEXT_SEQ,
            (f"{prefix}_{date_str}_", pattern, pattern, pattern),
        ).fetchone()
        if row is None or row[0] is None:
            return 1
        return int(row[0]) + 1
