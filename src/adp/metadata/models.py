"""Metadata record models — frozen dataclasses for all 7 registry tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DatasetRecord:
    """Immutable record representing a registered dataset in the ADP metadata store.

    Each dataset has a unique name, an optional pointer to the most recent
    normalized snapshot, and a schema hash for drift detection.
    """

    dataset_name: str
    description: str
    current_snapshot: str | None
    schema_hash: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class IngestionRecord:
    """Immutable record representing a single raw data ingestion event.

    Captures the provenance of raw data including its source type (e.g. CSV,
    API), location, and the number of rows ingested.
    """

    ingestion_id: str
    dataset_name: str
    source_type: str
    source_location: str
    row_count: int
    ingestion_timestamp: datetime


@dataclass(frozen=True)
class SnapshotRecord:
    """Immutable record representing a normalized, versioned dataset snapshot.

    A snapshot is the product of applying a normalization pipeline to one or
    more raw ingestions. It records the schema hash and normalization version
    used, enabling reproducibility.
    """

    snapshot_id: str
    dataset_name: str
    schema_hash: str
    normalization_version: str
    row_count: int
    storage_path: str
    created_at: datetime


@dataclass(frozen=True)
class SnapshotLineageRecord:
    """Immutable join record linking a snapshot to one of its source ingestions.

    Multiple lineage records per snapshot support snapshots built from several
    raw ingestion batches.
    """

    snapshot_id: str
    ingestion_id: str


@dataclass(frozen=True)
class FeatureDefinitionRecord:
    """Immutable record representing a versioned feature set definition.

    Stores the definition hash for change detection and optionally the raw
    YAML definition text for auditability.
    """

    feature_name: str
    dataset_name: str
    version: int
    definition_hash: str
    definition_yaml: str | None
    created_at: datetime


@dataclass(frozen=True)
class FeatureSnapshotRecord:
    """Immutable record representing a materialised feature snapshot.

    Produced by computing a feature set definition against a dataset snapshot.
    Includes the definition hash and version for reproducibility tracking.
    """

    feature_snapshot_id: str
    feature_name: str
    dataset_name: str
    feature_version: int
    definition_hash: str
    row_count: int
    storage_path: str
    created_at: datetime


@dataclass(frozen=True)
class FeatureLineageRecord:
    """Immutable join record linking a feature snapshot to the dataset snapshot it was derived from.

    Enables tracing any feature value back to its source data snapshot.
    """

    feature_snapshot_id: str
    dataset_snapshot_id: str
