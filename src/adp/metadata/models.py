"""Metadata record models — frozen dataclasses for all 7 registry tables."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class DatasetRecord:
    dataset_name: str
    description: str
    current_snapshot: str | None
    schema_hash: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class IngestionRecord:
    ingestion_id: str
    dataset_name: str
    source_type: str
    source_location: str
    row_count: int
    ingestion_timestamp: datetime


@dataclass(frozen=True)
class SnapshotRecord:
    snapshot_id: str
    dataset_name: str
    schema_hash: str
    normalization_version: str
    row_count: int
    storage_path: str
    created_at: datetime


@dataclass(frozen=True)
class SnapshotLineageRecord:
    snapshot_id: str
    ingestion_id: str


@dataclass(frozen=True)
class FeatureDefinitionRecord:
    feature_name: str
    dataset_name: str
    version: int
    definition_hash: str
    definition_yaml: str | None
    created_at: datetime


@dataclass(frozen=True)
class FeatureSnapshotRecord:
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
    feature_snapshot_id: str
    dataset_snapshot_id: str
