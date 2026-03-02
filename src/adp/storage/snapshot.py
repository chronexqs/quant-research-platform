"""Snapshot ID generation and SnapshotEngine for creating immutable snapshots."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from adp.config import DatasetConfig
from adp.exceptions import SnapshotError
from adp.metadata.registry import MetadataRegistry
from adp.processing.normalizer import NormalizationPipeline
from adp.processing.schema import compute_schema_hash_from_defs
from adp.storage.reader import read_parquet
from adp.storage.writer import write_metadata_json, write_parquet

logger = logging.getLogger(__name__)


def _today_str() -> str:
    return datetime.now(UTC).strftime("%Y%m%d")


def generate_ingestion_id(dataset_name: str, registry: MetadataRegistry) -> str:
    date_str = _today_str()
    prefix = f"{dataset_name}_ing"
    seq = registry.get_next_sequence(prefix, date_str)
    return f"{prefix}_{date_str}_{seq:03d}"


def generate_snapshot_id(dataset_name: str, registry: MetadataRegistry) -> str:
    date_str = _today_str()
    prefix = f"{dataset_name}_snap"
    seq = registry.get_next_sequence(prefix, date_str)
    return f"{prefix}_{date_str}_{seq:03d}"


def generate_feature_snapshot_id(
    dataset_name: str,
    feature_set_name: str,
    registry: MetadataRegistry,
) -> str:
    date_str = _today_str()
    prefix = f"{dataset_name}_{feature_set_name}_fsnap"
    seq = registry.get_next_sequence(prefix, date_str)
    return f"{prefix}_{date_str}_{seq:03d}"


class SnapshotEngine:
    """Creates immutable normalized snapshots from raw ingested data."""

    def __init__(self, data_dir: Path, registry: MetadataRegistry) -> None:
        self.data_dir = data_dir
        self.registry = registry

    def create_snapshot(
        self,
        dataset_name: str,
        dataset_config: DatasetConfig,
        ingestion_ids: list[str],
    ) -> str:
        """Create a normalized snapshot from one or more raw ingestions.

        Returns the snapshot_id.
        """
        if not ingestion_ids:
            raise SnapshotError("No ingestion IDs provided")

        # Load raw data from all ingestions
        frames: list[pl.LazyFrame] = []
        for ing_id in ingestion_ids:
            record = self.registry.get_ingestion(ing_id)
            if record is None:
                raise SnapshotError(f"Ingestion '{ing_id}' not found")
            raw_path = Path(record.source_location)
            if not raw_path.exists():
                # Try relative to data_dir/raw
                raw_path = self.data_dir / "raw" / dataset_name / f"{ing_id}.parquet"
            frames.append(read_parquet(raw_path))

        # Concatenate if multiple
        lf = frames[0] if len(frames) == 1 else pl.concat(frames)

        # Run normalization pipeline
        columns = dataset_config.schema_def.columns
        processing = dataset_config.processing
        pipeline = NormalizationPipeline.from_config(columns, processing)
        lf = pipeline.run(lf)

        # Collect
        df = lf.collect()
        row_count = len(df)

        # Generate IDs and paths
        snapshot_id = generate_snapshot_id(dataset_name, self.registry)
        schema_hash = compute_schema_hash_from_defs(columns)
        storage_dir = self.data_dir / "normalized" / dataset_name / snapshot_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        # Write Parquet
        write_parquet(df, storage_dir)

        # Write metadata JSON
        write_metadata_json(storage_dir, {
            "snapshot_id": snapshot_id,
            "dataset_name": dataset_name,
            "schema_hash": schema_hash,
            "normalization_version": processing.normalization_version,
            "row_count": row_count,
            "column_names": df.columns,
            "created_at": datetime.now(UTC).isoformat(),
        })

        # Register in metadata
        self.registry.create_snapshot(
            snapshot_id=snapshot_id,
            dataset_name=dataset_name,
            schema_hash=schema_hash,
            normalization_version=processing.normalization_version,
            row_count=row_count,
            storage_path=str(storage_dir),
        )

        # Link lineage
        for ing_id in ingestion_ids:
            self.registry.link_snapshot_lineage(snapshot_id, ing_id)

        # Update current snapshot
        self.registry.update_current_snapshot(dataset_name, snapshot_id)

        logger.info(
            "Created snapshot %s for %s (%d rows)",
            snapshot_id, dataset_name, row_count,
        )
        return snapshot_id
