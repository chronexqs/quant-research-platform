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
    """Return today's UTC date as a compact ``YYYYMMDD`` string for use in IDs."""
    return datetime.now(UTC).strftime("%Y%m%d")


def generate_ingestion_id(dataset_name: str, registry: MetadataRegistry) -> str:
    """Generate a unique ingestion ID in the format ``{dataset}_ing_{date}_{seq}``.

    Args:
        dataset_name: Name of the dataset being ingested.
        registry: Metadata registry used to determine the next sequence number.

    Returns:
        A new, collision-free ingestion identifier string.
    """
    date_str = _today_str()
    prefix = f"{dataset_name}_ing"
    seq = registry.get_next_sequence(prefix, date_str)
    return f"{prefix}_{date_str}_{seq:03d}"


def generate_snapshot_id(dataset_name: str, registry: MetadataRegistry) -> str:
    """Generate a unique snapshot ID in the format ``{dataset}_snap_{date}_{seq}``.

    Args:
        dataset_name: Name of the dataset being snapshotted.
        registry: Metadata registry used to determine the next sequence number.

    Returns:
        A new, collision-free snapshot identifier string.
    """
    date_str = _today_str()
    prefix = f"{dataset_name}_snap"
    seq = registry.get_next_sequence(prefix, date_str)
    return f"{prefix}_{date_str}_{seq:03d}"


def generate_feature_snapshot_id(
    dataset_name: str,
    feature_set_name: str,
    registry: MetadataRegistry,
) -> str:
    """Generate a unique feature snapshot ID.

    The format is ``{dataset}_{feature_set}_fsnap_{date}_{seq}``.

    Args:
        dataset_name: Name of the source dataset.
        feature_set_name: Logical name of the feature set.
        registry: Metadata registry used to determine the next sequence number.

    Returns:
        A new, collision-free feature snapshot identifier string.
    """
    date_str = _today_str()
    prefix = f"{dataset_name}_{feature_set_name}_fsnap"
    seq = registry.get_next_sequence(prefix, date_str)
    return f"{prefix}_{date_str}_{seq:03d}"


class SnapshotEngine:
    """Creates immutable, normalized snapshots from raw ingested data.

    The engine loads one or more raw ingestion Parquet files, concatenates
    them, runs the dataset's normalization pipeline, persists the output to
    Parquet with a sidecar ``_metadata.json``, and registers the snapshot
    (including lineage links) in the metadata registry.

    Attributes:
        data_dir: Root data directory containing ``raw/`` and ``normalized/``
            sub-trees.
        registry: Metadata registry for recording snapshots and lineage.
    """

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

        The method performs the following steps:

        1. Load and concatenate raw Parquet data for each ingestion ID.
        2. Apply the normalization pipeline defined in *dataset_config*.
        3. Write the resulting DataFrame to Parquet with a metadata sidecar.
        4. Register the snapshot and its lineage in the metadata registry.
        5. Update the dataset's ``current_snapshot`` pointer.

        Args:
            dataset_name: Name of the target dataset.
            dataset_config: Dataset configuration containing schema definitions
                and processing/normalization settings.
            ingestion_ids: One or more ingestion IDs whose raw data will be
                combined into the snapshot.

        Returns:
            The generated snapshot ID string.

        Raises:
            SnapshotError: If *ingestion_ids* is empty or any referenced
                ingestion record does not exist.
        """
        if not ingestion_ids:
            raise SnapshotError("No ingestion IDs provided")

        # Load raw data from all ingestions
        frames: list[pl.LazyFrame] = []
        for ing_id in ingestion_ids:
            record = self.registry.get_ingestion(ing_id)
            if record is None:
                raise SnapshotError(f"Ingestion '{ing_id}' not found")
            # Always use the canonical raw parquet path
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
        write_metadata_json(
            storage_dir,
            {
                "snapshot_id": snapshot_id,
                "dataset_name": dataset_name,
                "schema_hash": schema_hash,
                "normalization_version": processing.normalization_version,
                "row_count": row_count,
                "column_names": df.columns,
                "created_at": datetime.now(UTC).isoformat(),
            },
        )

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
            snapshot_id,
            dataset_name,
            row_count,
        )
        return snapshot_id
