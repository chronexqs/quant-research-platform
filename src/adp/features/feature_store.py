"""Feature store — loading feature snapshots as LazyFrames."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from adp.exceptions import FeatureSetNotFoundError, FeatureSnapshotNotFoundError
from adp.metadata.registry import MetadataRegistry
from adp.storage.reader import read_parquet


def load_feature_snapshot(
    dataset_name: str,
    feature_set_name: str,
    snapshot_id: str | None = None,
    *,
    data_dir: Path | None = None,
    registry: MetadataRegistry | None = None,
) -> pl.LazyFrame:
    """Load a materialised feature snapshot as a Polars LazyFrame.

    When *snapshot_id* is provided the exact snapshot is loaded; otherwise
    the most recent snapshot for the given feature set and dataset is
    resolved automatically via the metadata registry.

    Args:
        dataset_name: Name of the source dataset.
        feature_set_name: Logical name of the feature set.
        snapshot_id: Explicit feature snapshot ID.  When ``None`` the latest
            available snapshot is used.
        data_dir: Unused in the current implementation but reserved for
            future direct-path resolution without a registry.
        registry: Metadata registry used to look up snapshot records.

    Returns:
        A Polars ``LazyFrame`` scanning the feature snapshot Parquet data.

    Raises:
        FeatureSetNotFoundError: If *registry* is ``None`` or no snapshots
            exist for the requested feature set.
        FeatureSnapshotNotFoundError: If the explicit *snapshot_id* does not
            exist in the registry.
    """
    if registry is None:
        raise FeatureSetNotFoundError("MetadataRegistry is required")

    if snapshot_id:
        record = registry.get_feature_snapshot(snapshot_id)
        if record is None:
            raise FeatureSnapshotNotFoundError(f"Feature snapshot '{snapshot_id}' not found")
    else:
        record = registry.get_latest_feature_snapshot(feature_set_name, dataset_name)
        if record is None:
            raise FeatureSetNotFoundError(
                f"No feature snapshots for '{feature_set_name}' on dataset '{dataset_name}'"
            )

    return read_parquet(Path(record.storage_path))
