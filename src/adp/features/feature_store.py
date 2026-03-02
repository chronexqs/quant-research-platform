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
    """Load a feature snapshot as a Polars LazyFrame.

    If snapshot_id is None, loads the latest for this feature set.
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
