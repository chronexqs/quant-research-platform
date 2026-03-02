"""Feature materialiser — orchestrates feature computation from snapshots."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

from adp.exceptions import FeatureError, SnapshotNotFoundError
from adp.features.definitions import FeatureSetDefinition
from adp.features.strategies import STRATEGY_REGISTRY
from adp.metadata.registry import MetadataRegistry
from adp.storage.reader import read_parquet
from adp.storage.snapshot import generate_feature_snapshot_id
from adp.storage.writer import write_metadata_json, write_parquet

logger = logging.getLogger(__name__)


class FeatureMaterialiser:
    """Orchestrates feature computation from normalized dataset snapshots.

    Loads a dataset snapshot, applies each feature strategy sequentially,
    persists the enriched DataFrame to Parquet, registers the feature
    definition and snapshot in the metadata registry, and records lineage
    back to the source dataset snapshot.

    Attributes:
        data_dir: Root data directory containing the ``features/`` sub-tree.
        registry: Metadata registry for recording definitions, snapshots,
            and lineage.
    """

    def __init__(self, data_dir: Path, registry: MetadataRegistry) -> None:
        self.data_dir = data_dir
        self.registry = registry

    def materialise(
        self,
        feature_set_def: FeatureSetDefinition,
        snapshot_id: str | None = None,
    ) -> str:
        """Materialise a feature set and return the feature snapshot ID.

        The method resolves the source dataset snapshot, sorts the data by
        the feature set's sort column (defaulting to ``"timestamp"``),
        applies each feature strategy in order, writes the result to Parquet
        with a metadata sidecar, and registers everything in the metadata
        registry.

        Args:
            feature_set_def: Fully parsed feature set definition containing
                feature strategies and dataset binding.
            snapshot_id: Explicit dataset snapshot ID to compute features
                against.  When ``None`` the dataset's current snapshot is
                used.

        Returns:
            The generated feature snapshot ID string.

        Raises:
            SnapshotNotFoundError: If the resolved or explicit dataset
                snapshot does not exist.
            FeatureError: If a feature references an unknown strategy type
                or the specified sort column is missing from the snapshot
                schema.
        """
        dataset_name = feature_set_def.dataset_name

        # Resolve snapshot
        if snapshot_id:
            snap_record = self.registry.get_snapshot(snapshot_id)
            if snap_record is None:
                raise SnapshotNotFoundError(f"Snapshot '{snapshot_id}' not found")
        else:
            dataset = self.registry.get_dataset(dataset_name)
            if dataset is None or dataset.current_snapshot is None:
                raise SnapshotNotFoundError(f"No current snapshot for dataset '{dataset_name}'")
            snapshot_id = dataset.current_snapshot
            snap_record = self.registry.get_snapshot(snapshot_id)

        if snap_record is None:
            raise SnapshotNotFoundError(f"Snapshot '{snapshot_id}' not found")

        # Load snapshot as LazyFrame and ensure temporal ordering
        lf = read_parquet(Path(snap_record.storage_path))
        sort_col = feature_set_def.sort_column or "timestamp"
        schema_names = lf.collect_schema().names()
        if sort_col in schema_names:
            lf = lf.sort(sort_col)
        elif feature_set_def.sort_column is not None:
            raise FeatureError(f"Specified sort column '{sort_col}' not found in snapshot schema")

        # Apply all feature strategies sequentially
        for feat in feature_set_def.features:
            strategy = STRATEGY_REGISTRY.get(feat.type)
            if strategy is None:
                raise FeatureError(f"Unknown feature type: '{feat.type}'")
            logger.debug("Computing feature: %s (type=%s)", feat.name, feat.type)
            lf = strategy.compute(lf, feat.name, feat.params)

        # Single collect
        df = lf.collect()
        row_count = len(df)

        # Generate ID and paths
        fsnap_id = generate_feature_snapshot_id(dataset_name, feature_set_def.name, self.registry)
        storage_dir = self.data_dir / "features" / dataset_name / feature_set_def.name / fsnap_id
        storage_dir.mkdir(parents=True, exist_ok=True)

        # Write Parquet
        write_parquet(df, storage_dir)

        # Write metadata JSON
        feature_columns = [f.name for f in feature_set_def.features]
        write_metadata_json(
            storage_dir,
            {
                "feature_snapshot_id": fsnap_id,
                "feature_set": feature_set_def.name,
                "dataset_name": dataset_name,
                "dataset_snapshot_id": snapshot_id,
                "feature_version": feature_set_def.version,
                "definition_hash": feature_set_def.definition_hash,
                "feature_columns": feature_columns,
                "row_count": row_count,
                "created_at": datetime.now(UTC).isoformat(),
            },
        )

        # Register feature definition (idempotent)
        self.registry.register_feature_definition(
            feature_name=feature_set_def.name,
            dataset_name=dataset_name,
            version=feature_set_def.version,
            definition_hash=feature_set_def.definition_hash,
        )

        # Register feature snapshot
        self.registry.create_feature_snapshot(
            feature_snapshot_id=fsnap_id,
            feature_name=feature_set_def.name,
            dataset_name=dataset_name,
            feature_version=feature_set_def.version,
            definition_hash=feature_set_def.definition_hash,
            row_count=row_count,
            storage_path=str(storage_dir),
        )

        # Link lineage
        self.registry.link_feature_lineage(fsnap_id, snapshot_id)

        logger.info(
            "Materialised feature set '%s' for %s (%d rows, %d features) -> %s",
            feature_set_def.name,
            dataset_name,
            row_count,
            len(feature_columns),
            fsnap_id,
        )
        return fsnap_id
