"""Public Python API for the Athena Data Platform."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from adp.exceptions import (
    DatasetNotFoundError,
    FeatureSetNotFoundError,
    FeatureSnapshotNotFoundError,
    SnapshotNotFoundError,
)
from adp.metadata.registry import MetadataRegistry
from adp.storage.reader import read_parquet

_DEFAULT_METADATA_PATH = Path("metadata/adp_registry.db")
_DEFAULT_DATA_DIR = Path("data")


def _get_registry(registry_path: Path | None = None) -> MetadataRegistry:
    path = registry_path or _DEFAULT_METADATA_PATH
    return MetadataRegistry(path)


def load_dataset(
    name: str,
    snapshot_id: str | None = None,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.LazyFrame:
    """Load a dataset snapshot as a Polars LazyFrame."""
    reg = registry or _get_registry(registry_path)

    if snapshot_id:
        record = reg.get_snapshot(snapshot_id)
        if not record:
            raise SnapshotNotFoundError(f"Snapshot '{snapshot_id}' not found")
    else:
        dataset = reg.get_dataset(name)
        if not dataset:
            raise DatasetNotFoundError(f"Dataset '{name}' not registered")
        if not dataset.current_snapshot:
            raise SnapshotNotFoundError(f"No snapshots exist for dataset '{name}'")
        record = reg.get_snapshot(dataset.current_snapshot)
        if not record:
            raise SnapshotNotFoundError(f"Snapshot '{dataset.current_snapshot}' not found")

    return read_parquet(Path(record.storage_path))


def load_features(
    dataset: str,
    feature_set: str,
    snapshot_id: str | None = None,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.LazyFrame:
    """Load a feature snapshot as a Polars LazyFrame."""
    reg = registry or _get_registry(registry_path)

    if snapshot_id:
        record = reg.get_feature_snapshot(snapshot_id)
        if not record:
            raise FeatureSnapshotNotFoundError(f"Feature snapshot '{snapshot_id}' not found")
    else:
        record = reg.get_latest_feature_snapshot(feature_set, dataset)
        if not record:
            raise FeatureSetNotFoundError(
                f"No feature snapshots for '{feature_set}' on dataset '{dataset}'"
            )

    return read_parquet(Path(record.storage_path))


def query_dataset(
    name: str,
    sql: str,
    snapshot_id: str | None = None,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """Run a DuckDB SQL query over a dataset snapshot."""
    reg = registry or _get_registry(registry_path)

    if snapshot_id:
        record = reg.get_snapshot(snapshot_id)
    else:
        dataset_rec = reg.get_dataset(name)
        if not dataset_rec or not dataset_rec.current_snapshot:
            raise DatasetNotFoundError(f"Dataset '{name}' not found or no snapshots")
        record = reg.get_snapshot(dataset_rec.current_snapshot)
    if not record:
        raise SnapshotNotFoundError("Snapshot not found")

    return _run_duckdb_query(record.storage_path, sql, "dataset")


def query_features(
    dataset: str,
    feature_set: str,
    sql: str,
    snapshot_id: str | None = None,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """Run a DuckDB SQL query over a feature snapshot."""
    reg = registry or _get_registry(registry_path)

    if snapshot_id:
        record = reg.get_feature_snapshot(snapshot_id)
    else:
        record = reg.get_latest_feature_snapshot(feature_set, dataset)
    if not record:
        raise FeatureSetNotFoundError(
            f"Feature set '{feature_set}' not found for '{dataset}'"
        )

    return _run_duckdb_query(record.storage_path, sql, "features")


def _run_duckdb_query(
    storage_path: str, sql: str, view_name: str
) -> pl.DataFrame:
    """Execute a SQL query over Parquet data via DuckDB (read-only)."""
    parquet_path = str(Path(storage_path).resolve() / "*.parquet")
    conn = duckdb.connect()
    try:
        conn.execute("SET access_mode = 'read_only'")
        conn.execute(
            f"CREATE VIEW {view_name} AS "
            f"SELECT * FROM read_parquet('{parquet_path}')"
        )
        return conn.execute(sql).pl()
    finally:
        conn.close()


def list_datasets(
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """List all registered datasets."""
    reg = registry or _get_registry(registry_path)
    records = reg.list_datasets()
    if not records:
        return pl.DataFrame({"dataset_name": [], "current_snapshot": [], "schema_hash": []})
    return pl.DataFrame([
        {
            "dataset_name": r.dataset_name,
            "current_snapshot": r.current_snapshot,
            "schema_hash": r.schema_hash,
            "created_at": str(r.created_at),
        }
        for r in records
    ])


def list_snapshots(
    dataset: str,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """List all snapshots for a dataset."""
    reg = registry or _get_registry(registry_path)
    records = reg.list_snapshots(dataset)
    if not records:
        return pl.DataFrame({"snapshot_id": [], "row_count": [], "created_at": []})
    return pl.DataFrame([
        {
            "snapshot_id": r.snapshot_id,
            "row_count": r.row_count,
            "schema_hash": r.schema_hash,
            "created_at": str(r.created_at),
        }
        for r in records
    ])


def list_feature_sets(
    dataset: str,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """List all feature sets for a dataset."""
    reg = registry or _get_registry(registry_path)
    records = reg.list_feature_definitions(dataset)
    if not records:
        return pl.DataFrame({"feature_name": [], "version": [], "definition_hash": []})
    return pl.DataFrame([
        {
            "feature_name": r.feature_name,
            "version": r.version,
            "definition_hash": r.definition_hash,
            "created_at": str(r.created_at),
        }
        for r in records
    ])


def build_backtest_matrix(
    dataset: str,
    feature_set: str,
    forward_return_periods: list[int] | None = None,
    price_column: str = "close",
    sort_column: str = "timestamp",
    group_column: str | None = None,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.LazyFrame:
    """Build a feature matrix with forward returns for backtesting.

    Args:
        sort_column: Column to sort by before computing forward returns.
        group_column: Column to group by (e.g. "symbol") for multi-asset data.
            Prevents forward returns from bleeding across symbol boundaries.
    """
    if forward_return_periods is None:
        forward_return_periods = [1, 5, 10]

    features_lf = load_features(
        dataset, feature_set, registry=registry, registry_path=registry_path
    )

    # Ensure temporal ordering
    if sort_column in features_lf.collect_schema().names():
        features_lf = features_lf.sort(sort_column)

    for period in forward_return_periods:
        shift_expr = pl.col(price_column).shift(-period)
        if group_column:
            shift_expr = shift_expr.over(group_column)
        fwd_return = shift_expr / pl.col(price_column) - 1.0
        features_lf = features_lf.with_columns(
            fwd_return.alias(f"fwd_return_{period}")
        )

    return features_lf
