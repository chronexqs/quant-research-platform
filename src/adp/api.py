"""Public Python API for the Athena Data Platform.

This module contains all user-facing functions for loading, querying, and
exploring datasets and feature sets stored in the ADP lakehouse.  Every
function accepts an optional *registry* or *registry_path* parameter so
callers can point at a non-default metadata database.
"""

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


def _get_registry(registry_path: Path | None = None) -> MetadataRegistry:
    """Return a ``MetadataRegistry`` instance, falling back to the default path.

    Args:
        registry_path: Explicit path to the SQLite registry database.
            When *None*, ``_DEFAULT_METADATA_PATH`` is used.

    Returns:
        A connected :class:`MetadataRegistry`.
    """
    path = registry_path or _DEFAULT_METADATA_PATH
    return MetadataRegistry(path)


def load_dataset(
    name: str,
    snapshot_id: str | None = None,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.LazyFrame:
    """Load a dataset snapshot as a Polars LazyFrame.

    Resolves the snapshot to load in one of two ways:

    1. If *snapshot_id* is provided, that exact snapshot is loaded.
    2. Otherwise the **current** (latest promoted) snapshot of the named
       dataset is used.

    Args:
        name: Registered dataset name (e.g. ``"ohlcv_1d"``).
        snapshot_id: Specific snapshot identifier to load.  When *None*,
            the current snapshot of *name* is used.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A Polars ``LazyFrame`` backed by the snapshot's Parquet files.

    Raises:
        DatasetNotFoundError: If *name* is not registered.
        SnapshotNotFoundError: If the resolved snapshot does not exist.
    """
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
    """Load a feature snapshot as a Polars LazyFrame.

    Resolves the feature snapshot to load in one of two ways:

    1. If *snapshot_id* is provided, that exact feature snapshot is loaded.
    2. Otherwise the **latest** feature snapshot for the given
       *feature_set* / *dataset* pair is used.

    Args:
        dataset: Registered dataset name that the features belong to.
        feature_set: Name of the feature set (e.g. ``"momentum_v1"``).
        snapshot_id: Specific feature-snapshot identifier.  When *None*,
            the latest snapshot is resolved automatically.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A Polars ``LazyFrame`` backed by the feature snapshot's Parquet files.

    Raises:
        FeatureSnapshotNotFoundError: If the explicit *snapshot_id* is missing.
        FeatureSetNotFoundError: If no snapshots exist for the set/dataset pair.
    """
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
    """Run a DuckDB SQL query over a dataset snapshot.

    The snapshot's Parquet files are exposed as a DuckDB view named
    ``dataset`` so that *sql* can reference it directly, e.g.
    ``"SELECT * FROM dataset WHERE close > 100"``.

    Args:
        name: Registered dataset name.
        sql: DuckDB-compatible SQL query.  Must reference the view
            ``dataset``.
        snapshot_id: Optional snapshot to query.  Defaults to the current
            snapshot of *name*.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A materialised Polars ``DataFrame`` with the query results.

    Raises:
        DatasetNotFoundError: If the dataset is not registered or has no
            snapshots.
        SnapshotNotFoundError: If the resolved snapshot does not exist.
    """
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
    """Run a DuckDB SQL query over a feature snapshot.

    The feature snapshot's Parquet files are exposed as a DuckDB view
    named ``features`` so that *sql* can reference it directly, e.g.
    ``"SELECT * FROM features LIMIT 100"``.

    Args:
        dataset: Registered dataset name that the features belong to.
        feature_set: Name of the feature set.
        sql: DuckDB-compatible SQL query referencing the ``features`` view.
        snapshot_id: Optional feature-snapshot identifier.  Defaults to the
            latest snapshot for the *feature_set* / *dataset* pair.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A materialised Polars ``DataFrame`` with the query results.

    Raises:
        FeatureSetNotFoundError: If the feature set / dataset combination
            has no snapshots.
    """
    reg = registry or _get_registry(registry_path)

    if snapshot_id:
        record = reg.get_feature_snapshot(snapshot_id)
    else:
        record = reg.get_latest_feature_snapshot(feature_set, dataset)
    if not record:
        raise FeatureSetNotFoundError(f"Feature set '{feature_set}' not found for '{dataset}'")

    return _run_duckdb_query(record.storage_path, sql, "features")


def _run_duckdb_query(storage_path: str, sql: str, view_name: str) -> pl.DataFrame:
    """Execute a SQL query over Parquet data via an in-memory DuckDB connection.

    Creates a temporary view that exposes all ``*.parquet`` files under
    *storage_path*, executes *sql*, and returns the result as a Polars
    ``DataFrame``.

    Args:
        storage_path: Directory containing the Parquet partition files.
        sql: SQL statement to execute against the view.
        view_name: Name of the DuckDB view exposed to *sql*.  Must be
            either ``"dataset"`` or ``"features"``.

    Returns:
        A materialised Polars ``DataFrame`` with the query results.

    Raises:
        ValueError: If *view_name* is not one of the allowed names.
    """
    if view_name not in ("dataset", "features"):
        raise ValueError(f"Invalid view name: {view_name!r}")
    parquet_path = str(Path(storage_path).resolve() / "*.parquet")
    # Escape single quotes to prevent SQL injection via crafted paths.
    escaped_path = parquet_path.replace("'", "''")
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{escaped_path}')")
        return conn.execute(sql).pl()
    finally:
        conn.close()


def list_datasets(
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """List all registered datasets as a Polars DataFrame.

    Args:
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A Polars ``DataFrame`` with columns ``dataset_name``,
        ``current_snapshot``, ``schema_hash``, and ``created_at``.
        Returns an empty frame with the same schema when no datasets
        are registered.
    """
    reg = registry or _get_registry(registry_path)
    records = reg.list_datasets()
    if not records:
        return pl.DataFrame(
            {
                "dataset_name": [],
                "current_snapshot": [],
                "schema_hash": [],
                "created_at": [],
            }
        )
    return pl.DataFrame(
        [
            {
                "dataset_name": r.dataset_name,
                "current_snapshot": r.current_snapshot,
                "schema_hash": r.schema_hash,
                "created_at": str(r.created_at),
            }
            for r in records
        ]
    )


def list_snapshots(
    dataset: str,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """List all snapshots for a dataset as a Polars DataFrame.

    Args:
        dataset: Registered dataset name whose snapshots to list.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A Polars ``DataFrame`` with columns ``snapshot_id``,
        ``row_count``, ``schema_hash``, and ``created_at``.
        Returns an empty frame with the same schema when no snapshots
        exist.
    """
    reg = registry or _get_registry(registry_path)
    records = reg.list_snapshots(dataset)
    if not records:
        return pl.DataFrame(
            {
                "snapshot_id": [],
                "row_count": [],
                "schema_hash": [],
                "created_at": [],
            }
        )
    return pl.DataFrame(
        [
            {
                "snapshot_id": r.snapshot_id,
                "row_count": r.row_count,
                "schema_hash": r.schema_hash,
                "created_at": str(r.created_at),
            }
            for r in records
        ]
    )


def list_feature_sets(
    dataset: str,
    *,
    registry: MetadataRegistry | None = None,
    registry_path: Path | None = None,
) -> pl.DataFrame:
    """List all feature sets for a dataset as a Polars DataFrame.

    Args:
        dataset: Registered dataset name whose feature sets to list.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A Polars ``DataFrame`` with columns ``feature_name``,
        ``version``, ``definition_hash``, and ``created_at``.
        Returns an empty frame with the same schema when no feature
        sets exist.
    """
    reg = registry or _get_registry(registry_path)
    records = reg.list_feature_definitions(dataset)
    if not records:
        return pl.DataFrame(
            {
                "feature_name": [],
                "version": [],
                "definition_hash": [],
                "created_at": [],
            }
        )
    return pl.DataFrame(
        [
            {
                "feature_name": r.feature_name,
                "version": r.version,
                "definition_hash": r.definition_hash,
                "created_at": str(r.created_at),
            }
            for r in records
        ]
    )


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

    Loads the requested feature set, sorts by *sort_column* (optionally
    within groups), and appends ``fwd_return_{period}`` columns computed
    as simple arithmetic returns from *price_column*.

    Args:
        dataset: Registered dataset name.
        feature_set: Name of the feature set to load.
        forward_return_periods: List of look-ahead periods (in rows) for
            which to compute forward returns.  Defaults to ``[1, 5, 10]``.
        price_column: Column containing the price series used to derive
            forward returns.
        sort_column: Column to sort by before computing forward returns.
        group_column: Column to group by (e.g. ``"symbol"``) for
            multi-asset data.  Prevents forward returns from bleeding
            across group boundaries.
        registry: Pre-existing registry instance to reuse.
        registry_path: Path to the SQLite registry database.

    Returns:
        A Polars ``LazyFrame`` containing all original feature columns
        plus ``fwd_return_{period}`` columns for each requested period.

    Raises:
        ValueError: If *price_column* or *group_column* is not present
            in the feature set schema.
        FeatureSetNotFoundError: If the feature set cannot be resolved.
    """
    if forward_return_periods is None:
        forward_return_periods = [1, 5, 10]

    features_lf = load_features(
        dataset, feature_set, registry=registry, registry_path=registry_path
    )

    schema_names = features_lf.collect_schema().names()

    if price_column not in schema_names:
        raise ValueError(f"Price column '{price_column}' not found in feature set columns")
    if group_column and group_column not in schema_names:
        raise ValueError(f"Group column '{group_column}' not found in feature set columns")

    # Ensure temporal ordering (with group if multi-asset)
    if sort_column in schema_names:
        sort_by = [sort_column]
        if group_column:
            sort_by = [group_column, sort_column]
        features_lf = features_lf.sort(sort_by)

    price = pl.col(price_column)
    for period in forward_return_periods:
        # Shift price forward (negative shift = look-ahead) to get future price.
        shift_expr = price.shift(-period)
        if group_column:
            shift_expr = shift_expr.over(group_column)
        # Guard against division by near-zero prices; emit null instead.
        fwd_return = (
            pl.when(price.abs() > 1e-15)
            .then(shift_expr / price - 1.0)
            .otherwise(pl.lit(None, dtype=pl.Float64))
        )
        features_lf = features_lf.with_columns(fwd_return.alias(f"fwd_return_{period}"))

    return features_lf
