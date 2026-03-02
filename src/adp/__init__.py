"""Athena Data Platform -- local lakehouse and research factor platform.

This package exposes a high-level Python API for dataset and feature
management.  The public surface re-exported here covers the most common
workflows:

* **Loading data** -- :func:`load_dataset` and :func:`load_features` return
  Polars ``LazyFrame`` objects backed by Parquet snapshots.
* **SQL queries** -- :func:`query_dataset` and :func:`query_features` run
  ad-hoc DuckDB SQL over snapshots.
* **Discovery** -- :func:`list_datasets`, :func:`list_snapshots`, and
  :func:`list_feature_sets` enumerate registered metadata.
* **Backtesting** -- :func:`build_backtest_matrix` joins features with
  forward-return columns for factor research.

All functions are importable directly from ``adp``::

    import adp
    lf = adp.load_dataset("ohlcv_1d")
"""

from adp.api import (
    build_backtest_matrix,
    list_datasets,
    list_feature_sets,
    list_snapshots,
    load_dataset,
    load_features,
    query_dataset,
    query_features,
)

__version__ = "0.1.0"

# Public API surface -- consumed by ``from adp import *`` and documentation.
__all__ = [
    "build_backtest_matrix",
    "list_datasets",
    "list_feature_sets",
    "list_snapshots",
    "load_dataset",
    "load_features",
    "query_dataset",
    "query_features",
]
