"""Parquet writer with fixed settings for reproducibility."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import polars as pl

# Zstandard offers the best compression-ratio-to-speed trade-off for
# analytical Parquet workloads.
PARQUET_COMPRESSION: Literal["zstd"] = "zstd"

# 100 000 rows per row-group balances predicate push-down granularity with
# metadata overhead.
PARQUET_ROW_GROUP_SIZE = 100_000


def write_parquet(
    data: pl.DataFrame | pl.LazyFrame,
    output_dir: Path,
) -> int:
    """Write a DataFrame or LazyFrame to a single Parquet file inside *output_dir*.

    The output file is always named ``part-0.parquet``.  If *data* is a
    ``LazyFrame`` it is collected before writing.  The directory is created
    recursively if it does not exist.

    Args:
        data: The data to persist.  LazyFrames are collected automatically.
        output_dir: Target directory.  Created with ``parents=True`` if needed.

    Returns:
        The number of rows written.
    """
    df = data.collect() if isinstance(data, pl.LazyFrame) else data

    output_dir.mkdir(parents=True, exist_ok=True)

    df.write_parquet(
        output_dir / "part-0.parquet",
        compression=PARQUET_COMPRESSION,
        row_group_size=PARQUET_ROW_GROUP_SIZE,
        use_pyarrow=True,
    )

    return len(df)


def write_metadata_json(output_dir: Path, metadata: dict[str, Any]) -> None:
    """Write a ``_metadata.json`` sidecar file alongside the Parquet data.

    The sidecar file stores human-readable provenance information (snapshot
    IDs, row counts, schema hashes, etc.) that supplements the Parquet
    footer metadata.

    Args:
        output_dir: Directory where the JSON file will be written.
        metadata: Arbitrary key-value metadata to serialize.  Values that are
            not natively JSON-serializable are converted via ``str()``.
    """
    with open(output_dir / "_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)
