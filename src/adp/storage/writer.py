"""Parquet writer with fixed settings for reproducibility."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import polars as pl

PARQUET_COMPRESSION: Literal["zstd"] = "zstd"
PARQUET_ROW_GROUP_SIZE = 100_000


def write_parquet(
    data: pl.DataFrame | pl.LazyFrame,
    output_dir: Path,
) -> int:
    """Write data to Parquet in output_dir. Returns row count."""
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
    """Write _metadata.json alongside the Parquet files."""
    with open(output_dir / "_metadata.json", "w") as f:
        json.dump(metadata, f, indent=2, default=str)
