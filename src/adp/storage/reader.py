"""Parquet reader returning LazyFrames."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def read_parquet(path: Path | str) -> pl.LazyFrame:
    """Read Parquet from a directory or single file as LazyFrame.

    If path is a directory, scan all .parquet files within it.
    """
    path = Path(path)
    if path.is_dir():
        parquet_files = sorted(path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No Parquet files in {path}")
        return pl.scan_parquet(path / "*.parquet")
    else:
        return pl.scan_parquet(path)
