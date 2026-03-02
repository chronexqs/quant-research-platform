"""Parquet reader returning LazyFrames."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def read_parquet(path: Path | str) -> pl.LazyFrame:
    """Read Parquet data from a directory or single file as a LazyFrame.

    When *path* points to a directory the function scans all ``*.parquet``
    files within it (useful for partitioned outputs from
    :func:`adp.storage.writer.write_parquet`).  When it points to a single
    file the file is scanned directly.

    Args:
        path: Path to a Parquet file or a directory containing Parquet files.

    Returns:
        A Polars ``LazyFrame`` backed by the scanned Parquet data.

    Raises:
        FileNotFoundError: If *path* is a directory that contains no
            ``.parquet`` files.
    """
    path = Path(path)
    if path.is_dir():
        parquet_files = sorted(path.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No Parquet files in {path}")
        return pl.scan_parquet(path / "*.parquet")
    else:
        return pl.scan_parquet(path)
