"""Deduplication engine — configurable keep strategy."""

from __future__ import annotations

import logging
from typing import Literal

import polars as pl

logger = logging.getLogger(__name__)

UniqueKeep = Literal["first", "last", "any", "none"]


def deduplicate(
    lf: pl.LazyFrame,
    dedup_keys: list[str] | None,
    strategy: str = "keep_last",
    sort_column: str | None = None,
) -> pl.LazyFrame:
    """Remove duplicate rows based on key columns.

    If dedup_keys is None or empty, return lf unchanged.

    Args:
        lf: Input LazyFrame.
        dedup_keys: Column names to check for duplicates.
        strategy: 'keep_first' or 'keep_last'.
        sort_column: Column to sort by before dedup for deterministic ordering.

    Returns:
        Deduplicated LazyFrame.
    """
    if not dedup_keys:
        return lf

    # Sort before dedup for deterministic keep_first/keep_last semantics
    if sort_column and sort_column in lf.collect_schema().names():
        lf = lf.sort(sort_column)

    strategy_map: dict[str, UniqueKeep] = {
        "keep_last": "last",
        "keep_first": "first",
    }
    keep: UniqueKeep = strategy_map.get(strategy, "last")
    logger.debug("Deduplicating on keys=%s, keep=%s", dedup_keys, keep)
    return lf.unique(subset=dedup_keys, keep=keep, maintain_order=True)
