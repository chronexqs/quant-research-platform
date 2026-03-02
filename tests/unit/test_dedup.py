"""Unit tests for adp.processing.dedup — deduplicate function."""

from __future__ import annotations

import polars as pl
import pytest

from adp.processing.dedup import deduplicate


@pytest.mark.unit
class TestDeduplicate:
    def test_removes_duplicates(self) -> None:
        lf = pl.LazyFrame({
            "id": ["a", "b", "a", "c"],
            "value": [1, 2, 3, 4],
        })
        result = deduplicate(lf, dedup_keys=["id"], strategy="keep_last").collect()
        assert len(result) == 3
        ids = set(result["id"].to_list())
        assert ids == {"a", "b", "c"}

    def test_keep_last(self) -> None:
        """With keep_last, the later occurrence wins."""
        lf = pl.LazyFrame({
            "id": ["a", "a"],
            "value": [1, 2],
        })
        result = deduplicate(lf, dedup_keys=["id"], strategy="keep_last").collect()
        assert len(result) == 1
        assert result["value"][0] == 2

    def test_keep_first(self) -> None:
        """With keep_first, the earlier occurrence wins."""
        lf = pl.LazyFrame({
            "id": ["a", "a"],
            "value": [1, 2],
        })
        result = deduplicate(lf, dedup_keys=["id"], strategy="keep_first").collect()
        assert len(result) == 1
        assert result["value"][0] == 1

    def test_no_keys_passthrough(self) -> None:
        """When dedup_keys is None, the frame is returned unchanged."""
        lf = pl.LazyFrame({"id": ["a", "a"], "value": [1, 2]})
        result = deduplicate(lf, dedup_keys=None).collect()
        assert len(result) == 2

    def test_empty_keys_passthrough(self) -> None:
        """When dedup_keys is an empty list, the frame is returned unchanged."""
        lf = pl.LazyFrame({"id": ["a", "a"], "value": [1, 2]})
        result = deduplicate(lf, dedup_keys=[]).collect()
        assert len(result) == 2

    def test_returns_lazyframe(self) -> None:
        lf = pl.LazyFrame({"id": [1, 2, 3]})
        result = deduplicate(lf, dedup_keys=["id"])
        assert isinstance(result, pl.LazyFrame)

    def test_multi_column_key(self) -> None:
        lf = pl.LazyFrame({
            "sym": ["BTC", "BTC", "ETH", "BTC"],
            "ts":  [1,     2,     1,     1],
            "val": [10,    20,    30,    40],
        })
        result = deduplicate(lf, dedup_keys=["sym", "ts"], strategy="keep_last").collect()
        assert len(result) == 3
