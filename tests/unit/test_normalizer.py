"""Unit tests for adp.processing.normalizer — timezone normalization and pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from adp.config import ColumnDef, ColumnType, ProcessingConfig
from adp.processing.normalizer import NormalizationPipeline, normalize_timezones


# ── Helpers ──────────────────────────────────────────────────

def _ts_col(name: str = "timestamp", tz: str = "UTC") -> ColumnDef:
    return ColumnDef(name=name, type=ColumnType.datetime_, nullable=False, source_timezone=tz)


# ── normalize_timezones ──────────────────────────────────────

@pytest.mark.unit
class TestNormalizeTimezones:
    def test_utc_passthrough(self) -> None:
        """UTC-sourced column keeps its values; tzinfo is set to UTC."""
        ts = [datetime(2026, 1, 1, 12, 0), datetime(2026, 1, 1, 13, 0)]
        lf = pl.LazyFrame({"timestamp": ts})
        cols = [_ts_col("timestamp", "UTC")]
        result = normalize_timezones(lf, cols).collect()
        assert result["timestamp"].dtype == pl.Datetime("us", "UTC")
        assert result["timestamp"][0].hour == 12

    def test_eastern_to_utc(self) -> None:
        """US/Eastern -> UTC adds 5 hours in January (EST offset)."""
        ts = [datetime(2026, 1, 15, 10, 0)]
        lf = pl.LazyFrame({"timestamp": ts})
        cols = [_ts_col("timestamp", "US/Eastern")]
        result = normalize_timezones(lf, cols).collect()
        assert result["timestamp"].dtype == pl.Datetime("us", "UTC")
        # 10:00 EST = 15:00 UTC
        assert result["timestamp"][0].hour == 15

    def test_non_datetime_columns_unchanged(self) -> None:
        """Non-datetime columns are not touched by normalize_timezones."""
        lf = pl.LazyFrame({"price": [1.0, 2.0], "symbol": ["A", "B"]})
        cols = [
            ColumnDef(name="price", type=ColumnType.float_, nullable=False),
            ColumnDef(name="symbol", type=ColumnType.str_, nullable=False),
        ]
        result = normalize_timezones(lf, cols).collect()
        assert result["price"].to_list() == [1.0, 2.0]
        assert result["symbol"].to_list() == ["A", "B"]


# ── NormalizationPipeline ────────────────────────────────────

@pytest.mark.unit
class TestNormalizationPipeline:
    def test_pipeline_chains_steps(self) -> None:
        """Pipeline applies steps in order."""
        calls: list[str] = []

        def step_a(lf: pl.LazyFrame) -> pl.LazyFrame:
            calls.append("a")
            return lf

        def step_b(lf: pl.LazyFrame) -> pl.LazyFrame:
            calls.append("b")
            return lf

        pipeline = NormalizationPipeline(steps=[step_a, step_b])
        lf = pl.LazyFrame({"x": [1]})
        pipeline.run(lf)
        assert calls == ["a", "b"]

    def test_pipeline_returns_lazyframe(self) -> None:
        pipeline = NormalizationPipeline(steps=[])
        lf = pl.LazyFrame({"x": [1, 2, 3]})
        result = pipeline.run(lf)
        assert isinstance(result, pl.LazyFrame)

    def test_from_config_builds_pipeline(self) -> None:
        """from_config creates a pipeline with 3 steps (validate, tz, dedup)."""
        cols = [
            ColumnDef(name="price", type=ColumnType.float_, nullable=False),
            ColumnDef(name="timestamp", type=ColumnType.datetime_, nullable=False, source_timezone="UTC"),
        ]
        processing = ProcessingConfig(
            dedup_keys=["price"],
            normalization_version="1.0",
        )
        pipeline = NormalizationPipeline.from_config(cols, processing)
        assert len(pipeline.steps) == 3

    def test_pipeline_dedup_integration(
        self, sample_trades_with_duplicates: pl.DataFrame
    ) -> None:
        """Full pipeline deduplicates using configured keys."""
        # Cast timestamp to string to simulate raw CSV input (cast_dataframe
        # uses str.to_datetime, so it expects string input).
        df_str = sample_trades_with_duplicates.with_columns(
            pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S").alias("timestamp")
        )
        cols = [
            ColumnDef(name="trade_id", type=ColumnType.str_, nullable=False),
            ColumnDef(name="symbol", type=ColumnType.str_, nullable=False),
            ColumnDef(name="price", type=ColumnType.float_, nullable=False),
            ColumnDef(name="quantity", type=ColumnType.float_, nullable=False),
            ColumnDef(name="side", type=ColumnType.str_, nullable=False),
            ColumnDef(name="timestamp", type=ColumnType.datetime_, nullable=False, source_timezone="UTC"),
        ]
        processing = ProcessingConfig(
            dedup_keys=["trade_id"],
            dedup_strategy="keep_last",
            normalization_version="1.0",
        )
        pipeline = NormalizationPipeline.from_config(cols, processing)
        result = pipeline.run(df_str.lazy()).collect()
        # 105 rows with 5 duplicates on trade_id -> 100 unique rows
        assert len(result) == 100
