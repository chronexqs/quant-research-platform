"""Shared test fixtures for ADP tests."""

from __future__ import annotations

import textwrap
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from adp.metadata.registry import MetadataRegistry


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary ADP directory structure."""
    for subdir in ("raw", "staged", "normalized", "features"):
        (tmp_path / "data" / subdir).mkdir(parents=True)
    (tmp_path / "metadata").mkdir()
    (tmp_path / "config").mkdir()
    return tmp_path


@pytest.fixture
def in_memory_registry() -> MetadataRegistry:
    """In-memory SQLite metadata registry for testing."""
    return MetadataRegistry(":memory:")


@pytest.fixture
def sample_trades_df() -> pl.DataFrame:
    """100-row deterministic trade DataFrame."""
    n = 100
    base_time = datetime(2026, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
    return pl.DataFrame({
        "trade_id": [f"T_{i:04d}" for i in range(n)],
        "symbol": ["BTCUSDT" if i % 2 == 0 else "ETHUSDT" for i in range(n)],
        "price": [42000.0 + i * 10.0 + (i * 7 % 13) for i in range(n)],
        "quantity": [0.1 + (i * 3 % 10) / 10.0 for i in range(n)],
        "side": ["buy" if i % 3 != 0 else "sell" for i in range(n)],
        "timestamp": [
            base_time.replace(second=i % 60, minute=i // 60)
            for i in range(n)
        ],
    })


@pytest.fixture
def sample_trades_with_duplicates(sample_trades_df: pl.DataFrame) -> pl.DataFrame:
    """105-row DataFrame with 5 duplicates."""
    dupes = sample_trades_df.head(5)
    return pl.concat([sample_trades_df, dupes])


@pytest.fixture
def sample_ohlcv_df() -> pl.DataFrame:
    """30-row deterministic OHLCV DataFrame."""
    import math

    n = 30
    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_price = 42000.0

    rows = []
    for i in range(n):
        mid = base_price + 100.0 * math.sin(i * 2 * math.pi / 10) + i * 5
        o = round(mid + (i % 5) * 0.5 - 1.25, 2)
        h = round(max(mid + 10, o) + 1.0, 2)
        lo = round(min(mid - 10, o) - 1.0, 2)
        c = round(mid + (i % 7) * 0.3 - 1.05, 2)
        h = round(max(h, o, c) + 0.01, 2)
        lo = round(min(lo, o, c) - 0.01, 2)
        vol = round(1.0 + (i * 17 % 20), 2)
        rows.append({
            "timestamp": base_time.replace(day=i + 1) if i < 28 else base_time.replace(month=2, day=i - 27),
            "symbol": "BTCUSDT",
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "volume": vol,
        })

    return pl.DataFrame(rows)


@pytest.fixture
def sample_datasets_yaml(tmp_data_dir: Path) -> Path:
    """Write a valid datasets.yaml to temp dir."""
    config_path = tmp_data_dir / "config" / "datasets.yaml"
    config_path.write_text(textwrap.dedent("""\
        datasets:
          test_trades:
            description: "Test trades"
            source:
              type: file
              path: "test_trades.csv"
              format: csv
            schema:
              columns:
                - name: trade_id
                  type: str
                  nullable: false
                - name: symbol
                  type: str
                  nullable: false
                - name: price
                  type: float
                  nullable: false
                - name: quantity
                  type: float
                  nullable: false
                - name: side
                  type: str
                  nullable: false
                - name: timestamp
                  type: datetime
                  nullable: false
                  source_timezone: UTC
            processing:
              dedup_keys: [trade_id]
              dedup_strategy: keep_last
              normalization_version: "1.0"
    """))
    return config_path


@pytest.fixture
def sample_features_yaml(tmp_data_dir: Path) -> Path:
    """Write a valid features.yaml to temp dir."""
    config_path = tmp_data_dir / "config" / "features.yaml"
    config_path.write_text(textwrap.dedent("""\
        test_trades:
          basic_factors:
            version: 1
            description: "Basic trading factors"
            features:
              - name: price_sma_5
                type: moving_average
                column: price
                window: 5
              - name: price_returns
                type: returns
                column: price
    """))
    return config_path


@pytest.fixture
def sample_trades_csv(tmp_data_dir: Path, sample_trades_df: pl.DataFrame) -> Path:
    """Write sample trades to a CSV file."""
    csv_path = tmp_data_dir / "test_trades.csv"
    sample_trades_df.write_csv(csv_path)
    return csv_path


@pytest.fixture
def sample_trades_parquet(tmp_data_dir: Path, sample_trades_df: pl.DataFrame) -> Path:
    """Write sample trades to a Parquet file."""
    pq_path = tmp_data_dir / "test_trades.parquet"
    sample_trades_df.write_parquet(pq_path)
    return pq_path
