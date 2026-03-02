"""Integration tests for DuckDB query layer.

Tests cover: basic SQL queries on datasets and features, aggregation,
return type verification, and error handling for nonexistent datasets.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from adp.api import query_dataset, query_features
from adp.config import load_datasets_config, load_features_config
from adp.exceptions import DatasetNotFoundError, FeatureSetNotFoundError
from adp.features.definitions import parse_feature_set
from adp.features.materializer import FeatureMaterialiser
from adp.ingestion.file import FileIngestionStrategy
from adp.metadata.registry import MetadataRegistry
from adp.processing.schema import compute_schema_hash_from_defs
from adp.storage.snapshot import SnapshotEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_trades_csv(path: Path, n: int = 100) -> Path:
    """Create a deterministic trades CSV with tz-naive timestamp strings."""
    base_time = datetime(2026, 1, 15, 10, 0, 0)
    rows = []
    for i in range(n):
        ts = base_time.replace(second=i % 60, minute=i // 60)
        rows.append({
            "trade_id": f"T_{i:04d}",
            "symbol": "BTCUSDT" if i % 2 == 0 else "ETHUSDT",
            "price": 42000.0 + i * 10.0 + (i * 7 % 13),
            "quantity": 0.1 + (i * 3 % 10) / 10.0,
            "side": "buy" if i % 3 != 0 else "sell",
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        })

    df = pl.DataFrame(rows)
    df.write_csv(path)
    return path


def _setup_full_environment(
    tmp_data_dir: Path,
    sample_datasets_yaml: Path,
    sample_features_yaml: Path,
) -> tuple[MetadataRegistry, str, str]:
    """Set up a complete environment: ingest -> snapshot -> features.

    Returns (registry, snapshot_id, feature_snapshot_id).
    """
    db_path = tmp_data_dir / "metadata" / "adp_registry.db"
    registry = MetadataRegistry(db_path)

    datasets_cfg = load_datasets_config(sample_datasets_yaml)
    ds_config = datasets_cfg.datasets["test_trades"]

    schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
    registry.register_dataset(
        dataset_name="test_trades",
        schema_hash=schema_hash,
        description=ds_config.description,
    )

    # Create CSV with tz-naive timestamps
    csv_path = _make_trades_csv(tmp_data_dir / "trades.csv")

    # Ingest
    strategy = FileIngestionStrategy(
        data_dir=tmp_data_dir / "data",
        registry=registry,
    )
    ing_result = strategy.ingest("test_trades", {
        "path": str(csv_path),
        "format": "csv",
        "encoding": "utf-8",
    })

    # Snapshot
    engine = SnapshotEngine(
        data_dir=tmp_data_dir / "data",
        registry=registry,
    )
    snapshot_id = engine.create_snapshot(
        "test_trades", ds_config, [ing_result.ingestion_id],
    )

    # Features
    features_cfg = load_features_config(sample_features_yaml)
    fs_config = features_cfg["test_trades"]["basic_factors"]
    fs_def = parse_feature_set("test_trades", "basic_factors", fs_config)

    materialiser = FeatureMaterialiser(
        data_dir=tmp_data_dir / "data",
        registry=registry,
    )
    fsnap_id = materialiser.materialise(fs_def)

    return registry, snapshot_id, fsnap_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestDuckDBQueries:
    """Integration tests for DuckDB SQL query interface."""

    def test_query_dataset_basic_sql(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """SELECT * FROM dataset LIMIT 5 should return 5 rows."""
        registry, snapshot_id, fsnap_id = _setup_full_environment(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        result = query_dataset(
            "test_trades",
            "SELECT * FROM dataset LIMIT 5",
            registry=registry,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5
        assert "trade_id" in result.columns
        assert "price" in result.columns
        assert "symbol" in result.columns

    def test_query_dataset_aggregation(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """SELECT COUNT(*), AVG(price) should return valid aggregation."""
        registry, snapshot_id, fsnap_id = _setup_full_environment(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        result = query_dataset(
            "test_trades",
            "SELECT COUNT(*) AS cnt, AVG(price) AS avg_price FROM dataset",
            registry=registry,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 1
        assert result["cnt"][0] == 100
        assert result["avg_price"][0] > 0

    def test_query_features_basic_sql(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """SELECT * FROM features LIMIT 5 should return 5 rows with feature columns."""
        registry, snapshot_id, fsnap_id = _setup_full_environment(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        result = query_features(
            "test_trades",
            "basic_factors",
            "SELECT * FROM features LIMIT 5",
            registry=registry,
        )

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 5
        assert "price_sma_5" in result.columns
        assert "price_returns" in result.columns

    def test_query_returns_polars_dataframe(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Verify return type is pl.DataFrame for both dataset and feature queries."""
        registry, snapshot_id, fsnap_id = _setup_full_environment(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        ds_result = query_dataset(
            "test_trades",
            "SELECT * FROM dataset LIMIT 1",
            registry=registry,
        )
        assert type(ds_result) is pl.DataFrame

        feat_result = query_features(
            "test_trades",
            "basic_factors",
            "SELECT * FROM features LIMIT 1",
            registry=registry,
        )
        assert type(feat_result) is pl.DataFrame

    def test_query_nonexistent_dataset(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Querying a dataset that does not exist should raise an appropriate error."""
        registry, snapshot_id, fsnap_id = _setup_full_environment(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        with pytest.raises(DatasetNotFoundError):
            query_dataset(
                "nonexistent_dataset",
                "SELECT * FROM dataset LIMIT 5",
                registry=registry,
            )

        with pytest.raises(FeatureSetNotFoundError):
            query_features(
                "test_trades",
                "nonexistent_feature_set",
                "SELECT * FROM features LIMIT 5",
                registry=registry,
            )
