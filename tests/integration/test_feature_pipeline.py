"""Integration tests for the feature pipeline.

Tests cover: snapshot-to-features materialisation, feature metadata
registration, Parquet readability, column presence, explicit snapshot
targeting, and multiple feature sets from the same snapshot.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from adp.config import (
    load_datasets_config,
    load_features_config,
)
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


def _setup_full_snapshot(
    tmp_data_dir: Path,
    sample_datasets_yaml: Path,
    sample_features_yaml: Path,
    n: int = 100,
) -> tuple[MetadataRegistry, str, Path]:
    """Set up registry, ingest CSV, create snapshot.

    Returns (registry, snapshot_id, features_yaml_path).
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
    csv_path = _make_trades_csv(tmp_data_dir / "trades.csv", n=n)

    # Ingest
    strategy = FileIngestionStrategy(
        data_dir=tmp_data_dir / "data",
        registry=registry,
    )
    result = strategy.ingest("test_trades", {
        "path": str(csv_path),
        "format": "csv",
        "encoding": "utf-8",
    })

    # Snapshot
    engine = SnapshotEngine(
        data_dir=tmp_data_dir / "data",
        registry=registry,
    )
    snapshot_id = engine.create_snapshot("test_trades", ds_config, [result.ingestion_id])

    return registry, snapshot_id, sample_features_yaml


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestFeaturePipeline:
    """Integration tests for the feature materialisation pipeline."""

    def test_snapshot_to_features(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Create snapshot, build features, verify feature Parquet exists."""
        registry, _snapshot_id, features_yaml = _setup_full_snapshot(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        features_cfg = load_features_config(features_yaml)
        fs_config = features_cfg["test_trades"]["basic_factors"]
        fs_def = parse_feature_set("test_trades", "basic_factors", fs_config)

        materialiser = FeatureMaterialiser(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        fsnap_id = materialiser.materialise(fs_def)

        # Verify feature Parquet directory exists
        fsnap_record = registry.get_feature_snapshot(fsnap_id)
        assert fsnap_record is not None
        feature_dir = Path(fsnap_record.storage_path)
        assert feature_dir.exists()
        parquet_files = list(feature_dir.glob("*.parquet"))
        assert len(parquet_files) >= 1

    def test_feature_metadata_registered(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Verify feature_definitions and feature_snapshots tables are populated."""
        registry, snapshot_id, features_yaml = _setup_full_snapshot(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        features_cfg = load_features_config(features_yaml)
        fs_config = features_cfg["test_trades"]["basic_factors"]
        fs_def = parse_feature_set("test_trades", "basic_factors", fs_config)

        materialiser = FeatureMaterialiser(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        fsnap_id = materialiser.materialise(fs_def)

        # Verify feature definition was registered
        feat_def = registry.get_feature_definition("basic_factors", "test_trades", version=1)
        assert feat_def is not None
        assert feat_def.feature_name == "basic_factors"
        assert feat_def.dataset_name == "test_trades"
        assert feat_def.version == 1
        assert feat_def.definition_hash is not None

        # Verify feature snapshot was registered
        fsnap = registry.get_feature_snapshot(fsnap_id)
        assert fsnap is not None
        assert fsnap.feature_name == "basic_factors"
        assert fsnap.dataset_name == "test_trades"
        assert fsnap.row_count == 100

        # Verify feature lineage
        lineage = registry.get_feature_lineage(fsnap_id)
        assert len(lineage) == 1
        assert lineage[0].dataset_snapshot_id == snapshot_id

    def test_feature_parquet_readable(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Read feature Parquet, verify columns include feature names."""
        registry, _snapshot_id, features_yaml = _setup_full_snapshot(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        features_cfg = load_features_config(features_yaml)
        fs_config = features_cfg["test_trades"]["basic_factors"]
        fs_def = parse_feature_set("test_trades", "basic_factors", fs_config)

        materialiser = FeatureMaterialiser(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        fsnap_id = materialiser.materialise(fs_def)

        fsnap = registry.get_feature_snapshot(fsnap_id)
        feature_dir = Path(fsnap.storage_path)
        parquet_files = sorted(feature_dir.glob("*.parquet"))
        df = pl.read_parquet(parquet_files[0])

        assert len(df) == 100
        # Feature columns should be present
        assert "price_sma_5" in df.columns
        assert "price_returns" in df.columns

    def test_feature_columns_present(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Check all expected feature columns exist in the output DataFrame."""
        registry, _snapshot_id, features_yaml = _setup_full_snapshot(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        features_cfg = load_features_config(features_yaml)
        fs_config = features_cfg["test_trades"]["basic_factors"]
        fs_def = parse_feature_set("test_trades", "basic_factors", fs_config)

        materialiser = FeatureMaterialiser(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        fsnap_id = materialiser.materialise(fs_def)

        fsnap = registry.get_feature_snapshot(fsnap_id)
        feature_dir = Path(fsnap.storage_path)
        df = pl.read_parquet(sorted(feature_dir.glob("*.parquet"))[0])

        expected_feature_cols = {"price_sma_5", "price_returns"}
        assert expected_feature_cols.issubset(set(df.columns))

        # Original columns should also be preserved
        original_cols = {"trade_id", "symbol", "price", "quantity", "side", "timestamp"}
        assert original_cols.issubset(set(df.columns))

    def test_feature_from_specific_snapshot(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Build features with explicit snapshot_id targeting a specific snapshot."""
        registry, snap1_id, features_yaml = _setup_full_snapshot(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        # Create a second ingestion and snapshot with fewer rows
        datasets_cfg = load_datasets_config(sample_datasets_yaml)
        ds_config = datasets_cfg.datasets["test_trades"]

        csv2 = _make_trades_csv(tmp_data_dir / "trades_v2.csv", n=50)

        strategy = FileIngestionStrategy(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        result2 = strategy.ingest("test_trades", {
            "path": str(csv2),
            "format": "csv",
            "encoding": "utf-8",
        })

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        engine.create_snapshot("test_trades", ds_config, [result2.ingestion_id])

        # Build features explicitly from the FIRST snapshot (100 rows)
        features_cfg = load_features_config(features_yaml)
        fs_config = features_cfg["test_trades"]["basic_factors"]
        fs_def = parse_feature_set("test_trades", "basic_factors", fs_config)

        materialiser = FeatureMaterialiser(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        fsnap_id = materialiser.materialise(fs_def, snapshot_id=snap1_id)

        fsnap = registry.get_feature_snapshot(fsnap_id)
        assert fsnap.row_count == 100  # Should be from snap1, not snap2

        # Verify lineage points to snap1
        lineage = registry.get_feature_lineage(fsnap_id)
        assert lineage[0].dataset_snapshot_id == snap1_id

    def test_multiple_feature_sets(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_features_yaml: Path,
    ) -> None:
        """Build two different feature sets from the same snapshot."""
        registry, _snapshot_id, features_yaml = _setup_full_snapshot(
            tmp_data_dir, sample_datasets_yaml, sample_features_yaml,
        )

        features_cfg = load_features_config(features_yaml)

        # Build the first feature set (basic_factors)
        fs_config1 = features_cfg["test_trades"]["basic_factors"]
        fs_def1 = parse_feature_set("test_trades", "basic_factors", fs_config1)

        materialiser = FeatureMaterialiser(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        fsnap_id1 = materialiser.materialise(fs_def1)

        # Create a second feature set config inline
        second_features_yaml = tmp_data_dir / "config" / "features_v2.yaml"
        second_features_yaml.write_text(
            "test_trades:\n"
            "  price_analytics:\n"
            "    version: 1\n"
            "    description: 'Price analytics'\n"
            "    features:\n"
            "      - name: price_log_returns\n"
            "        type: log_returns\n"
            "        column: price\n"
        )
        features_cfg2 = load_features_config(second_features_yaml)
        fs_config2 = features_cfg2["test_trades"]["price_analytics"]
        fs_def2 = parse_feature_set("test_trades", "price_analytics", fs_config2)

        fsnap_id2 = materialiser.materialise(fs_def2)

        # Both feature snapshots should exist
        fsnap1 = registry.get_feature_snapshot(fsnap_id1)
        fsnap2 = registry.get_feature_snapshot(fsnap_id2)
        assert fsnap1 is not None
        assert fsnap2 is not None
        assert fsnap1.feature_name == "basic_factors"
        assert fsnap2.feature_name == "price_analytics"

        # Both should have the correct row count
        assert fsnap1.row_count == 100
        assert fsnap2.row_count == 100

        # Verify separate storage paths
        assert fsnap1.storage_path != fsnap2.storage_path
