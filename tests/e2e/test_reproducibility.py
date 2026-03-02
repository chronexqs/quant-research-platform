"""End-to-end reproducibility tests.

Tests verify that identical inputs produce bit-for-bit identical outputs
at the snapshot and feature levels, and that data survives registry
close/reopen cycles.
"""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import polars as pl
import pytest

from adp.api import load_dataset, load_features
from adp.config import load_datasets_config, load_features_config
from adp.features.definitions import parse_feature_set
from adp.features.materializer import FeatureMaterialiser
from adp.ingestion import run_ingestion
from adp.metadata.registry import MetadataRegistry
from adp.processing.schema import compute_schema_hash_from_defs
from adp.storage.snapshot import SnapshotEngine

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MOCK_DATA_DIR = _PROJECT_ROOT / "data" / "mock_data"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_isolated_env(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Create an isolated directory structure."""
    data_dir = tmp_path / "data"
    config_dir = tmp_path / "config"
    metadata_dir = tmp_path / "metadata"

    for subdir in ("raw", "staged", "normalized", "features"):
        (data_dir / subdir).mkdir(parents=True)
    config_dir.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)

    # Copy mock data
    mock_dir = data_dir / "mock_data"
    mock_dir.mkdir(parents=True)
    for csv_file in _MOCK_DATA_DIR.glob("*.csv"):
        shutil.copy2(csv_file, mock_dir / csv_file.name)

    return data_dir, config_dir, metadata_dir


def _write_ohlcv_configs(config_dir: Path, data_dir: Path) -> tuple[Path, Path]:
    """Write datasets.yaml and features.yaml for OHLCV."""
    csv_path = data_dir / "mock_data" / "ohlcv_btcusdt_1s.csv"

    datasets_yaml = config_dir / "datasets.yaml"
    datasets_yaml.write_text(textwrap.dedent(f"""\
        datasets:
          ohlcv_btcusdt:
            description: "BTCUSDT 1-second OHLCV candles"
            source:
              type: file
              path: "{csv_path}"
              format: csv
            schema:
              columns:
                - name: timestamp
                  type: datetime
                  nullable: false
                  source_timezone: UTC
                - name: symbol
                  type: str
                  nullable: false
                - name: open
                  type: float
                  nullable: false
                - name: high
                  type: float
                  nullable: false
                - name: low
                  type: float
                  nullable: false
                - name: close
                  type: float
                  nullable: false
                - name: volume
                  type: float
                  nullable: false
            processing:
              dedup_keys: [timestamp, symbol]
              dedup_strategy: keep_last
              normalization_version: "1.0"
    """))

    features_yaml = config_dir / "features.yaml"
    features_yaml.write_text(textwrap.dedent("""\
        ohlcv_btcusdt:
          candle_factors:
            version: 1
            description: "Core candle-based analytical factors"
            features:
              - name: rolling_vol_5
                type: rolling_std
                column: close
                window: 5
              - name: sma_10
                type: moving_average
                column: close
                window: 10
              - name: close_returns
                type: returns
                column: close
    """))

    return datasets_yaml, features_yaml


def _ingest_and_snapshot(
    dataset_name: str,
    data_dir: Path,
    config_dir: Path,
    registry: MetadataRegistry,
) -> tuple[str, str]:
    """Run ingest + snapshot, return (ingestion_id, snapshot_id)."""
    datasets_yaml = config_dir / "datasets.yaml"
    datasets_cfg = load_datasets_config(datasets_yaml)
    ds_config = datasets_cfg.datasets[dataset_name]

    ing_result = run_ingestion(
        dataset_name=dataset_name,
        dataset_config=ds_config,
        data_dir=data_dir,
        registry=registry,
        force=True,
    )

    engine = SnapshotEngine(data_dir=data_dir, registry=registry)
    snapshot_id = engine.create_snapshot(
        dataset_name, ds_config, [ing_result.ingestion_id],
    )
    return ing_result.ingestion_id, snapshot_id


def _build_features(
    dataset_name: str,
    feature_set_name: str,
    data_dir: Path,
    config_dir: Path,
    registry: MetadataRegistry,
    snapshot_id: str | None = None,
) -> str:
    """Build features and return the feature_snapshot_id."""
    features_yaml = config_dir / "features.yaml"
    features_cfg = load_features_config(features_yaml)
    fs_config = features_cfg[dataset_name][feature_set_name]
    fs_def = parse_feature_set(dataset_name, feature_set_name, fs_config)

    materialiser = FeatureMaterialiser(data_dir=data_dir, registry=registry)
    return materialiser.materialise(fs_def, snapshot_id=snapshot_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.e2e
class TestReproducibility:
    """End-to-end reproducibility tests."""

    def test_snapshot_reproducibility(self, tmp_path: Path) -> None:
        """Ingest same data, create snapshot twice from same raw data,
        verify Parquet files have identical content."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_configs(config_dir, data_dir)

        db_path = metadata_dir / "adp_registry.db"
        registry = MetadataRegistry(db_path)

        datasets_cfg = load_datasets_config(config_dir / "datasets.yaml")
        ds_config = datasets_cfg.datasets["ohlcv_btcusdt"]
        schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
        registry.register_dataset(
            dataset_name="ohlcv_btcusdt",
            schema_hash=schema_hash,
            description=ds_config.description,
        )

        # Ingest once
        ing_id, snap1_id = _ingest_and_snapshot(
            "ohlcv_btcusdt", data_dir, config_dir, registry,
        )

        # Create second snapshot from the SAME raw ingestion
        engine = SnapshotEngine(data_dir=data_dir, registry=registry)
        snap2_id = engine.create_snapshot("ohlcv_btcusdt", ds_config, [ing_id])

        # Read both snapshots
        snap1_rec = registry.get_snapshot(snap1_id)
        snap2_rec = registry.get_snapshot(snap2_id)

        df1 = pl.read_parquet(Path(snap1_rec.storage_path) / "part-0.parquet")
        df2 = pl.read_parquet(Path(snap2_rec.storage_path) / "part-0.parquet")

        # Verify identical content (sort to handle non-deterministic row order
        # from the dedup step which uses ``unique(keep="last")``)
        assert df1.shape == df2.shape
        assert df1.columns == df2.columns
        sort_cols = ["timestamp", "symbol"]
        assert df1.sort(sort_cols).equals(df2.sort(sort_cols)), (
            "Snapshots from identical data should be identical"
        )

    def test_feature_reproducibility(self, tmp_path: Path) -> None:
        """Build features from same snapshot twice, verify identical content."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_configs(config_dir, data_dir)

        db_path = metadata_dir / "adp_registry.db"
        registry = MetadataRegistry(db_path)

        datasets_cfg = load_datasets_config(config_dir / "datasets.yaml")
        ds_config = datasets_cfg.datasets["ohlcv_btcusdt"]
        schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
        registry.register_dataset(
            dataset_name="ohlcv_btcusdt",
            schema_hash=schema_hash,
            description=ds_config.description,
        )

        _, snapshot_id = _ingest_and_snapshot(
            "ohlcv_btcusdt", data_dir, config_dir, registry,
        )

        # Build features twice from the same snapshot
        fsnap_id1 = _build_features(
            "ohlcv_btcusdt", "candle_factors", data_dir, config_dir, registry,
            snapshot_id=snapshot_id,
        )
        fsnap_id2 = _build_features(
            "ohlcv_btcusdt", "candle_factors", data_dir, config_dir, registry,
            snapshot_id=snapshot_id,
        )

        # Read both feature snapshots
        fsnap1 = registry.get_feature_snapshot(fsnap_id1)
        fsnap2 = registry.get_feature_snapshot(fsnap_id2)

        df1 = pl.read_parquet(Path(fsnap1.storage_path) / "part-0.parquet")
        df2 = pl.read_parquet(Path(fsnap2.storage_path) / "part-0.parquet")

        # Verify identical content
        assert df1.shape == df2.shape
        assert df1.columns == df2.columns
        assert df1.equals(df2), "Features from same snapshot should be identical"

    def test_cross_session_reproducibility(self, tmp_path: Path) -> None:
        """Full pipeline, close registry, reopen, load data, verify it matches."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_configs(config_dir, data_dir)

        db_path = metadata_dir / "adp_registry.db"

        # Session 1: run pipeline
        registry1 = MetadataRegistry(db_path)
        datasets_cfg = load_datasets_config(config_dir / "datasets.yaml")
        ds_config = datasets_cfg.datasets["ohlcv_btcusdt"]
        schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
        registry1.register_dataset(
            dataset_name="ohlcv_btcusdt",
            schema_hash=schema_hash,
            description=ds_config.description,
        )

        _, snapshot_id = _ingest_and_snapshot(
            "ohlcv_btcusdt", data_dir, config_dir, registry1,
        )
        fsnap_id = _build_features(
            "ohlcv_btcusdt", "candle_factors", data_dir, config_dir, registry1,
            snapshot_id=snapshot_id,
        )

        # Load data in session 1
        ds_df_session1 = load_dataset("ohlcv_btcusdt", registry=registry1).collect()
        feat_df_session1 = load_features(
            "ohlcv_btcusdt", "candle_factors", registry=registry1,
        ).collect()

        # Close session 1
        registry1.close()

        # Session 2: reopen and load
        registry2 = MetadataRegistry(db_path)

        ds_df_session2 = load_dataset("ohlcv_btcusdt", registry=registry2).collect()
        feat_df_session2 = load_features(
            "ohlcv_btcusdt", "candle_factors", registry=registry2,
        ).collect()

        # Verify data matches across sessions
        assert ds_df_session1.shape == ds_df_session2.shape
        assert ds_df_session1.columns == ds_df_session2.columns
        assert ds_df_session1.equals(ds_df_session2), (
            "Dataset data should survive registry close/reopen"
        )

        assert feat_df_session1.shape == feat_df_session2.shape
        assert feat_df_session1.columns == feat_df_session2.columns
        assert feat_df_session1.equals(feat_df_session2), (
            "Feature data should survive registry close/reopen"
        )

        # Verify metadata is intact
        dataset = registry2.get_dataset("ohlcv_btcusdt")
        assert dataset is not None
        assert dataset.current_snapshot == snapshot_id

        fsnap = registry2.get_feature_snapshot(fsnap_id)
        assert fsnap is not None
        assert fsnap.feature_name == "candle_factors"

        registry2.close()
