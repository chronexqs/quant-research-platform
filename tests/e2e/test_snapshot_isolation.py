"""End-to-end snapshot isolation tests.

Tests verify that creating new snapshots or features does not mutate
previously created artifacts, and that old snapshots remain loadable
after new ones are created.
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


def _setup_registered_dataset(
    data_dir: Path,
    config_dir: Path,
    metadata_dir: Path,
) -> MetadataRegistry:
    """Create registry and register the OHLCV dataset. Returns the registry."""
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
    return registry


def _ingest(
    data_dir: Path,
    config_dir: Path,
    registry: MetadataRegistry,
) -> str:
    """Run ingestion and return the ingestion_id."""
    datasets_cfg = load_datasets_config(config_dir / "datasets.yaml")
    ds_config = datasets_cfg.datasets["ohlcv_btcusdt"]

    ing_result = run_ingestion(
        dataset_name="ohlcv_btcusdt",
        dataset_config=ds_config,
        data_dir=data_dir,
        registry=registry,
        force=True,
    )
    return ing_result.ingestion_id


def _create_snapshot(
    data_dir: Path,
    config_dir: Path,
    registry: MetadataRegistry,
    ingestion_id: str,
) -> str:
    """Create a snapshot and return the snapshot_id."""
    datasets_cfg = load_datasets_config(config_dir / "datasets.yaml")
    ds_config = datasets_cfg.datasets["ohlcv_btcusdt"]

    engine = SnapshotEngine(data_dir=data_dir, registry=registry)
    return engine.create_snapshot("ohlcv_btcusdt", ds_config, [ingestion_id])


def _build_features(
    data_dir: Path,
    config_dir: Path,
    registry: MetadataRegistry,
    snapshot_id: str | None = None,
) -> str:
    """Build features and return the feature_snapshot_id."""
    features_cfg = load_features_config(config_dir / "features.yaml")
    fs_config = features_cfg["ohlcv_btcusdt"]["candle_factors"]
    fs_def = parse_feature_set("ohlcv_btcusdt", "candle_factors", fs_config)

    materialiser = FeatureMaterialiser(data_dir=data_dir, registry=registry)
    return materialiser.materialise(fs_def, snapshot_id=snapshot_id)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.e2e
class TestSnapshotIsolation:
    """End-to-end tests verifying snapshot and feature immutability."""

    def test_new_snapshot_doesnt_modify_old(self, tmp_path: Path) -> None:
        """Create snapshot, create another; verify first snapshot data unchanged."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_configs(config_dir, data_dir)
        registry = _setup_registered_dataset(data_dir, config_dir, metadata_dir)

        # First ingest + snapshot
        ing1 = _ingest(data_dir, config_dir, registry)
        snap1_id = _create_snapshot(data_dir, config_dir, registry, ing1)

        # Capture first snapshot data
        snap1_rec = registry.get_snapshot(snap1_id)
        df_snap1_before = pl.read_parquet(Path(snap1_rec.storage_path) / "part-0.parquet")

        # Second ingest + snapshot (same data, but creates a new snapshot)
        ing2 = _ingest(data_dir, config_dir, registry)
        snap2_id = _create_snapshot(data_dir, config_dir, registry, ing2)

        # Verify first snapshot is still intact and unchanged
        df_snap1_after = pl.read_parquet(Path(snap1_rec.storage_path) / "part-0.parquet")
        assert df_snap1_before.shape == df_snap1_after.shape
        assert df_snap1_before.equals(df_snap1_after), (
            "Creating a new snapshot must not modify the first snapshot"
        )

        # Verify snapshots are distinct
        assert snap1_id != snap2_id

        # Verify current_snapshot was updated to snap2
        dataset = registry.get_dataset("ohlcv_btcusdt")
        assert dataset.current_snapshot == snap2_id

    def test_new_feature_doesnt_modify_old(self, tmp_path: Path) -> None:
        """Build features, build again; verify first feature snapshot unchanged."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_configs(config_dir, data_dir)
        registry = _setup_registered_dataset(data_dir, config_dir, metadata_dir)

        # Ingest + snapshot
        ing_id = _ingest(data_dir, config_dir, registry)
        snap_id = _create_snapshot(data_dir, config_dir, registry, ing_id)

        # First feature build
        fsnap1_id = _build_features(data_dir, config_dir, registry, snapshot_id=snap_id)

        # Capture first feature snapshot data
        fsnap1_rec = registry.get_feature_snapshot(fsnap1_id)
        df_feat1_before = pl.read_parquet(Path(fsnap1_rec.storage_path) / "part-0.parquet")

        # Second feature build from the same snapshot
        fsnap2_id = _build_features(data_dir, config_dir, registry, snapshot_id=snap_id)

        # Verify first feature snapshot is unchanged
        df_feat1_after = pl.read_parquet(Path(fsnap1_rec.storage_path) / "part-0.parquet")
        assert df_feat1_before.shape == df_feat1_after.shape
        assert df_feat1_before.equals(df_feat1_after), (
            "Building new features must not modify previously materialised features"
        )

        # Verify feature snapshots are distinct
        assert fsnap1_id != fsnap2_id

    def test_load_old_snapshot_after_new(self, tmp_path: Path) -> None:
        """After creating new snapshot, old snapshot still loadable and correct."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_configs(config_dir, data_dir)
        registry = _setup_registered_dataset(data_dir, config_dir, metadata_dir)

        # First ingest + snapshot
        ing1 = _ingest(data_dir, config_dir, registry)
        snap1_id = _create_snapshot(data_dir, config_dir, registry, ing1)

        # Load first snapshot via API and capture data
        df_snap1 = load_dataset(
            "ohlcv_btcusdt", snapshot_id=snap1_id, registry=registry,
        ).collect()

        # Build features from first snapshot
        fsnap1_id = _build_features(data_dir, config_dir, registry, snapshot_id=snap1_id)
        df_feat1 = load_features(
            "ohlcv_btcusdt", "candle_factors", snapshot_id=fsnap1_id, registry=registry,
        ).collect()

        # Second ingest + snapshot + features
        ing2 = _ingest(data_dir, config_dir, registry)
        snap2_id = _create_snapshot(data_dir, config_dir, registry, ing2)
        fsnap2_id = _build_features(data_dir, config_dir, registry, snapshot_id=snap2_id)

        # Verify old snapshot is still loadable via explicit snapshot_id
        df_snap1_after = load_dataset(
            "ohlcv_btcusdt", snapshot_id=snap1_id, registry=registry,
        ).collect()
        assert df_snap1.shape == df_snap1_after.shape
        assert df_snap1.equals(df_snap1_after), (
            "Old snapshot should still be loadable after creating a new one"
        )

        # Verify old feature snapshot is still loadable
        df_feat1_after = load_features(
            "ohlcv_btcusdt", "candle_factors", snapshot_id=fsnap1_id, registry=registry,
        ).collect()
        assert df_feat1.shape == df_feat1_after.shape
        assert df_feat1.equals(df_feat1_after), (
            "Old feature snapshot should still be loadable after creating a new one"
        )

        # Verify the default (current) snapshot is the new one
        df_current = load_dataset("ohlcv_btcusdt", registry=registry).collect()
        snap2_rec = registry.get_snapshot(snap2_id)
        assert len(df_current) == snap2_rec.row_count

        # Both snapshots are in the list
        snapshots = registry.list_snapshots("ohlcv_btcusdt")
        snap_ids = {s.snapshot_id for s in snapshots}
        assert snap1_id in snap_ids
        assert snap2_id in snap_ids
