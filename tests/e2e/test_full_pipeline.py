"""End-to-end tests for the full ADP pipeline.

Tests cover: CSV-to-features for OHLCV and RFQ data using the actual
mock data files, plus metadata integrity verification across all 7 tables.
"""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import pytest

from adp.api import load_dataset, load_features
from adp.config import load_datasets_config, load_features_config
from adp.features.definitions import parse_feature_set
from adp.features.materializer import FeatureMaterialiser
from adp.ingestion import run_ingestion
from adp.metadata.registry import MetadataRegistry
from adp.processing.schema import compute_schema_hash_from_defs
from adp.storage.snapshot import SnapshotEngine

# Project root for accessing mock data
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_MOCK_DATA_DIR = _PROJECT_ROOT / "data" / "mock_data"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_isolated_env(tmp_path: Path) -> tuple[Path, Path, Path]:
    """Create an isolated directory structure and return
    (data_dir, config_dir, metadata_dir)."""
    data_dir = tmp_path / "data"
    config_dir = tmp_path / "config"
    metadata_dir = tmp_path / "metadata"

    for subdir in ("raw", "staged", "normalized", "features"):
        (data_dir / subdir).mkdir(parents=True)
    config_dir.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)

    # Copy mock data into the isolated environment
    mock_dir = data_dir / "mock_data"
    mock_dir.mkdir(parents=True)
    for csv_file in _MOCK_DATA_DIR.glob("*.csv"):
        shutil.copy2(csv_file, mock_dir / csv_file.name)

    return data_dir, config_dir, metadata_dir


def _write_ohlcv_config(config_dir: Path, data_dir: Path) -> Path:
    """Write datasets.yaml for OHLCV dataset with correct path."""
    datasets_yaml = config_dir / "datasets.yaml"
    csv_path = data_dir / "mock_data" / "ohlcv_btcusdt_1s.csv"
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
    return datasets_yaml


def _write_ohlcv_features_config(config_dir: Path) -> Path:
    """Write features.yaml for OHLCV dataset."""
    features_yaml = config_dir / "features.yaml"
    features_yaml.write_text(textwrap.dedent("""\
        ohlcv_btcusdt:
          candle_factors:
            version: 1
            description: "Core candle-based analytical factors for BTCUSDT"
            features:
              - name: rolling_vol_5
                type: rolling_std
                column: close
                window: 5
              - name: sma_10
                type: moving_average
                column: close
                window: 10
              - name: sma_50
                type: moving_average
                column: close
                window: 50
              - name: ewma_20
                type: ewma
                column: close
                span: 20
              - name: close_returns
                type: returns
                column: close
              - name: close_log_returns
                type: log_returns
                column: close
              - name: vwap
                type: vwap
                price_column: close
                volume_column: volume
    """))
    return features_yaml


def _write_rfq_config(config_dir: Path, data_dir: Path) -> Path:
    """Write datasets.yaml for RFQ dataset with correct path."""
    datasets_yaml = config_dir / "datasets.yaml"
    csv_path = data_dir / "mock_data" / "rfq_events.csv"
    datasets_yaml.write_text(textwrap.dedent(f"""\
        datasets:
          rfq_events:
            description: "Pre-trade RFQ lifecycle events"
            source:
              type: file
              path: "{csv_path}"
              format: csv
            schema:
              columns:
                - name: event_id
                  type: str
                  nullable: false
                - name: rfq_id
                  type: str
                  nullable: false
                - name: instrument
                  type: str
                  nullable: false
                - name: side
                  type: str
                  nullable: false
                - name: requested_qty
                  type: float
                  nullable: false
                - name: counterparty
                  type: str
                  nullable: false
                - name: timestamp
                  type: datetime
                  nullable: false
                  source_timezone: UTC
                - name: status
                  type: str
                  nullable: false
                - name: quoted_price
                  type: float
                  nullable: true
            processing:
              dedup_keys: [event_id]
              dedup_strategy: keep_last
              normalization_version: "1.0"
    """))
    return datasets_yaml


def _run_full_pipeline(
    dataset_name: str,
    data_dir: Path,
    config_dir: Path,
    metadata_dir: Path,
    feature_set_name: str | None = None,
) -> tuple[MetadataRegistry, str, str | None]:
    """Run the full pipeline: register -> ingest -> snapshot -> features.

    Returns (registry, snapshot_id, feature_snapshot_id).
    """
    db_path = metadata_dir / "adp_registry.db"
    registry = MetadataRegistry(db_path)

    datasets_yaml = config_dir / "datasets.yaml"
    datasets_cfg = load_datasets_config(datasets_yaml)
    ds_config = datasets_cfg.datasets[dataset_name]

    # Register dataset
    schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
    registry.register_dataset(
        dataset_name=dataset_name,
        schema_hash=schema_hash,
        description=ds_config.description,
    )

    # Ingest
    ing_result = run_ingestion(
        dataset_name=dataset_name,
        dataset_config=ds_config,
        data_dir=data_dir,
        registry=registry,
    )

    # Snapshot
    engine = SnapshotEngine(data_dir=data_dir, registry=registry)
    snapshot_id = engine.create_snapshot(
        dataset_name, ds_config, [ing_result.ingestion_id],
    )

    # Features (if requested)
    fsnap_id = None
    if feature_set_name:
        features_yaml = config_dir / "features.yaml"
        features_cfg = load_features_config(features_yaml)
        fs_config = features_cfg[dataset_name][feature_set_name]
        fs_def = parse_feature_set(dataset_name, feature_set_name, fs_config)

        materialiser = FeatureMaterialiser(data_dir=data_dir, registry=registry)
        fsnap_id = materialiser.materialise(fs_def)

    return registry, snapshot_id, fsnap_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.e2e
class TestFullPipeline:
    """End-to-end tests for the complete ADP pipeline."""

    def test_full_pipeline_csv_to_features(self, tmp_path: Path) -> None:
        """Full pipeline for OHLCV data: init -> register -> ingest ->
        snapshot -> features -> load.  Verify final feature DataFrame
        has correct shape and all expected columns."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_config(config_dir, data_dir)
        _write_ohlcv_features_config(config_dir)

        registry, _snapshot_id, fsnap_id = _run_full_pipeline(
            dataset_name="ohlcv_btcusdt",
            data_dir=data_dir,
            config_dir=config_dir,
            metadata_dir=metadata_dir,
            feature_set_name="candle_factors",
        )

        assert fsnap_id is not None

        # Load dataset via API
        ds_lf = load_dataset("ohlcv_btcusdt", registry=registry)
        ds_df = ds_lf.collect()
        assert len(ds_df) == 3600
        assert set(ds_df.columns) == {
            "timestamp", "symbol", "open", "high", "low", "close", "volume",
        }

        # Load features via API
        feat_lf = load_features("ohlcv_btcusdt", "candle_factors", registry=registry)
        feat_df = feat_lf.collect()
        assert len(feat_df) == 3600

        expected_feature_cols = {
            "rolling_vol_5", "sma_10", "sma_50", "ewma_20",
            "close_returns", "close_log_returns", "vwap",
        }
        expected_all_cols = {
            "timestamp", "symbol", "open", "high", "low", "close", "volume",
        } | expected_feature_cols
        assert expected_all_cols == set(feat_df.columns)

    def test_full_pipeline_rfq_to_snapshot(self, tmp_path: Path) -> None:
        """Full pipeline for RFQ data: ingest -> snapshot.
        Verify dataset is loadable and has the correct row count."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_rfq_config(config_dir, data_dir)

        registry, snapshot_id, _ = _run_full_pipeline(
            dataset_name="rfq_events",
            data_dir=data_dir,
            config_dir=config_dir,
            metadata_dir=metadata_dir,
        )

        # Load dataset via API
        ds_lf = load_dataset("rfq_events", registry=registry)
        ds_df = ds_lf.collect()
        assert len(ds_df) == 60
        assert "event_id" in ds_df.columns
        assert "rfq_id" in ds_df.columns
        assert "quoted_price" in ds_df.columns
        assert "timestamp" in ds_df.columns

        # Verify snapshot record
        snap = registry.get_snapshot(snapshot_id)
        assert snap is not None
        assert snap.dataset_name == "rfq_events"
        assert snap.row_count == 60

    def test_full_pipeline_metadata_integrity(self, tmp_path: Path) -> None:
        """After full pipeline, verify all 7 metadata tables have correct
        linked records."""
        data_dir, config_dir, metadata_dir = _create_isolated_env(tmp_path)
        _write_ohlcv_config(config_dir, data_dir)
        _write_ohlcv_features_config(config_dir)

        registry, snapshot_id, fsnap_id = _run_full_pipeline(
            dataset_name="ohlcv_btcusdt",
            data_dir=data_dir,
            config_dir=config_dir,
            metadata_dir=metadata_dir,
            feature_set_name="candle_factors",
        )

        # 1. datasets table
        dataset = registry.get_dataset("ohlcv_btcusdt")
        assert dataset is not None
        assert dataset.current_snapshot == snapshot_id
        assert dataset.schema_hash is not None

        # 2. raw_ingestions table
        ingestions = registry.list_ingestions("ohlcv_btcusdt")
        assert len(ingestions) >= 1
        ing = ingestions[0]
        assert ing.dataset_name == "ohlcv_btcusdt"
        assert ing.row_count == 3600

        # 3. snapshots table
        snapshots = registry.list_snapshots("ohlcv_btcusdt")
        assert len(snapshots) >= 1
        snap = registry.get_snapshot(snapshot_id)
        assert snap.dataset_name == "ohlcv_btcusdt"
        assert snap.row_count == 3600

        # 4. snapshot_lineage table
        lineage = registry.get_snapshot_lineage(snapshot_id)
        assert len(lineage) >= 1
        assert lineage[0].ingestion_id == ing.ingestion_id

        # 5. feature_definitions table
        feat_defs = registry.list_feature_definitions("ohlcv_btcusdt")
        assert len(feat_defs) >= 1
        assert any(fd.feature_name == "candle_factors" for fd in feat_defs)

        # 6. feature_snapshots table
        assert fsnap_id is not None
        fsnap = registry.get_feature_snapshot(fsnap_id)
        assert fsnap is not None
        assert fsnap.feature_name == "candle_factors"
        assert fsnap.row_count == 3600

        # 7. feature_lineage table
        feat_lineage = registry.get_feature_lineage(fsnap_id)
        assert len(feat_lineage) >= 1
        assert feat_lineage[0].dataset_snapshot_id == snapshot_id
