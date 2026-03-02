"""Integration tests for the snapshot pipeline.

Tests cover: raw-to-normalized snapshot creation, metadata registration,
lineage linking, current_snapshot update, schema enforcement, multiple
snapshot coexistence, and Parquet readability.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from adp.config import (
    ColumnDef,
    ColumnType,
    DatasetConfig,
    DedupStrategy,
    FileFormat,
    ProcessingConfig,
    SchemaConfig,
    SourceConfig,
    SourceType,
    load_datasets_config,
)
from adp.exceptions import SchemaValidationError
from adp.ingestion.file import FileIngestionStrategy
from adp.metadata.registry import MetadataRegistry
from adp.processing.schema import compute_schema_hash_from_defs
from adp.storage.snapshot import SnapshotEngine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_trades_csv(path: Path, n: int = 100) -> Path:
    """Create a deterministic trades CSV with tz-naive timestamp strings.

    This mimics how real CSV data files look (no timezone marker), which
    allows the normalization pipeline's ``str.to_datetime`` to succeed.
    """
    base_time = datetime(2026, 1, 15, 10, 0, 0)
    rows = []
    for i in range(n):
        ts = base_time.replace(second=i % 60, minute=i // 60)
        rows.append(
            {
                "trade_id": f"T_{i:04d}",
                "symbol": "BTCUSDT" if i % 2 == 0 else "ETHUSDT",
                "price": 42000.0 + i * 10.0 + (i * 7 % 13),
                "quantity": 0.1 + (i * 3 % 10) / 10.0,
                "side": "buy" if i % 3 != 0 else "sell",
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            }
        )

    df = pl.DataFrame(rows)
    df.write_csv(path)
    return path


def _setup_environment(
    tmp_data_dir: Path,
    sample_datasets_yaml: Path,
) -> tuple[MetadataRegistry, DatasetConfig]:
    """Create a file-backed registry and register the test_trades dataset."""
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
    return registry, ds_config


def _ingest_csv(
    tmp_data_dir: Path,
    registry: MetadataRegistry,
    csv_path: Path,
    dataset_name: str = "test_trades",
) -> str:
    """Ingest a CSV file and return the ingestion_id."""
    strategy = FileIngestionStrategy(
        data_dir=tmp_data_dir / "data",
        registry=registry,
    )
    result = strategy.ingest(
        dataset_name,
        {
            "path": str(csv_path),
            "format": "csv",
            "encoding": "utf-8",
        },
    )
    return result.ingestion_id


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestSnapshotPipeline:
    """Integration tests for the snapshot pipeline."""

    def test_raw_to_normalized_snapshot(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Full path from raw Parquet to normalized snapshot."""
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)
        csv_path = _make_trades_csv(tmp_data_dir / "trades.csv")
        ingestion_id = _ingest_csv(tmp_data_dir, registry, csv_path)

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        snapshot_id = engine.create_snapshot("test_trades", ds_config, [ingestion_id])

        # Verify snapshot directory exists
        snap_dir = tmp_data_dir / "data" / "normalized" / "test_trades" / snapshot_id
        assert snap_dir.exists()

        # Verify Parquet file exists
        parquet_files = list(snap_dir.glob("*.parquet"))
        assert len(parquet_files) >= 1

        # Verify metadata JSON
        metadata_file = snap_dir / "_metadata.json"
        assert metadata_file.exists()

    def test_snapshot_metadata_registered(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Verify snapshots table populated correctly after snapshot creation."""
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)
        csv_path = _make_trades_csv(tmp_data_dir / "trades.csv")
        ingestion_id = _ingest_csv(tmp_data_dir, registry, csv_path)

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        snapshot_id = engine.create_snapshot("test_trades", ds_config, [ingestion_id])

        # Verify snapshot record
        snap_record = registry.get_snapshot(snapshot_id)
        assert snap_record is not None
        assert snap_record.dataset_name == "test_trades"
        assert snap_record.row_count == 100
        assert snap_record.normalization_version == "1.0"
        assert snap_record.schema_hash is not None
        assert snap_record.storage_path is not None
        assert snap_record.created_at is not None

    def test_snapshot_lineage_linked(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Verify snapshot_lineage table links snapshot to ingestion."""
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)
        csv_path = _make_trades_csv(tmp_data_dir / "trades.csv")
        ingestion_id = _ingest_csv(tmp_data_dir, registry, csv_path)

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        snapshot_id = engine.create_snapshot("test_trades", ds_config, [ingestion_id])

        # Verify lineage
        lineage = registry.get_snapshot_lineage(snapshot_id)
        assert len(lineage) == 1
        assert lineage[0].snapshot_id == snapshot_id
        assert lineage[0].ingestion_id == ingestion_id

    def test_current_snapshot_updated(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Verify datasets.current_snapshot is set after snapshot creation."""
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)
        csv_path = _make_trades_csv(tmp_data_dir / "trades.csv")
        ingestion_id = _ingest_csv(tmp_data_dir, registry, csv_path)

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        snapshot_id = engine.create_snapshot("test_trades", ds_config, [ingestion_id])

        # Verify current_snapshot on the dataset
        dataset = registry.get_dataset("test_trades")
        assert dataset is not None
        assert dataset.current_snapshot == snapshot_id

    def test_schema_enforcement_rejects_bad_data(
        self,
        tmp_data_dir: Path,
    ) -> None:
        """Ingest data with wrong columns; snapshot creation should fail
        during schema validation."""
        db_path = tmp_data_dir / "metadata" / "adp_registry.db"
        registry = MetadataRegistry(db_path)

        # Create a dataset config expecting specific columns
        ds_config = DatasetConfig(
            description="Bad schema test",
            source=SourceConfig(type=SourceType.file, path="dummy.csv", format=FileFormat.csv),
            schema=SchemaConfig(
                columns=[
                    ColumnDef(name="trade_id", type=ColumnType.str_, nullable=False),
                    ColumnDef(name="price", type=ColumnType.float_, nullable=False),
                    ColumnDef(
                        name="timestamp",
                        type=ColumnType.datetime_,
                        nullable=False,
                        source_timezone="UTC",
                    ),
                ]
            ),
            processing=ProcessingConfig(
                dedup_keys=["trade_id"],
                dedup_strategy=DedupStrategy.keep_last,
                normalization_version="1.0",
            ),
        )

        schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
        registry.register_dataset(
            dataset_name="bad_schema",
            schema_hash=schema_hash,
            description="Bad schema test",
        )

        # Write CSV with wrong column names (missing expected columns)
        bad_csv = tmp_data_dir / "bad_data.csv"
        bad_df = pl.DataFrame(
            {
                "wrong_col_a": ["a", "b", "c"],
                "wrong_col_b": [1.0, 2.0, 3.0],
            }
        )
        bad_df.write_csv(bad_csv)

        # Ingest the bad data
        strategy = FileIngestionStrategy(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        result = strategy.ingest(
            "bad_schema",
            {
                "path": str(bad_csv),
                "format": "csv",
                "encoding": "utf-8",
            },
        )

        # Creating a snapshot should fail due to schema validation
        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        with pytest.raises(SchemaValidationError, match="Missing columns"):
            engine.create_snapshot("bad_schema", ds_config, [result.ingestion_id])

    def test_multiple_snapshots_coexist(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Create 2 snapshots; both should be accessible via registry."""
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)

        # First ingestion and snapshot
        csv1 = _make_trades_csv(tmp_data_dir / "trades_v1.csv", n=100)
        ing1 = _ingest_csv(tmp_data_dir, registry, csv1)

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        snap1 = engine.create_snapshot("test_trades", ds_config, [ing1])

        # Second ingestion (slightly different CSV with fewer rows)
        csv2 = _make_trades_csv(tmp_data_dir / "trades_v2.csv", n=50)
        ing2 = _ingest_csv(tmp_data_dir, registry, csv2)
        snap2 = engine.create_snapshot("test_trades", ds_config, [ing2])

        # Both snapshots should be listed
        snapshots = registry.list_snapshots("test_trades")
        snapshot_ids = {s.snapshot_id for s in snapshots}
        assert snap1 in snapshot_ids
        assert snap2 in snapshot_ids
        assert len(snapshots) == 2

        # Both should be retrievable
        assert registry.get_snapshot(snap1) is not None
        assert registry.get_snapshot(snap2) is not None

    def test_snapshot_parquet_readable(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Verify output Parquet is readable by Polars."""
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)
        csv_path = _make_trades_csv(tmp_data_dir / "trades.csv")
        ingestion_id = _ingest_csv(tmp_data_dir, registry, csv_path)

        engine = SnapshotEngine(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        snapshot_id = engine.create_snapshot("test_trades", ds_config, [ingestion_id])

        # Read back via Polars
        snap_record = registry.get_snapshot(snapshot_id)
        snap_dir = Path(snap_record.storage_path)
        parquet_files = sorted(snap_dir.glob("*.parquet"))
        assert len(parquet_files) >= 1

        df = pl.read_parquet(parquet_files[0])
        assert len(df) == 100
        assert "trade_id" in df.columns
        assert "price" in df.columns
        assert "timestamp" in df.columns

        # Verify types are correct after normalization
        assert df["price"].dtype == pl.Float64
        assert df["trade_id"].dtype == pl.Utf8
