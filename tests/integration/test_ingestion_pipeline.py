"""Integration tests for the ingestion pipeline.

Tests cover: CSV/Parquet ingest to raw storage, metadata logging,
failure handling (nonexistent files), and idempotency enforcement.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from adp.config import DatasetConfig, load_datasets_config
from adp.exceptions import IngestionError
from adp.ingestion import run_ingestion
from adp.ingestion.file import FileIngestionStrategy
from adp.metadata.registry import MetadataRegistry
from adp.processing.schema import compute_schema_hash_from_defs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_environment(
    tmp_data_dir: Path,
    sample_datasets_yaml: Path,
) -> tuple[MetadataRegistry, DatasetConfig]:
    """Create a registry on disk and register the test_trades dataset."""
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestIngestionPipeline:
    """Integration tests for the file ingestion pipeline."""

    def test_ingest_csv_to_raw_storage(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_trades_csv: Path,
    ) -> None:
        """Write sample_trades_df to CSV, ingest via FileIngestionStrategy,
        verify Parquet written to raw/ and metadata logged."""
        registry, _ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)

        strategy = FileIngestionStrategy(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        result = strategy.ingest(
            "test_trades",
            {
                "path": str(sample_trades_csv),
                "format": "csv",
                "encoding": "utf-8",
            },
        )

        # Verify Parquet file was written to the raw directory
        raw_dir = tmp_data_dir / "data" / "raw" / "test_trades"
        parquet_files = list(raw_dir.glob("*.parquet"))
        assert len(parquet_files) == 1, f"Expected 1 parquet file, found {len(parquet_files)}"

        # Verify the ingestion result
        assert result.row_count == 100
        assert result.source_type == "file"
        assert result.ingestion_id is not None

        # Verify Parquet content is readable and matches
        df = pl.read_parquet(parquet_files[0])
        assert len(df) == 100
        assert "trade_id" in df.columns
        assert "price" in df.columns

    def test_ingest_parquet_to_raw(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_trades_parquet: Path,
    ) -> None:
        """Ingest from a Parquet source file and verify raw storage."""
        registry, _ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)

        strategy = FileIngestionStrategy(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        result = strategy.ingest(
            "test_trades",
            {
                "path": str(sample_trades_parquet),
                "format": "parquet",
            },
        )

        raw_dir = tmp_data_dir / "data" / "raw" / "test_trades"
        parquet_files = list(raw_dir.glob("*.parquet"))
        assert len(parquet_files) == 1

        assert result.row_count == 100
        assert result.source_type == "file"

        df = pl.read_parquet(parquet_files[0])
        assert len(df) == 100
        assert set(df.columns) == {"trade_id", "symbol", "price", "quantity", "side", "timestamp"}

    def test_ingest_logs_metadata(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_trades_csv: Path,
    ) -> None:
        """Verify raw_ingestions table has correct entry after ingest."""
        registry, _ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)

        strategy = FileIngestionStrategy(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )
        result = strategy.ingest(
            "test_trades",
            {
                "path": str(sample_trades_csv),
                "format": "csv",
                "encoding": "utf-8",
            },
        )

        # Query the metadata registry directly
        ingestions = registry.list_ingestions("test_trades")
        assert len(ingestions) == 1

        record = ingestions[0]
        assert record.ingestion_id == result.ingestion_id
        assert record.dataset_name == "test_trades"
        assert record.source_type == "file"
        assert record.row_count == 100
        assert record.ingestion_timestamp is not None

    def test_ingest_failed_no_metadata(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
    ) -> None:
        """Ingest a nonexistent file, verify no orphan metadata is left."""
        registry, _ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)

        strategy = FileIngestionStrategy(
            data_dir=tmp_data_dir / "data",
            registry=registry,
        )

        with pytest.raises(IngestionError, match="File not found"):
            strategy.ingest(
                "test_trades",
                {
                    "path": "/nonexistent/path/to/file.csv",
                    "format": "csv",
                    "encoding": "utf-8",
                },
            )

        # Verify no orphan ingestion records were created
        ingestions = registry.list_ingestions("test_trades")
        assert len(ingestions) == 0

    def test_ingest_idempotency(
        self,
        tmp_data_dir: Path,
        sample_datasets_yaml: Path,
        sample_trades_csv: Path,
    ) -> None:
        """Ingest same file twice without force; second should raise IngestionError.

        Note: ``FileIngestionStrategy`` records the *source file path* as
        ``source_location``, so the idempotency check in ``run_ingestion``
        detects duplicates by matching the original source path.
        """
        registry, ds_config = _setup_environment(tmp_data_dir, sample_datasets_yaml)

        # First ingestion should succeed
        result1 = run_ingestion(
            dataset_name="test_trades",
            dataset_config=ds_config,
            data_dir=tmp_data_dir / "data",
            registry=registry,
            force=False,
            path_override=str(sample_trades_csv),
        )
        assert result1.row_count == 100

        # Same source path triggers the idempotency check.
        with pytest.raises(IngestionError, match="Already ingested"):
            run_ingestion(
                dataset_name="test_trades",
                dataset_config=ds_config,
                data_dir=tmp_data_dir / "data",
                registry=registry,
                force=False,
                path_override=str(sample_trades_csv),
            )

        # Only one ingestion record should exist
        ingestions = registry.list_ingestions("test_trades")
        assert len(ingestions) == 1
