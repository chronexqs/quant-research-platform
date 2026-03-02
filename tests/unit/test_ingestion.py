"""Unit tests for adp.ingestion.file — FileIngestionStrategy and IngestionResult."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from adp.exceptions import IngestionError
from adp.ingestion.file import FileIngestionStrategy
from adp.ingestion.strategies import IngestionResult, IngestionStrategy
from adp.metadata.registry import MetadataRegistry


@pytest.mark.unit
class TestFileIngestionCSV:
    def test_ingest_csv(
        self,
        tmp_data_dir: Path,
        in_memory_registry: MetadataRegistry,
        sample_trades_csv: Path,
    ) -> None:
        in_memory_registry.register_dataset("test_trades", schema_hash="h")
        strategy = FileIngestionStrategy(tmp_data_dir, in_memory_registry)
        result = strategy.ingest(
            "test_trades",
            {"path": str(sample_trades_csv), "format": "csv"},
        )
        assert isinstance(result, IngestionResult)
        assert result.row_count == 100
        assert result.source_type == "file"
        # Raw parquet should exist
        assert Path(result.raw_data_path).exists()


@pytest.mark.unit
class TestFileIngestionParquet:
    def test_ingest_parquet(
        self,
        tmp_data_dir: Path,
        in_memory_registry: MetadataRegistry,
        sample_trades_parquet: Path,
    ) -> None:
        in_memory_registry.register_dataset("test_trades", schema_hash="h")
        strategy = FileIngestionStrategy(tmp_data_dir, in_memory_registry)
        result = strategy.ingest(
            "test_trades",
            {"path": str(sample_trades_parquet), "format": "parquet"},
        )
        assert result.row_count == 100
        assert Path(result.raw_data_path).exists()


@pytest.mark.unit
class TestFileIngestionErrors:
    def test_missing_file(
        self, tmp_data_dir: Path, in_memory_registry: MetadataRegistry
    ) -> None:
        strategy = FileIngestionStrategy(tmp_data_dir, in_memory_registry)
        with pytest.raises(IngestionError, match="File not found"):
            strategy.ingest("test", {"path": "/nonexistent/file.csv", "format": "csv"})

    def test_empty_file(
        self, tmp_data_dir: Path, in_memory_registry: MetadataRegistry
    ) -> None:
        in_memory_registry.register_dataset("test_empty", schema_hash="h")
        empty_csv = tmp_data_dir / "empty.csv"
        empty_csv.write_text("col_a,col_b\n")
        strategy = FileIngestionStrategy(tmp_data_dir, in_memory_registry)
        with pytest.raises(IngestionError, match="empty"):
            strategy.ingest("test_empty", {"path": str(empty_csv), "format": "csv"})


@pytest.mark.unit
class TestIngestionResult:
    def test_fields(self) -> None:
        now = datetime(2026, 1, 1)
        result = IngestionResult(
            ingestion_id="ing_001",
            raw_data_path="/data/raw/ing_001.parquet",
            row_count=50,
            source_type="file",
            source_location="/data/input.csv",
            ingestion_timestamp=now,
        )
        assert result.ingestion_id == "ing_001"
        assert result.row_count == 50
        assert result.source_type == "file"
        assert result.source_location == "/data/input.csv"
        assert result.ingestion_timestamp == now


@pytest.mark.unit
class TestIngestionProtocol:
    def test_file_strategy_conforms_to_protocol(
        self, tmp_data_dir: Path, in_memory_registry: MetadataRegistry
    ) -> None:
        strategy = FileIngestionStrategy(tmp_data_dir, in_memory_registry)
        assert isinstance(strategy, IngestionStrategy)
