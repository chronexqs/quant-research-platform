"""Ingestion strategy protocol and registry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class IngestionResult:
    """Result of a data ingestion operation."""

    ingestion_id: str
    raw_data_path: str
    row_count: int
    source_type: str
    source_location: str
    ingestion_timestamp: datetime


@runtime_checkable
class IngestionStrategy(Protocol):
    """Protocol for data ingestion strategies."""

    def ingest(self, dataset_name: str, config: dict[str, Any]) -> IngestionResult:
        """Extract data from source and store as raw Parquet."""
        ...
