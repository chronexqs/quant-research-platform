"""Ingestion strategy protocol and result container.

Defines the ``IngestionStrategy`` protocol that all concrete ingestion
backends must satisfy, and the ``IngestionResult`` dataclass returned
after a successful ingestion.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class IngestionResult:
    """Immutable record produced by a successful ingestion operation.

    Attributes:
        ingestion_id: Unique identifier for this ingestion run
            (e.g. ``"trades_ing_003"``).
        raw_data_path: Absolute path to the written raw Parquet file.
        row_count: Number of rows ingested.
        source_type: Strategy key that performed the ingestion
            (e.g. ``"file"``, ``"athena"``).
        source_location: Human-readable source reference (file path or
            SQL query string).
        ingestion_timestamp: UTC timestamp of when the ingestion
            completed.
    """

    ingestion_id: str
    raw_data_path: str
    row_count: int
    source_type: str
    source_location: str
    ingestion_timestamp: datetime


@runtime_checkable
class IngestionStrategy(Protocol):
    """Protocol that all ingestion strategy implementations must satisfy.

    Concrete classes (e.g. ``FileIngestionStrategy``,
    ``AthenaIngestionStrategy``) must implement the ``ingest`` method.
    The protocol is ``@runtime_checkable`` so that ``isinstance`` tests
    work at runtime.
    """

    def ingest(self, dataset_name: str, config: dict[str, Any]) -> IngestionResult:
        """Extract data from an external source and store it as raw Parquet.

        Args:
            dataset_name: Logical dataset name used for namespacing
                storage paths and metadata entries.
            config: Strategy-specific configuration dict. Common keys
                include ``"path"``, ``"format"``, ``"query"``,
                ``"database"``, and ``"s3_output"``.

        Returns:
            An ``IngestionResult`` describing the completed ingestion.
        """
        ...
