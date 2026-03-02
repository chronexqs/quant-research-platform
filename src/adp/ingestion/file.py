"""File ingestion strategy — CSV, JSON, Parquet, TXT."""

from __future__ import annotations

import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from adp.exceptions import IngestionError
from adp.ingestion.strategies import IngestionResult
from adp.metadata.registry import MetadataRegistry
from adp.storage.snapshot import generate_ingestion_id

logger = logging.getLogger(__name__)


class FileIngestionStrategy:
    """Ingest data from local files (CSV, JSON, Parquet, TXT).

    Reads a file from the local filesystem, converts it to Parquet for
    uniform downstream processing, and logs the event in the metadata
    registry.

    Attributes:
        data_dir: Root directory under which raw Parquet snapshots are
            stored (``<data_dir>/raw/<dataset>/``).
        registry: Metadata registry for recording ingestion events and
            generating unique ingestion IDs.
    """

    def __init__(self, data_dir: Path, registry: MetadataRegistry) -> None:
        self.data_dir = data_dir
        self.registry = registry

    def ingest(self, dataset_name: str, config: dict[str, Any]) -> IngestionResult:
        """Read a local file and store its contents as a raw Parquet snapshot.

        The file format is auto-detected from the extension when not
        explicitly provided in *config*. For Parquet sources the file is
        copied directly; all other formats are read into a Polars
        DataFrame and then serialized to Parquet.

        Args:
            dataset_name: Logical dataset name used for directory layout
                and metadata.
            config: Configuration dict with the following keys:

                - ``"path"`` (str): Path to the source file (required).
                - ``"format"`` (str | None): Explicit format override
                  (``"csv"``, ``"json"``, ``"parquet"``, ``"txt"``).
                - ``"encoding"`` (str): Character encoding for text
                  formats (default ``"utf-8"``).

        Returns:
            An ``IngestionResult`` with the generated ingestion ID,
            output path, row count, and timestamp.

        Raises:
            IngestionError: If the source file does not exist, is empty,
                or cannot be read.
        """
        source_path = Path(config.get("path", ""))
        if not source_path.exists():
            raise IngestionError(f"File not found: {source_path}")

        file_format = config.get("format", source_path.suffix.lstrip("."))
        encoding = config.get("encoding", "utf-8")

        # Read into DataFrame
        df = self._read_file(source_path, file_format, encoding)
        if df.is_empty():
            raise IngestionError(f"File is empty: {source_path}")

        row_count = len(df)

        # Generate ID and write raw Parquet
        ingestion_id = generate_ingestion_id(dataset_name, self.registry)
        raw_dir = self.data_dir / "raw" / dataset_name
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{ingestion_id}.parquet"

        if file_format == "parquet":
            shutil.copy2(source_path, raw_path)
        else:
            df.write_parquet(raw_path)

        # Log in metadata
        self.registry.log_ingestion(
            ingestion_id=ingestion_id,
            dataset_name=dataset_name,
            source_type="file",
            source_location=str(source_path),
            row_count=row_count,
        )

        logger.info(
            "Ingested %s from %s (%d rows) -> %s",
            dataset_name,
            source_path,
            row_count,
            raw_path,
        )

        return IngestionResult(
            ingestion_id=ingestion_id,
            raw_data_path=str(raw_path),
            row_count=row_count,
            source_type="file",
            source_location=str(source_path),
            ingestion_timestamp=datetime.now(UTC),
        )

    def _read_file(self, path: Path, fmt: str, encoding: str) -> pl.DataFrame:
        """Read a file into a Polars DataFrame based on its format.

        Args:
            path: Filesystem path to the source file.
            fmt: File format identifier (``"csv"``, ``"json"``,
                ``"parquet"``, or ``"txt"``). ``"txt"`` is treated as
                tab-separated CSV.
            encoding: Character encoding for text-based formats.

        Returns:
            A ``pl.DataFrame`` containing the file's data.

        Raises:
            IngestionError: If the format is unsupported or reading
                fails for any reason.
        """
        try:
            if fmt == "csv":
                return pl.read_csv(path, encoding=encoding, infer_schema_length=10000)
            elif fmt == "json":
                return pl.read_json(path)
            elif fmt == "parquet":
                return pl.read_parquet(path)
            elif fmt == "txt":
                return pl.read_csv(path, separator="\t", encoding=encoding)
            else:
                raise IngestionError(f"Unsupported file format: {fmt}")
        except IngestionError:
            raise
        except Exception as e:
            raise IngestionError(f"Failed to read {path}: {e}") from e
