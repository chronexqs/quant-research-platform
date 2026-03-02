"""Athena ingestion strategy — awswrangler-based."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from adp.exceptions import IngestionError
from adp.ingestion.strategies import IngestionResult
from adp.metadata.registry import MetadataRegistry
from adp.storage.snapshot import generate_ingestion_id

logger = logging.getLogger(__name__)


class AthenaIngestionStrategy:
    """Ingest data from AWS Athena via ``awswrangler``.

    Executes a SQL query against an Athena database, converts the result
    to a Polars DataFrame, writes it as raw Parquet, and records the
    event in the metadata registry.

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
        """Execute an Athena SQL query and store the result as raw Parquet.

        Uses ``awswrangler`` (imported lazily) to run the query against
        AWS Athena. The resulting pandas DataFrame is converted to
        Polars, written to Parquet, and logged in the metadata registry.

        Args:
            dataset_name: Logical dataset name used for directory layout
                and metadata.
            config: Configuration dict with the following keys:

                - ``"query"`` (str): SQL query to execute (required).
                - ``"database"`` (str): Athena database name (required).
                - ``"s3_output"`` (str | None): S3 path for Athena
                  query results staging.

        Returns:
            An ``IngestionResult`` with the generated ingestion ID,
            output path, row count, and timestamp.

        Raises:
            IngestionError: If ``awswrangler`` is not installed, required
                config keys are missing, the query fails, or the query
                returns zero rows.
        """
        try:
            import awswrangler as wr
        except ImportError as e:
            raise IngestionError("awswrangler is required for Athena ingestion") from e

        query = config.get("query")
        database = config.get("database")
        s3_output = config.get("s3_output")

        if not query:
            raise IngestionError("Athena config requires 'query'")
        if not database:
            raise IngestionError("Athena config requires 'database'")

        try:
            pandas_df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=s3_output,
                ctas_approach=False,
            )
        except Exception as e:
            raise IngestionError(f"Athena query failed: {e}") from e

        df = pl.from_pandas(pandas_df)
        if df.is_empty():
            raise IngestionError("Athena query returned no rows")

        row_count = len(df)

        # Generate ID and write raw Parquet
        ingestion_id = generate_ingestion_id(dataset_name, self.registry)
        raw_dir = self.data_dir / "raw" / dataset_name
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{ingestion_id}.parquet"
        df.write_parquet(raw_path)

        # Log in metadata
        self.registry.log_ingestion(
            ingestion_id=ingestion_id,
            dataset_name=dataset_name,
            source_type="athena",
            source_location=query,
            row_count=row_count,
        )

        logger.info(
            "Ingested %s from Athena (%d rows) -> %s",
            dataset_name,
            row_count,
            raw_path,
        )

        return IngestionResult(
            ingestion_id=ingestion_id,
            raw_data_path=str(raw_path),
            row_count=row_count,
            source_type="athena",
            source_location=query,
            ingestion_timestamp=datetime.now(UTC),
        )
