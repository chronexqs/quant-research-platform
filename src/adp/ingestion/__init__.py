"""Data ingestion layer — orchestrator and strategy registry."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from adp.config import DatasetConfig
from adp.exceptions import IngestionError
from adp.ingestion.athena import AthenaIngestionStrategy
from adp.ingestion.file import FileIngestionStrategy
from adp.ingestion.strategies import IngestionResult, IngestionStrategy
from adp.metadata.registry import MetadataRegistry

logger = logging.getLogger(__name__)

# Registry mapping source type identifiers to their strategy implementation classes.
# To add a new ingestion source, implement ``IngestionStrategy`` and register it here.
STRATEGY_REGISTRY: dict[str, type] = {
    "file": FileIngestionStrategy,
    "athena": AthenaIngestionStrategy,
}


def get_strategy(
    source_type: str,
    data_dir: Path,
    registry: MetadataRegistry,
) -> IngestionStrategy:
    """Look up and instantiate an ingestion strategy by source type name.

    Args:
        source_type: Key into ``STRATEGY_REGISTRY`` (e.g. ``"file"``,
            ``"athena"``).
        data_dir: Root directory for raw data storage.
        registry: Metadata registry used to record ingestion events.

    Returns:
        An instantiated strategy object satisfying the
        ``IngestionStrategy`` protocol.

    Raises:
        IngestionError: If ``source_type`` is not found in
            ``STRATEGY_REGISTRY``.
    """
    cls = STRATEGY_REGISTRY.get(source_type)
    if cls is None:
        valid = sorted(STRATEGY_REGISTRY)
        raise IngestionError(f"Unknown source type: '{source_type}'. Valid: {valid}")
    return cls(data_dir=data_dir, registry=registry)  # type: ignore[no-any-return]


def run_ingestion(
    dataset_name: str,
    dataset_config: DatasetConfig,
    data_dir: Path,
    registry: MetadataRegistry,
    force: bool = False,
    source_override: str | None = None,
    path_override: str | None = None,
) -> IngestionResult:
    """Run the full ingestion workflow for a single dataset.

    Resolves the appropriate strategy, applies optional overrides, performs
    an idempotency check (unless *force* is set), and delegates to the
    strategy's ``ingest`` method.

    Args:
        dataset_name: Logical name of the dataset (used as a namespace
            in storage and metadata).
        dataset_config: Parsed dataset configuration containing source
            details and processing parameters.
        data_dir: Root data directory where raw Parquet files are stored.
        registry: Metadata registry for idempotency lookups and
            ingestion logging.
        force: If ``True``, bypass the duplicate-ingestion guard and
            re-ingest unconditionally.
        source_override: If provided, use this source type instead of
            the one specified in *dataset_config*.
        path_override: If provided, use this file path instead of the
            one specified in *dataset_config*.

    Returns:
        An ``IngestionResult`` containing the ingestion ID, storage
        path, row count, and timestamp.

    Raises:
        IngestionError: If the dataset has already been ingested from
            the same source path and *force* is ``False``, or if the
            underlying strategy raises an ingestion failure.
    """
    source_type = source_override or dataset_config.source.type.value
    config: dict[str, Any] = {
        "path": path_override or dataset_config.source.path,
        "format": dataset_config.source.format.value if dataset_config.source.format else None,
        "encoding": dataset_config.source.encoding,
        "query": dataset_config.source.query,
        "database": dataset_config.source.database,
        "s3_output": dataset_config.source.s3_output,
    }

    # Idempotency check
    if not force and config.get("path"):
        existing = registry.find_ingestion_by_source(dataset_name, str(config["path"]))
        if existing:
            logger.warning(
                "Dataset '%s' already ingested from '%s' "
                "(ingestion_id=%s). Use --force to re-ingest.",
                dataset_name,
                config["path"],
                existing.ingestion_id,
            )
            raise IngestionError(
                f"Already ingested from '{config['path']}'. Use --force to re-ingest."
            )

    strategy = get_strategy(source_type, data_dir, registry)
    return strategy.ingest(dataset_name, config)
