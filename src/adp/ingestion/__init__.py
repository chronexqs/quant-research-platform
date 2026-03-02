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

STRATEGY_REGISTRY: dict[str, type] = {
    "file": FileIngestionStrategy,
    "athena": AthenaIngestionStrategy,
}


def get_strategy(
    source_type: str,
    data_dir: Path,
    registry: MetadataRegistry,
) -> IngestionStrategy:
    """Get an ingestion strategy instance by source type."""
    cls = STRATEGY_REGISTRY.get(source_type)
    if cls is None:
        valid = sorted(STRATEGY_REGISTRY)
        raise IngestionError(
            f"Unknown source type: '{source_type}'. Valid: {valid}"
        )
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
    """Run ingestion for a dataset.

    Args:
        dataset_name: Name of the dataset.
        dataset_config: Dataset configuration.
        data_dir: Root data directory.
        registry: Metadata registry.
        force: If True, skip idempotency check.
        source_override: Override source type from config.
        path_override: Override file path from config.
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
                dataset_name, config["path"], existing.ingestion_id,
            )
            raise IngestionError(
                f"Already ingested from '{config['path']}'. Use --force to re-ingest."
            )

    strategy = get_strategy(source_type, data_dir, registry)
    return strategy.ingest(dataset_name, config)
