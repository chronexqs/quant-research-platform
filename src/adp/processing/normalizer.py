"""Normalization pipeline — composable steps for schema validation, timezone, dedup."""

from __future__ import annotations

import logging
from collections.abc import Callable

import polars as pl

from adp.config import ColumnDef, ColumnType, ProcessingConfig
from adp.processing.dedup import deduplicate
from adp.processing.schema import validate_dataframe

logger = logging.getLogger(__name__)

NormalizationStep = Callable[[pl.LazyFrame], pl.LazyFrame]


def normalize_timezones(
    lf: pl.LazyFrame,
    columns: list[ColumnDef],
) -> pl.LazyFrame:
    """Convert all datetime columns to UTC.

    Handles both tz-naive (from CSV) and tz-aware (from Parquet) inputs.
    """
    schema = lf.collect_schema()
    for col in columns:
        if col.type != ColumnType.datetime_:
            continue
        col_dtype = schema[col.name]
        source_tz = col.source_timezone or "UTC"

        # Check if column already has timezone info
        is_tz_aware = isinstance(col_dtype, pl.Datetime) and col_dtype.time_zone is not None

        if is_tz_aware:
            # Already tz-aware: just convert to UTC
            lf = lf.with_columns(pl.col(col.name).dt.convert_time_zone("UTC"))
        elif source_tz == "UTC":
            # tz-naive, stamp as UTC
            lf = lf.with_columns(pl.col(col.name).dt.replace_time_zone("UTC"))
        else:
            # tz-naive: stamp with source tz then convert to UTC
            lf = lf.with_columns(
                pl.col(col.name).dt.replace_time_zone(source_tz).dt.convert_time_zone("UTC")
            )
    return lf


class NormalizationPipeline:
    """Composable pipeline that chains normalization steps."""

    def __init__(self, steps: list[NormalizationStep] | None = None) -> None:
        self.steps = steps or []

    @classmethod
    def from_config(
        cls,
        columns: list[ColumnDef],
        processing: ProcessingConfig,
    ) -> NormalizationPipeline:
        """Build the default pipeline from dataset configuration."""
        steps: list[NormalizationStep] = []

        # Step 1: Schema validation and type casting
        _cols = columns
        steps.append(lambda lf: validate_dataframe(lf, _cols))

        # Step 2: Timezone normalization
        steps.append(lambda lf: normalize_timezones(lf, _cols))

        # Step 3: Deduplication
        _proc = processing
        steps.append(lambda lf: deduplicate(lf, _proc.dedup_keys, _proc.dedup_strategy.value))

        return cls(steps)

    def run(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Execute all steps sequentially, returning the final LazyFrame."""
        for i, step in enumerate(self.steps):
            logger.debug("Running normalization step %d/%d", i + 1, len(self.steps))
            lf = step(lf)
        return lf
