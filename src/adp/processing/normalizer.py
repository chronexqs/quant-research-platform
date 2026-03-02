"""Normalization pipeline — composable steps for schema validation, timezone, dedup."""

from __future__ import annotations

import logging
from collections.abc import Callable

import polars as pl

from adp.config import ColumnDef, ColumnType, ProcessingConfig
from adp.processing.dedup import deduplicate
from adp.processing.schema import validate_dataframe

logger = logging.getLogger(__name__)

# A single normalization step: a callable that transforms a LazyFrame in place
# within the pipeline. Steps are executed sequentially by ``NormalizationPipeline``.
NormalizationStep = Callable[[pl.LazyFrame], pl.LazyFrame]


def normalize_timezones(
    lf: pl.LazyFrame,
    columns: list[ColumnDef],
) -> pl.LazyFrame:
    """Convert all datetime columns to UTC.

    Handles both tz-naive (typical of CSV sources) and tz-aware (typical
    of Parquet sources) inputs. Each column's ``source_timezone`` from
    its ``ColumnDef`` determines how tz-naive values are interpreted.

    Args:
        lf: Input LazyFrame whose datetime columns will be normalized.
        columns: Column definitions. Only columns with
            ``ColumnType.datetime_`` are processed; the rest are
            ignored.

    Returns:
        A new LazyFrame with all datetime columns converted to UTC.
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
    """Composable pipeline that chains normalization steps sequentially.

    Steps are executed in order against a ``pl.LazyFrame``. The default
    pipeline built via ``from_config`` applies schema validation,
    timezone normalization, and deduplication.

    Attributes:
        steps: Ordered list of ``NormalizationStep`` callables to apply.
    """

    def __init__(self, steps: list[NormalizationStep] | None = None) -> None:
        self.steps = steps or []

    @classmethod
    def from_config(
        cls,
        columns: list[ColumnDef],
        processing: ProcessingConfig,
    ) -> NormalizationPipeline:
        """Build the default three-step pipeline from dataset configuration.

        The pipeline applies, in order:

        1. Schema validation and type casting.
        2. Timezone normalization to UTC.
        3. Deduplication based on configured keys and strategy.

        Args:
            columns: Column definitions for schema validation and
                timezone handling.
            processing: Processing configuration containing dedup keys
                and strategy.

        Returns:
            A fully configured ``NormalizationPipeline`` instance.
        """
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
        """Execute all steps sequentially, returning the final LazyFrame.

        Args:
            lf: Input LazyFrame to transform.

        Returns:
            The LazyFrame after all normalization steps have been
            applied.
        """
        for i, step in enumerate(self.steps):
            logger.debug("Running normalization step %d/%d", i + 1, len(self.steps))
            lf = step(lf)
        return lf
