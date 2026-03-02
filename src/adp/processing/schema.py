"""Schema hashing, dynamic Pydantic model factory, and DataFrame validation."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import polars as pl
from pydantic import create_model

from adp.config import ColumnDef, ColumnType
from adp.exceptions import SchemaValidationError

# Map config types to Python types
TYPE_MAP: dict[ColumnType, type] = {
    ColumnType.str_: str,
    ColumnType.int_: int,
    ColumnType.float_: float,
    ColumnType.bool_: bool,
    ColumnType.datetime_: datetime,
    ColumnType.date_: date,
    ColumnType.decimal_: Decimal,
}

# Map config types to Polars dtypes
POLARS_TYPE_MAP: dict[ColumnType, pl.DataType] = {
    ColumnType.str_: pl.Utf8(),
    ColumnType.int_: pl.Int64(),
    ColumnType.float_: pl.Float64(),
    ColumnType.bool_: pl.Boolean(),
    ColumnType.datetime_: pl.Datetime("us"),
    ColumnType.date_: pl.Date(),
    ColumnType.decimal_: pl.Float64(),  # Polars doesn't have native Decimal
}


def compute_schema_hash(columns: list[dict[str, Any]]) -> str:
    """Compute SHA-256 hash of a column specification.

    Columns are sorted by name for determinism, then JSON-serialized
    with sort_keys=True.
    """
    normalized = sorted(columns, key=lambda c: c["name"])
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_schema_hash_from_defs(columns: list[ColumnDef]) -> str:
    """Compute schema hash from ColumnDef objects."""
    col_dicts = [
        {"name": c.name, "type": c.type.value, "nullable": c.nullable}
        for c in columns
    ]
    return compute_schema_hash(col_dicts)


def build_schema_model(
    dataset_name: str,
    columns: list[ColumnDef],
) -> type:
    """Dynamically create a Pydantic model from column definitions."""
    fields: dict[str, Any] = {}
    for col in columns:
        python_type = TYPE_MAP[col.type]
        if col.nullable:
            fields[col.name] = (python_type | None, None)
        else:
            fields[col.name] = (python_type, ...)
    return create_model(f"{dataset_name}_Schema", **fields)  # type: ignore[no-any-return]


def get_expected_polars_schema(columns: list[ColumnDef]) -> dict[str, pl.DataType]:
    """Get expected Polars column dtypes from config."""
    return {col.name: POLARS_TYPE_MAP[col.type] for col in columns}


def cast_dataframe(lf: pl.LazyFrame, columns: list[ColumnDef]) -> pl.LazyFrame:
    """Cast LazyFrame columns to expected Polars types.

    Handles both string-typed columns (from CSV) and already-typed
    columns (from Parquet) gracefully.
    """
    schema = lf.collect_schema()
    cast_exprs = []
    for col in columns:
        target_dtype = POLARS_TYPE_MAP[col.type]
        actual_dtype = schema.get(col.name)

        # Skip if already the target type
        if actual_dtype == target_dtype:
            continue

        if col.type == ColumnType.datetime_ and actual_dtype == pl.Utf8():
            cast_exprs.append(
                pl.col(col.name).str.to_datetime(strict=False).alias(col.name)
            )
        elif col.type == ColumnType.date_ and actual_dtype == pl.Utf8():
            cast_exprs.append(
                pl.col(col.name).str.to_date(strict=False).alias(col.name)
            )
        else:
            cast_exprs.append(
                pl.col(col.name).cast(target_dtype).alias(col.name)
            )
    if cast_exprs:
        return lf.with_columns(cast_exprs)
    return lf


def validate_dataframe(
    lf: pl.LazyFrame,
    columns: list[ColumnDef],
) -> pl.LazyFrame:
    """Validate a LazyFrame against schema definitions.

    1. Check all expected columns exist
    2. Cast columns to expected types
    3. Validate non-nullable constraints on the full dataset

    Returns the casted LazyFrame.
    """
    # Check columns exist
    schema = lf.collect_schema()
    expected_names = {c.name for c in columns}
    actual_names = set(schema.names())
    missing = expected_names - actual_names
    if missing:
        raise SchemaValidationError(
            f"Missing columns: {sorted(missing)}. Available: {sorted(actual_names)}"
        )

    # Cast types
    lf = cast_dataframe(lf, columns)

    # Validate non-nullable constraints on the full dataset
    non_nullable = [c for c in columns if not c.nullable]
    if non_nullable:
        null_counts = lf.select(
            [pl.col(c.name).null_count().alias(c.name) for c in non_nullable]
        ).collect()
        for col in non_nullable:
            count = null_counts[col.name][0]
            if count > 0:
                raise SchemaValidationError(
                    f"Column '{col.name}' has {count} null values "
                    "but is not nullable"
                )

    return lf
