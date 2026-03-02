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

# Mapping from ``ColumnType`` enum values to native Python types.
# Used by ``build_schema_model`` to construct Pydantic validation models.
TYPE_MAP: dict[ColumnType, type] = {
    ColumnType.str_: str,
    ColumnType.int_: int,
    ColumnType.float_: float,
    ColumnType.bool_: bool,
    ColumnType.datetime_: datetime,
    ColumnType.date_: date,
    ColumnType.decimal_: Decimal,
}

# Mapping from ``ColumnType`` enum values to Polars data types.
# Used by ``cast_dataframe`` and ``get_expected_polars_schema`` for type
# coercion and validation. Note: ``Decimal`` maps to ``Float64`` because
# Polars does not have a native decimal type.
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
    """Compute a deterministic SHA-256 hash of a column specification.

    Columns are sorted by name before hashing so that ordering
    differences do not produce different hashes.

    Args:
        columns: List of column dicts, each containing at minimum
            ``"name"``, ``"type"``, and ``"nullable"`` keys.

    Returns:
        A lowercase hex-encoded SHA-256 digest string.
    """
    normalized = sorted(columns, key=lambda c: c["name"])
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_schema_hash_from_defs(columns: list[ColumnDef]) -> str:
    """Compute a deterministic schema hash from ``ColumnDef`` objects.

    Convenience wrapper around ``compute_schema_hash`` that converts
    ``ColumnDef`` instances to plain dicts first.

    Args:
        columns: List of ``ColumnDef`` configuration objects.

    Returns:
        A lowercase hex-encoded SHA-256 digest string.
    """
    col_dicts = [{"name": c.name, "type": c.type.value, "nullable": c.nullable} for c in columns]
    return compute_schema_hash(col_dicts)


def build_schema_model(
    dataset_name: str,
    columns: list[ColumnDef],
) -> type:
    """Dynamically create a Pydantic model from column definitions.

    The generated model can be used for row-level validation. Nullable
    columns default to ``None``; non-nullable columns are required.

    Args:
        dataset_name: Used as the prefix for the generated model class
            name (e.g. ``"trades"`` produces ``trades_Schema``).
        columns: Column definitions specifying field names, types, and
            nullability.

    Returns:
        A Pydantic ``BaseModel`` subclass with fields matching the
        column definitions.
    """
    fields: dict[str, Any] = {}
    for col in columns:
        python_type = TYPE_MAP[col.type]
        if col.nullable:
            fields[col.name] = (python_type | None, None)
        else:
            fields[col.name] = (python_type, ...)
    return create_model(f"{dataset_name}_Schema", **fields)  # type: ignore[no-any-return]


def get_expected_polars_schema(columns: list[ColumnDef]) -> dict[str, pl.DataType]:
    """Build a mapping of column names to expected Polars data types.

    Args:
        columns: Column definitions from dataset configuration.

    Returns:
        Dict mapping column name to its expected ``pl.DataType``.
    """
    return {col.name: POLARS_TYPE_MAP[col.type] for col in columns}


def cast_dataframe(lf: pl.LazyFrame, columns: list[ColumnDef]) -> pl.LazyFrame:
    """Cast LazyFrame columns to their expected Polars data types.

    Handles both string-typed columns (typical of CSV ingestion) and
    already-typed columns (typical of Parquet ingestion) gracefully.
    String-to-datetime and string-to-date conversions use dedicated
    parsing expressions; all other casts use ``pl.Expr.cast``.

    Args:
        lf: Input LazyFrame to cast.
        columns: Column definitions specifying target types.

    Returns:
        A new LazyFrame with columns cast to the types defined in
        ``POLARS_TYPE_MAP``. Columns already matching their target type
        are left untouched.
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
            cast_exprs.append(pl.col(col.name).str.to_datetime(strict=False).alias(col.name))
        elif col.type == ColumnType.date_ and actual_dtype == pl.Utf8():
            cast_exprs.append(pl.col(col.name).str.to_date(strict=False).alias(col.name))
        else:
            cast_exprs.append(pl.col(col.name).cast(target_dtype).alias(col.name))
    if cast_exprs:
        return lf.with_columns(cast_exprs)
    return lf


def validate_dataframe(
    lf: pl.LazyFrame,
    columns: list[ColumnDef],
) -> pl.LazyFrame:
    """Validate and coerce a LazyFrame against schema definitions.

    Performs the following checks in order:

    1. Verify all expected columns exist in the LazyFrame.
    2. Cast columns to their expected Polars types via ``cast_dataframe``.
    3. Enforce non-nullable constraints by counting nulls across the
       full dataset.

    Args:
        lf: Input LazyFrame to validate.
        columns: Column definitions specifying expected names, types,
            and nullability constraints.

    Returns:
        The validated and type-cast LazyFrame, ready for downstream
        processing.

    Raises:
        SchemaValidationError: If required columns are missing or
            non-nullable columns contain null values.
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
                    f"Column '{col.name}' has {count} null values but is not nullable"
                )

    return lf
