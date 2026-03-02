"""Unit tests for adp.processing.schema — hash generation, model factory, validation."""

from __future__ import annotations

import re

import polars as pl
import pytest

from adp.config import ColumnDef, ColumnType
from adp.exceptions import SchemaValidationError
from adp.processing.schema import (
    build_schema_model,
    compute_schema_hash,
    compute_schema_hash_from_defs,
    validate_dataframe,
)

# ── Helpers ──────────────────────────────────────────────────

def _col(name: str, tp: str, nullable: bool = False) -> ColumnDef:
    return ColumnDef(name=name, type=ColumnType(tp), nullable=nullable)


BASIC_COLS = [
    {"name": "price", "type": "float", "nullable": False},
    {"name": "symbol", "type": "str", "nullable": False},
]


# ── compute_schema_hash ─────────────────────────────────────

@pytest.mark.unit
class TestComputeSchemaHash:
    def test_deterministic(self) -> None:
        h1 = compute_schema_hash(BASIC_COLS)
        h2 = compute_schema_hash(BASIC_COLS)
        assert h1 == h2

    def test_column_order_invariant(self) -> None:
        """Hash does not change when columns are given in different order."""
        reversed_cols = list(reversed(BASIC_COLS))
        assert compute_schema_hash(BASIC_COLS) == compute_schema_hash(reversed_cols)

    def test_changes_on_column_add(self) -> None:
        extra = [*BASIC_COLS, {"name": "volume", "type": "float", "nullable": False}]
        assert compute_schema_hash(BASIC_COLS) != compute_schema_hash(extra)

    def test_changes_on_type_change(self) -> None:
        modified = [
            {"name": "price", "type": "int", "nullable": False},
            {"name": "symbol", "type": "str", "nullable": False},
        ]
        assert compute_schema_hash(BASIC_COLS) != compute_schema_hash(modified)

    def test_sha256_format(self) -> None:
        h = compute_schema_hash(BASIC_COLS)
        assert re.fullmatch(r"[0-9a-f]{64}", h), f"Not a valid SHA-256 hex: {h}"

    def test_from_defs_matches_manual(self) -> None:
        defs = [_col("price", "float"), _col("symbol", "str")]
        dicts = [
            {"name": "price", "type": "float", "nullable": False},
            {"name": "symbol", "type": "str", "nullable": False},
        ]
        assert compute_schema_hash_from_defs(defs) == compute_schema_hash(dicts)


# ── build_schema_model ───────────────────────────────────────

@pytest.mark.unit
class TestBuildSchemaModel:
    def test_creates_model(self) -> None:
        Model = build_schema_model("trades", [_col("price", "float"), _col("symbol", "str")])
        assert Model.__name__ == "trades_Schema"

    def test_validates_good_data(self) -> None:
        Model = build_schema_model("trades", [_col("price", "float"), _col("symbol", "str")])
        instance = Model(price=42000.0, symbol="BTCUSDT")
        assert instance.price == 42000.0
        assert instance.symbol == "BTCUSDT"

    def test_rejects_bad_data(self) -> None:
        Model = build_schema_model("trades", [_col("price", "float")])
        with pytest.raises(ValueError):  # Pydantic ValidationError
            Model(price="not_a_float_and_not_convertible")

    def test_nullable_field_accepts_none(self) -> None:
        Model = build_schema_model("trades", [_col("price", "float", nullable=True)])
        instance = Model(price=None)
        assert instance.price is None


# ── validate_dataframe ───────────────────────────────────────

@pytest.mark.unit
class TestValidateDataframe:
    def test_missing_column(self) -> None:
        lf = pl.LazyFrame({"a": [1, 2]})
        cols = [_col("b", "int")]
        with pytest.raises(SchemaValidationError, match="Missing columns"):
            validate_dataframe(lf, cols)

    def test_null_in_non_nullable(self) -> None:
        lf = pl.LazyFrame({"price": [1.0, None, 3.0]})
        cols = [_col("price", "float", nullable=False)]
        with pytest.raises(SchemaValidationError, match="null values"):
            validate_dataframe(lf, cols)
