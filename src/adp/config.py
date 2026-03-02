"""Configuration loading and Pydantic models for datasets.yaml and features.yaml."""

from __future__ import annotations

import os
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, model_validator

from adp.exceptions import ConfigError

# ── Enums ─────────────────────────────────────────────────────

class SourceType(StrEnum):
    athena = "athena"
    file = "file"


class FileFormat(StrEnum):
    csv = "csv"
    json = "json"
    parquet = "parquet"
    txt = "txt"


class ColumnType(StrEnum):
    str_ = "str"
    int_ = "int"
    float_ = "float"
    bool_ = "bool"
    datetime_ = "datetime"
    date_ = "date"
    decimal_ = "Decimal"


class DedupStrategy(StrEnum):
    keep_first = "keep_first"
    keep_last = "keep_last"


# ── Dataset config models ────────────────────────────────────

class ColumnDef(BaseModel):
    name: str
    type: ColumnType
    nullable: bool = False
    source_timezone: str | None = None


class SchemaConfig(BaseModel):
    columns: list[ColumnDef]


class SourceConfig(BaseModel):
    type: SourceType
    path: str | None = None
    format: FileFormat | None = None
    database: str | None = None
    query: str | None = None
    s3_output: str | None = None
    encoding: str = "utf-8"


class ProcessingConfig(BaseModel):
    dedup_keys: list[str] | None = None
    dedup_strategy: DedupStrategy = DedupStrategy.keep_last
    normalization_version: str = "1.0"


class DatasetConfig(BaseModel):
    description: str = ""
    source: SourceConfig
    schema_def: SchemaConfig = Field(alias="schema")
    processing: ProcessingConfig = ProcessingConfig()

    model_config = {"populate_by_name": True}


class DatasetsConfig(BaseModel):
    datasets: dict[str, DatasetConfig]


# ── Feature config models ────────────────────────────────────

def _get_valid_feature_types() -> set[str]:
    """Get valid feature types from the strategy registry (single source of truth)."""
    from adp.features.strategies import STRATEGY_REGISTRY

    return set(STRATEGY_REGISTRY.keys())


class FeatureDefConfig(BaseModel):
    name: str
    type: str
    column: str | None = None
    window: int | None = None
    span: int | None = None
    price_column: str | None = None
    volume_column: str | None = None
    group_by: str | None = None
    description: str = ""

    @model_validator(mode="after")
    def _check_type(self) -> FeatureDefConfig:
        valid_types = _get_valid_feature_types()
        if self.type not in valid_types:
            valid = sorted(valid_types)
            raise ValueError(
                f"Unknown feature type: '{self.type}'. Valid: {valid}"
            )
        return self


class FeatureSetConfig(BaseModel):
    version: int
    description: str = ""
    features: list[FeatureDefConfig]


FeaturesConfig = dict[str, dict[str, FeatureSetConfig]]


# ── Loaders ───────────────────────────────────────────────────

def _resolve_config_dir() -> Path:
    env = os.environ.get("ADP_CONFIG_DIR")
    if env:
        return Path(env)
    return Path("config")


def load_datasets_config(config_path: Path | None = None) -> DatasetsConfig:
    """Load and validate datasets.yaml."""
    path = config_path or (_resolve_config_dir() / "datasets.yaml")
    if not path.exists():
        raise ConfigError(f"Dataset config not found: {path}")
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Malformed YAML in {path}: {e}") from e
    if not isinstance(raw, dict) or "datasets" not in raw:
        raise ConfigError(f"Expected top-level 'datasets' key in {path}")
    try:
        return DatasetsConfig.model_validate(raw)
    except Exception as e:
        raise ConfigError(f"Validation error in {path}: {e}") from e


def load_features_config(config_path: Path | None = None) -> FeaturesConfig:
    """Load and validate features.yaml."""
    path = config_path or (_resolve_config_dir() / "features.yaml")
    if not path.exists():
        raise ConfigError(f"Features config not found: {path}")
    try:
        with open(path) as f:
            raw: dict[str, Any] = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigError(f"Malformed YAML in {path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"Expected dict at top level in {path}")

    result: FeaturesConfig = {}
    try:
        for dataset_name, feature_sets in raw.items():
            result[dataset_name] = {}
            for fs_name, fs_raw in feature_sets.items():
                result[dataset_name][fs_name] = FeatureSetConfig.model_validate(fs_raw)
    except Exception as e:
        raise ConfigError(f"Validation error in {path}: {e}") from e
    return result
