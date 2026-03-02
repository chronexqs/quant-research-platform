"""Configuration loading and Pydantic models for datasets.yaml and features.yaml.

This module defines the strongly-typed schema for two YAML configuration
files consumed by the ADP pipeline:

* **datasets.yaml** -- describes source locations, file formats, column
  schemas, and processing options for each registered dataset.
* **features.yaml** -- declares named feature sets, each containing an
  ordered list of feature definitions that reference strategy types from
  :mod:`adp.features.strategies`.

The two public loader functions, :func:`load_datasets_config` and
:func:`load_features_config`, read, parse, and validate these files,
raising :class:`~adp.exceptions.ConfigError` on any failure.
"""

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
    """Supported data-source back-ends for dataset ingestion."""

    athena = "athena"
    file = "file"


class FileFormat(StrEnum):
    """File formats recognised by the file-based ingestion reader."""

    csv = "csv"
    json = "json"
    parquet = "parquet"
    txt = "txt"


class ColumnType(StrEnum):
    """Logical column types used in dataset schema definitions.

    Each member maps to a string token that appears in ``datasets.yaml``
    and is translated to the appropriate Polars dtype during normalisation.
    """

    str_ = "str"
    int_ = "int"
    float_ = "float"
    bool_ = "bool"
    datetime_ = "datetime"
    date_ = "date"
    decimal_ = "Decimal"


class DedupStrategy(StrEnum):
    """Strategy for resolving duplicate rows during processing."""

    keep_first = "keep_first"
    keep_last = "keep_last"


# ── Dataset config models ────────────────────────────────────


class ColumnDef(BaseModel):
    """Definition of a single column within a dataset schema.

    Attributes:
        name: Column name as it appears in the raw source data.
        type: Logical data type (maps to a Polars dtype during normalisation).
        nullable: Whether the column may contain null values.
        source_timezone: IANA timezone string (e.g. ``"UTC"``) applied when
            casting datetime columns from timezone-naive sources.
    """

    name: str
    type: ColumnType
    nullable: bool = False
    source_timezone: str | None = None


class SchemaConfig(BaseModel):
    """Ordered list of column definitions forming a dataset's schema.

    Attributes:
        columns: Column definitions in the order they should appear
            after normalisation.
    """

    columns: list[ColumnDef]


class SourceConfig(BaseModel):
    """Configuration describing where and how to read raw source data.

    Attributes:
        type: Back-end type (``"file"`` or ``"athena"``).
        path: Filesystem path or glob pattern (used when *type* is ``"file"``).
        format: File format hint (used when *type* is ``"file"``).
        database: AWS Athena database name (used when *type* is ``"athena"``).
        query: SQL query for the Athena back-end.
        s3_output: S3 location for Athena query results.
        encoding: Text encoding for file-based sources.
    """

    type: SourceType
    path: str | None = None
    format: FileFormat | None = None
    database: str | None = None
    query: str | None = None
    s3_output: str | None = None
    encoding: str = "utf-8"


class ProcessingConfig(BaseModel):
    """Options that control post-ingestion processing (dedup, normalisation).

    Attributes:
        dedup_keys: Column names used to identify duplicate rows.  When
            *None*, deduplication is skipped.
        dedup_strategy: Which duplicate to keep when *dedup_keys* match.
        normalization_version: Semantic version tag stamped on each
            snapshot to track pipeline changes.
    """

    dedup_keys: list[str] | None = None
    dedup_strategy: DedupStrategy = DedupStrategy.keep_last
    normalization_version: str = "1.0"


class DatasetConfig(BaseModel):
    """Complete configuration for a single dataset entry in ``datasets.yaml``.

    Attributes:
        description: Human-readable description of the dataset.
        source: Where and how to read the raw data.
        schema_def: Expected column schema (aliased from the YAML key
            ``schema`` to avoid shadowing the Python built-in).
        processing: Post-ingestion processing options.
    """

    description: str = ""
    source: SourceConfig
    schema_def: SchemaConfig = Field(alias="schema")
    processing: ProcessingConfig = ProcessingConfig()

    model_config = {"populate_by_name": True}


class DatasetsConfig(BaseModel):
    """Top-level wrapper for the ``datasets.yaml`` file.

    Attributes:
        datasets: Mapping from dataset name to its configuration.
    """

    datasets: dict[str, DatasetConfig]


# ── Feature config models ────────────────────────────────────


def _get_valid_feature_types() -> set[str]:
    """Return the set of valid feature type identifiers.

    The single source of truth is :data:`adp.features.strategies.STRATEGY_REGISTRY`.
    This import is deferred to avoid circular dependencies between the
    config and feature modules.

    Returns:
        A set of string keys recognised by the feature strategy registry.
    """
    from adp.features.strategies import STRATEGY_REGISTRY

    return set(STRATEGY_REGISTRY.keys())


class FeatureDefConfig(BaseModel):
    """Configuration for a single feature within a feature set.

    Which optional fields are relevant depends on the *type*; for example
    a ``"rolling_mean"`` strategy requires *column* and *window*, whereas
    ``"vwap"`` requires *price_column* and *volume_column*.

    Attributes:
        name: Output column name for the computed feature.
        type: Strategy identifier (must exist in the strategy registry).
        column: Input column name for single-column strategies.
        window: Rolling window size (number of rows).
        span: Exponential span parameter (for EMA-based strategies).
        price_column: Price column used by composite strategies (e.g. VWAP).
        volume_column: Volume column used by composite strategies.
        group_by: Column to partition by before computing (e.g. ``"symbol"``).
        description: Human-readable description of the feature.
    """

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
            raise ValueError(f"Unknown feature type: '{self.type}'. Valid: {valid}")
        return self


class FeatureSetConfig(BaseModel):
    """Configuration for a named feature set within ``features.yaml``.

    Attributes:
        version: Integer version number for the feature set definition.
            Bumped when the feature list changes.
        description: Human-readable description of the feature set.
        features: Ordered list of individual feature definitions.
    """

    version: int
    description: str = ""
    features: list[FeatureDefConfig]


#: Parsed representation of the full ``features.yaml`` file.
#: Structure: ``{dataset_name: {feature_set_name: FeatureSetConfig}}``.
FeaturesConfig = dict[str, dict[str, FeatureSetConfig]]


# ── Loaders ───────────────────────────────────────────────────


def _resolve_config_dir() -> Path:
    """Resolve the configuration directory, honouring ``ADP_CONFIG_DIR``.

    Returns:
        The path from the ``ADP_CONFIG_DIR`` environment variable when
        set, otherwise ``Path("config")`` (relative to the working
        directory).
    """
    env = os.environ.get("ADP_CONFIG_DIR")
    if env:
        return Path(env)
    return Path("config")


def load_datasets_config(config_path: Path | None = None) -> DatasetsConfig:
    """Load and validate ``datasets.yaml`` into a :class:`DatasetsConfig`.

    Args:
        config_path: Explicit path to the YAML file.  When *None*, the
            file is located via :func:`_resolve_config_dir`.

    Returns:
        A validated :class:`DatasetsConfig` instance.

    Raises:
        ConfigError: If the file is missing, contains malformed YAML, is
            missing the top-level ``datasets`` key, or fails Pydantic
            validation.
    """
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
    """Load and validate ``features.yaml`` into a :data:`FeaturesConfig` dict.

    The YAML structure is a two-level mapping:
    ``{dataset_name: {feature_set_name: {version, description, features}}}``.

    Args:
        config_path: Explicit path to the YAML file.  When *None*, the
            file is located via :func:`_resolve_config_dir`.

    Returns:
        A nested dictionary mapping dataset names to their feature sets,
        each validated as a :class:`FeatureSetConfig`.

    Raises:
        ConfigError: If the file is missing, contains malformed YAML,
            is not a dict at the top level, or fails Pydantic validation.
    """
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
