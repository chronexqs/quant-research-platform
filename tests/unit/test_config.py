"""Unit tests for adp.config — YAML loaders and Pydantic config models."""

from __future__ import annotations

import pytest
from pathlib import Path

from adp.config import (
    DatasetsConfig,
    FeatureSetConfig,
    load_datasets_config,
    load_features_config,
)
from adp.exceptions import ConfigError


@pytest.mark.unit
class TestLoadDatasetsConfig:
    def test_load_valid(self, sample_datasets_yaml: Path) -> None:
        config = load_datasets_config(sample_datasets_yaml)
        assert isinstance(config, DatasetsConfig)
        assert "test_trades" in config.datasets

    def test_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="not found"):
            load_datasets_config(tmp_path / "nonexistent.yaml")

    def test_malformed_yaml(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(": invalid: yaml: [")
        with pytest.raises(ConfigError, match="Malformed"):
            load_datasets_config(bad)

    def test_missing_required_field(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("datasets:\n  test:\n    source:\n      type: file\n")
        with pytest.raises(ConfigError, match="Validation"):
            load_datasets_config(bad)

    def test_invalid_column_type(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "datasets:\n"
            "  test:\n"
            "    source:\n"
            "      type: file\n"
            "      path: test.csv\n"
            "      format: csv\n"
            "    schema:\n"
            "      columns:\n"
            "        - name: col1\n"
            "          type: invalid_type\n"
            "    processing:\n"
            '      normalization_version: "1.0"\n'
        )
        with pytest.raises(ConfigError):
            load_datasets_config(bad)


@pytest.mark.unit
class TestLoadFeaturesConfig:
    def test_load_features_valid(self, sample_features_yaml: Path) -> None:
        config = load_features_config(sample_features_yaml)
        assert "test_trades" in config
        assert "basic_factors" in config["test_trades"]
        fs = config["test_trades"]["basic_factors"]
        assert isinstance(fs, FeatureSetConfig)
        assert fs.version == 1
        assert len(fs.features) == 2

    def test_load_features_missing(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="not found"):
            load_features_config(tmp_path / "nope.yaml")

    def test_load_features_unknown_type(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text(
            "test:\n"
            "  fset:\n"
            "    version: 1\n"
            "    features:\n"
            "      - name: bad_feature\n"
            "        type: nonexistent_strategy\n"
            "        column: price\n"
        )
        with pytest.raises(ConfigError):
            load_features_config(bad)
