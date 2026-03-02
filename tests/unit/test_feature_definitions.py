"""Unit tests for adp.features.definitions — parsing and hashing."""

from __future__ import annotations

import re

import pytest

from adp.config import FeatureDefConfig, FeatureSetConfig
from adp.features.definitions import (
    FeatureDefinition,
    FeatureSetDefinition,
    compute_definition_hash,
    parse_feature_set,
)

# ── Helpers ──────────────────────────────────────────────────


def _make_feature_set_config(
    features: list[dict] | None = None,
    version: int = 1,
) -> FeatureSetConfig:
    """Build a ``FeatureSetConfig`` with sensible defaults for testing.

    When *features* is ``None``, two standard features are used:
    ``price_sma_5`` (moving average, window 5) and ``price_returns``
    (simple returns).

    Args:
        features: Optional list of feature definition dicts.  Each dict
            is unpacked into a ``FeatureDefConfig``.
        version: Feature set version number.

    Returns:
        A ``FeatureSetConfig`` ready for ``parse_feature_set`` or
        ``compute_definition_hash`` calls.
    """
    if features is None:
        features = [
            {"name": "price_sma_5", "type": "moving_average", "column": "price", "window": 5},
            {"name": "price_returns", "type": "returns", "column": "price"},
        ]
    return FeatureSetConfig(
        version=version,
        description="test feature set",
        features=[FeatureDefConfig(**f) for f in features],
    )


# ── parse_feature_set ────────────────────────────────────────


@pytest.mark.unit
class TestParseFeatureSet:
    def test_parse_basic(self) -> None:
        config = _make_feature_set_config()
        result = parse_feature_set("test_ds", "basic_factors", config)
        assert isinstance(result, FeatureSetDefinition)
        assert result.name == "basic_factors"
        assert result.dataset_name == "test_ds"
        assert result.version == 1
        assert len(result.features) == 2

    def test_feature_params_populated(self) -> None:
        config = _make_feature_set_config()
        result = parse_feature_set("test_ds", "basic_factors", config)
        sma_feat = result.features[0]
        assert sma_feat.name == "price_sma_5"
        assert sma_feat.type == "moving_average"
        assert sma_feat.params["column"] == "price"
        assert sma_feat.params["window"] == 5

    def test_definition_hash_present(self) -> None:
        config = _make_feature_set_config()
        result = parse_feature_set("test_ds", "factors", config)
        assert re.fullmatch(r"[0-9a-f]{64}", result.definition_hash)

    def test_parse_from_conftest_yaml(self, sample_features_yaml) -> None:
        """Use the conftest fixture to ensure end-to-end parse works."""
        from adp.config import load_features_config

        raw = load_features_config(sample_features_yaml)
        fs_config = raw["test_trades"]["basic_factors"]
        result = parse_feature_set("test_trades", "basic_factors", fs_config)
        assert result.version == 1
        assert len(result.features) == 2


# ── compute_definition_hash ──────────────────────────────────


@pytest.mark.unit
class TestComputeDefinitionHash:
    def test_deterministic(self) -> None:
        features = [
            FeatureDefinition(name="a", type="returns", params={"column": "price"}),
            FeatureDefinition(
                name="b", type="moving_average", params={"column": "price", "window": 5}
            ),
        ]
        h1 = compute_definition_hash(features)
        h2 = compute_definition_hash(features)
        assert h1 == h2

    def test_hash_changes_on_param_change(self) -> None:
        features_v1 = [
            FeatureDefinition(
                name="sma", type="moving_average", params={"column": "price", "window": 5}
            ),
        ]
        features_v2 = [
            FeatureDefinition(
                name="sma", type="moving_average", params={"column": "price", "window": 10}
            ),
        ]
        assert compute_definition_hash(features_v1) != compute_definition_hash(features_v2)

    def test_hash_changes_on_add_feature(self) -> None:
        base = [
            FeatureDefinition(name="a", type="returns", params={"column": "price"}),
        ]
        extended = [
            *base,
            FeatureDefinition(name="b", type="log_returns", params={"column": "price"}),
        ]
        assert compute_definition_hash(base) != compute_definition_hash(extended)
