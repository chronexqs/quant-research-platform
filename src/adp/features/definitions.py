"""Feature definition models and parsing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from adp.config import FeatureSetConfig


@dataclass(frozen=True)
class FeatureDefinition:
    """A single feature definition parsed from YAML."""

    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass(frozen=True)
class FeatureSetDefinition:
    """A complete feature set parsed from YAML."""

    name: str
    dataset_name: str
    version: int
    description: str
    features: list[FeatureDefinition]
    definition_hash: str
    sort_column: str | None = None


def parse_feature_set(
    dataset_name: str,
    feature_set_name: str,
    config: FeatureSetConfig,
) -> FeatureSetDefinition:
    """Parse a FeatureSetConfig into a FeatureSetDefinition."""
    features: list[FeatureDefinition] = []
    for fc in config.features:
        params: dict[str, Any] = {}
        if fc.column is not None:
            params["column"] = fc.column
        if fc.window is not None:
            params["window"] = fc.window
        if fc.span is not None:
            params["span"] = fc.span
        if fc.price_column is not None:
            params["price_column"] = fc.price_column
        if fc.volume_column is not None:
            params["volume_column"] = fc.volume_column
        if fc.group_by is not None:
            params["group_by"] = fc.group_by
        features.append(
            FeatureDefinition(
                name=fc.name,
                type=fc.type,
                params=params,
                description=fc.description,
            )
        )

    definition_hash = compute_definition_hash(features)
    return FeatureSetDefinition(
        name=feature_set_name,
        dataset_name=dataset_name,
        version=config.version,
        description=config.description,
        features=features,
        definition_hash=definition_hash,
    )


def compute_definition_hash(features: list[FeatureDefinition]) -> str:
    """Compute SHA-256 hash of serialized feature definitions."""
    payload = json.dumps(
        [{"name": f.name, "type": f.type, "params": f.params} for f in features],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
