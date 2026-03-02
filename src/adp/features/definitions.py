"""Feature definition models and parsing."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from adp.config import FeatureSetConfig


@dataclass(frozen=True)
class FeatureDefinition:
    """A single feature definition parsed from a YAML feature-set configuration.

    Attributes:
        name: Unique name that becomes the output column name in the
            materialised feature snapshot.
        type: Strategy key (e.g. ``"moving_average"``, ``"z_score"``) used to
            look up the computation in :data:`STRATEGY_REGISTRY`.
        params: Strategy-specific parameters such as ``column``, ``window``,
            and ``span``.
        description: Optional human-readable description of the feature.
    """

    name: str
    type: str
    params: dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass(frozen=True)
class FeatureSetDefinition:
    """A complete, versioned feature set parsed from a YAML configuration.

    Bundles one or more :class:`FeatureDefinition` items together with
    dataset binding information and a content-addressable hash used for
    change detection by the metadata registry.

    Attributes:
        name: Logical name of the feature set (e.g. ``"price_features"``).
        dataset_name: Name of the dataset this feature set targets.
        version: Monotonically increasing version number.
        description: Human-readable description of the feature set.
        features: Ordered list of individual feature definitions.
        definition_hash: SHA-256 digest of the canonical JSON representation
            of *features*, computed by :func:`compute_definition_hash`.
        sort_column: Column to sort the snapshot by before computing
            features.  Defaults to ``"timestamp"`` at materialisation time
            if left as ``None``.
    """

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
    """Parse a :class:`~adp.config.FeatureSetConfig` into a :class:`FeatureSetDefinition`.

    Each feature entry in *config* is converted into a :class:`FeatureDefinition`
    with only the non-``None`` parameters included in its ``params`` dict.  A
    content-addressable definition hash is computed over all features so the
    metadata registry can detect definition changes.

    Args:
        dataset_name: Name of the dataset the feature set targets.
        feature_set_name: Logical name for the feature set.
        config: Raw configuration object loaded from YAML.

    Returns:
        A fully resolved :class:`FeatureSetDefinition` ready for
        materialisation.
    """
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
    """Compute a SHA-256 hash of the canonical JSON representation of *features*.

    The hash is deterministic: keys are sorted and separators are compact
    (no whitespace).  Only the ``name``, ``type``, and ``params`` fields are
    included so that cosmetic changes to descriptions do not trigger a version
    bump.

    Args:
        features: Ordered list of feature definitions to hash.

    Returns:
        Hex-encoded SHA-256 digest string.
    """
    payload = json.dumps(
        [{"name": f.name, "type": f.type, "params": f.params} for f in features],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
