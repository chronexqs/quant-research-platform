"""ADP exception hierarchy.

All ADP-specific exceptions inherit from :class:`ADPError`, making it
easy to catch every platform error in a single ``except`` clause.

Hierarchy::

    ADPError
    +-- ConfigError
    +-- IngestionError
    +-- SchemaValidationError
    +-- NormalizationError
    +-- SnapshotError
    +-- DatasetNotFoundError
    +-- SnapshotNotFoundError
    +-- FeatureError
    +-- FeatureSetNotFoundError
    +-- FeatureSnapshotNotFoundError
    +-- MetadataError
"""


class ADPError(Exception):
    """Base exception for all ADP errors."""


class ConfigError(ADPError):
    """Configuration loading or validation error."""


class IngestionError(ADPError):
    """Data ingestion error."""


class SchemaValidationError(ADPError):
    """Schema validation failure."""


class NormalizationError(ADPError):
    """Normalization pipeline error."""


class SnapshotError(ADPError):
    """Snapshot creation or retrieval error."""


class DatasetNotFoundError(ADPError):
    """Dataset not registered in metadata."""


class SnapshotNotFoundError(ADPError):
    """Snapshot not found in metadata."""


class FeatureError(ADPError):
    """Feature computation error."""


class FeatureSetNotFoundError(ADPError):
    """Feature set not found."""


class FeatureSnapshotNotFoundError(ADPError):
    """Feature snapshot not found."""


class MetadataError(ADPError):
    """Metadata registry error."""
