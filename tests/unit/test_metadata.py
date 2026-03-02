"""Unit tests for adp.metadata.registry — MetadataRegistry backed by in-memory SQLite."""

from __future__ import annotations

import pytest

from adp.exceptions import MetadataError
from adp.metadata.models import (
    DatasetRecord,
    FeatureDefinitionRecord,
    FeatureLineageRecord,
    FeatureSnapshotRecord,
    IngestionRecord,
    SnapshotLineageRecord,
    SnapshotRecord,
)
from adp.metadata.registry import MetadataRegistry

# ── Helpers ──────────────────────────────────────────────────


def _register_dataset(reg: MetadataRegistry, name: str = "test_ds") -> DatasetRecord:
    return reg.register_dataset(name, schema_hash="abc123", description="test")


def _log_ingestion(
    reg: MetadataRegistry,
    dataset_name: str = "test_ds",
    ingestion_id: str = "ing_001",
) -> IngestionRecord:
    return reg.log_ingestion(
        ingestion_id=ingestion_id,
        dataset_name=dataset_name,
        source_type="file",
        source_location="/data/raw/test.parquet",
        row_count=100,
    )


def _create_snapshot(
    reg: MetadataRegistry,
    dataset_name: str = "test_ds",
    snapshot_id: str = "snap_001",
) -> SnapshotRecord:
    return reg.create_snapshot(
        snapshot_id=snapshot_id,
        dataset_name=dataset_name,
        schema_hash="abc123",
        normalization_version="1.0",
        row_count=100,
        storage_path="/data/normalized/test_ds/snap_001",
    )


# ── Init ─────────────────────────────────────────────────────


@pytest.mark.unit
class TestMetadataRegistryInit:
    def test_init_creates_tables(self, in_memory_registry: MetadataRegistry) -> None:
        tables = {
            row[0]
            for row in in_memory_registry.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        expected = {
            "datasets",
            "raw_ingestions",
            "snapshots",
            "snapshot_lineage",
            "feature_definitions",
            "feature_snapshots",
            "feature_lineage",
        }
        assert expected.issubset(tables)

    def test_init_idempotent(self) -> None:
        """Creating two registries on the same DB does not error."""
        reg1 = MetadataRegistry(":memory:")
        # Re-initialize on the same connection should not raise
        from adp.metadata.schema import initialize_schema

        initialize_schema(reg1.conn)


# ── Datasets ─────────────────────────────────────────────────


@pytest.mark.unit
class TestDatasetOperations:
    def test_register_dataset(self, in_memory_registry: MetadataRegistry) -> None:
        rec = _register_dataset(in_memory_registry)
        assert isinstance(rec, DatasetRecord)
        assert rec.dataset_name == "test_ds"
        assert rec.schema_hash == "abc123"
        assert rec.current_snapshot is None

    def test_register_dataset_duplicate(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        with pytest.raises(MetadataError, match="already registered"):
            _register_dataset(in_memory_registry)

    def test_list_datasets(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry, "alpha")
        _register_dataset(in_memory_registry, "beta")
        datasets = in_memory_registry.list_datasets()
        names = [d.dataset_name for d in datasets]
        assert names == ["alpha", "beta"]

    def test_update_current_snapshot(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        _create_snapshot(in_memory_registry)
        in_memory_registry.update_current_snapshot("test_ds", "snap_001")
        rec = in_memory_registry.get_dataset("test_ds")
        assert rec is not None
        assert rec.current_snapshot == "snap_001"


# ── Ingestions ───────────────────────────────────────────────


@pytest.mark.unit
class TestIngestionOperations:
    def test_log_ingestion(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        rec = _log_ingestion(in_memory_registry)
        assert isinstance(rec, IngestionRecord)
        assert rec.ingestion_id == "ing_001"
        assert rec.row_count == 100

        fetched = in_memory_registry.get_ingestion("ing_001")
        assert fetched is not None
        assert fetched.ingestion_id == "ing_001"


# ── Snapshots ────────────────────────────────────────────────


@pytest.mark.unit
class TestSnapshotOperations:
    def test_create_snapshot(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        rec = _create_snapshot(in_memory_registry)
        assert isinstance(rec, SnapshotRecord)
        assert rec.snapshot_id == "snap_001"
        assert rec.row_count == 100

    def test_list_snapshots(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        _create_snapshot(in_memory_registry, snapshot_id="snap_001")
        _create_snapshot(in_memory_registry, snapshot_id="snap_002")
        snaps = in_memory_registry.list_snapshots("test_ds")
        assert len(snaps) == 2


# ── Lineage ──────────────────────────────────────────────────


@pytest.mark.unit
class TestLineageOperations:
    def test_link_snapshot_lineage(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        _log_ingestion(in_memory_registry)
        _create_snapshot(in_memory_registry)
        rec = in_memory_registry.link_snapshot_lineage("snap_001", "ing_001")
        assert isinstance(rec, SnapshotLineageRecord)
        assert rec.snapshot_id == "snap_001"
        assert rec.ingestion_id == "ing_001"

    def test_get_snapshot_lineage(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        _log_ingestion(in_memory_registry, ingestion_id="ing_001")
        _log_ingestion(in_memory_registry, ingestion_id="ing_002")
        _create_snapshot(in_memory_registry)
        in_memory_registry.link_snapshot_lineage("snap_001", "ing_001")
        in_memory_registry.link_snapshot_lineage("snap_001", "ing_002")
        lineage = in_memory_registry.get_snapshot_lineage("snap_001")
        assert len(lineage) == 2
        assert {rec.ingestion_id for rec in lineage} == {"ing_001", "ing_002"}


# ── Transaction rollback ─────────────────────────────────────


@pytest.mark.unit
class TestTransactions:
    def test_transaction_rollback_on_error(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        try:
            with in_memory_registry.transaction() as cur:
                cur.execute(
                    "INSERT INTO snapshots (snapshot_id, dataset_name, schema_hash, normalization_version, row_count, storage_path, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        "rollback_snap",
                        "test_ds",
                        "abc",
                        "1.0",
                        10,
                        "/tmp",
                        "2026-01-01T00:00:00+00:00",
                    ),
                )
                raise RuntimeError("Force rollback")
        except RuntimeError:
            pass
        result = in_memory_registry.get_snapshot("rollback_snap")
        assert result is None, "Snapshot should have been rolled back"


# ── Feature definitions ──────────────────────────────────────


@pytest.mark.unit
class TestFeatureDefinitionOperations:
    def test_register_feature_definition(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        rec = in_memory_registry.register_feature_definition(
            feature_name="basic_factors",
            dataset_name="test_ds",
            version=1,
            definition_hash="def_hash_abc",
            definition_yaml="features: [...]",
        )
        assert isinstance(rec, FeatureDefinitionRecord)
        assert rec.feature_name == "basic_factors"
        assert rec.version == 1

        fetched = in_memory_registry.get_feature_definition("basic_factors", "test_ds", version=1)
        assert fetched is not None
        assert fetched.definition_hash == "def_hash_abc"

    def test_hash_mismatch_raises(self, in_memory_registry: MetadataRegistry) -> None:
        """Re-registering same name+version with different hash should raise."""
        _register_dataset(in_memory_registry)
        in_memory_registry.register_feature_definition(
            feature_name="basic_factors",
            dataset_name="test_ds",
            version=1,
            definition_hash="hash_v1_original",
        )
        with pytest.raises(MetadataError, match="hash mismatch"):
            in_memory_registry.register_feature_definition(
                feature_name="basic_factors",
                dataset_name="test_ds",
                version=1,
                definition_hash="hash_v1_changed",
            )

    def test_update_nonexistent_dataset_raises(self, in_memory_registry: MetadataRegistry) -> None:
        """Updating current_snapshot for a nonexistent dataset should raise."""
        with pytest.raises(MetadataError, match="not found"):
            in_memory_registry.update_current_snapshot("ghost_dataset", "snap_001")


# ── Feature snapshots ────────────────────────────────────────


@pytest.mark.unit
class TestFeatureSnapshotOperations:
    def test_create_feature_snapshot(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        in_memory_registry.register_feature_definition(
            feature_name="basic_factors",
            dataset_name="test_ds",
            version=1,
            definition_hash="def_hash_abc",
        )
        rec = in_memory_registry.create_feature_snapshot(
            feature_snapshot_id="fsnap_001",
            feature_name="basic_factors",
            dataset_name="test_ds",
            feature_version=1,
            definition_hash="def_hash_abc",
            row_count=100,
            storage_path="/data/features/test_ds/basic_factors/fsnap_001",
        )
        assert isinstance(rec, FeatureSnapshotRecord)
        assert rec.feature_snapshot_id == "fsnap_001"
        assert rec.row_count == 100

    def test_link_feature_lineage(self, in_memory_registry: MetadataRegistry) -> None:
        _register_dataset(in_memory_registry)
        _create_snapshot(in_memory_registry)
        in_memory_registry.register_feature_definition(
            feature_name="basic_factors",
            dataset_name="test_ds",
            version=1,
            definition_hash="def_hash_abc",
        )
        in_memory_registry.create_feature_snapshot(
            feature_snapshot_id="fsnap_001",
            feature_name="basic_factors",
            dataset_name="test_ds",
            feature_version=1,
            definition_hash="def_hash_abc",
            row_count=100,
            storage_path="/data/features/test_ds/basic_factors/fsnap_001",
        )
        rec = in_memory_registry.link_feature_lineage("fsnap_001", "snap_001")
        assert isinstance(rec, FeatureLineageRecord)
        assert rec.feature_snapshot_id == "fsnap_001"
        assert rec.dataset_snapshot_id == "snap_001"

        lineage = in_memory_registry.get_feature_lineage("fsnap_001")
        assert len(lineage) == 1
        assert lineage[0].dataset_snapshot_id == "snap_001"
