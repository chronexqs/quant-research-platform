"""Unit tests for adp.storage.snapshot — ID generation functions."""

from __future__ import annotations

import re

import pytest
import time_machine

from adp.metadata.registry import MetadataRegistry
from adp.storage.snapshot import (
    generate_feature_snapshot_id,
    generate_ingestion_id,
    generate_snapshot_id,
)


@pytest.mark.unit
class TestIDGeneration:
    @time_machine.travel("2026-03-15 12:00:00+00:00")
    def test_ingestion_id_format(self, in_memory_registry: MetadataRegistry) -> None:
        """Ingestion ID matches {dataset}_ing_{YYYYMMDD}_{seq:03d}."""
        iid = generate_ingestion_id("test_ds", in_memory_registry)
        assert re.fullmatch(r"test_ds_ing_20260315_\d{3}", iid), f"Bad format: {iid}"

    @time_machine.travel("2026-03-15 12:00:00+00:00")
    def test_snapshot_id_sequence_increments(self, in_memory_registry: MetadataRegistry) -> None:
        """Calling generate_snapshot_id twice increments the sequence."""
        # Register a dataset and create a first snapshot to consume seq 1
        in_memory_registry.register_dataset("test_ds", schema_hash="h")
        sid1 = generate_snapshot_id("test_ds", in_memory_registry)
        in_memory_registry.create_snapshot(
            snapshot_id=sid1,
            dataset_name="test_ds",
            schema_hash="h",
            normalization_version="1.0",
            row_count=10,
            storage_path="/tmp/s1",
        )
        sid2 = generate_snapshot_id("test_ds", in_memory_registry)
        # Extract sequence numbers
        seq1 = int(sid1.rsplit("_", 1)[-1])
        seq2 = int(sid2.rsplit("_", 1)[-1])
        assert seq2 == seq1 + 1

    @time_machine.travel("2026-03-15 12:00:00+00:00")
    def test_no_collision_different_datasets(self, in_memory_registry: MetadataRegistry) -> None:
        """IDs for different datasets never collide."""
        sid_a = generate_snapshot_id("alpha", in_memory_registry)
        sid_b = generate_snapshot_id("beta", in_memory_registry)
        assert sid_a != sid_b

    @time_machine.travel("2026-06-01 09:00:00+00:00")
    def test_feature_snapshot_id_format(self, in_memory_registry: MetadataRegistry) -> None:
        """Feature snapshot ID: {dataset}_{featureset}_fsnap_{YYYYMMDD}_{seq:03d}."""
        fid = generate_feature_snapshot_id("test_ds", "basic_factors", in_memory_registry)
        assert re.fullmatch(r"test_ds_basic_factors_fsnap_20260601_\d{3}", fid), (
            f"Bad format: {fid}"
        )
