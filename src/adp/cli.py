"""Typer CLI for the Athena Data Platform."""

from __future__ import annotations

import logging
from pathlib import Path

import typer

from adp.exceptions import ADPError
from adp.metadata.registry import MetadataRegistry

app = typer.Typer(name="adp", help="Athena Data Platform CLI")
snapshot_app = typer.Typer(help="Manage dataset snapshots")
features_app = typer.Typer(help="Manage feature materialisation")
app.add_typer(snapshot_app, name="snapshot")
app.add_typer(features_app, name="features")


@app.callback()
def _main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Athena Data Platform CLI."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


def _get_registry(metadata_dir: Path) -> MetadataRegistry:
    db_path = metadata_dir / "adp_registry.db"
    return MetadataRegistry(db_path)


# ── Init ──────────────────────────────────────────────────────


@app.command()
def init(
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """Initialize the ADP platform directory structure and metadata database."""
    for subdir in ["raw", "staged", "normalized", "features"]:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)

    _get_registry(metadata_dir)
    typer.echo(f"Platform initialized at {data_dir.resolve()}")


# ── Ingest ────────────────────────────────────────────────────


@app.command()
def ingest(
    dataset_name: str = typer.Argument(..., help="Dataset name from config"),
    source: str | None = typer.Option(None, help="Override source type"),
    path: Path | None = typer.Option(None, help="Override file path"),
    force: bool = typer.Option(False, help="Force re-ingestion"),
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """Ingest data for a dataset."""
    from adp.config import load_datasets_config
    from adp.ingestion import run_ingestion
    from adp.processing.schema import compute_schema_hash_from_defs

    try:
        datasets_config = load_datasets_config(config_dir / "datasets.yaml")
        if dataset_name not in datasets_config.datasets:
            typer.echo(
                f"Error: Dataset '{dataset_name}' not found in config.",
                err=True,
            )
            typer.echo(
                f"Available: {sorted(datasets_config.datasets.keys())}",
                err=True,
            )
            raise typer.Exit(1) from None

        ds_config = datasets_config.datasets[dataset_name]
        registry = _get_registry(metadata_dir)

        # Ensure dataset is registered
        if registry.get_dataset(dataset_name) is None:
            schema_hash = compute_schema_hash_from_defs(
                ds_config.schema_def.columns
            )
            registry.register_dataset(
                dataset_name=dataset_name,
                schema_hash=schema_hash,
                description=ds_config.description,
            )

        result = run_ingestion(
            dataset_name=dataset_name,
            dataset_config=ds_config,
            data_dir=data_dir,
            registry=registry,
            force=force,
            source_override=source,
            path_override=str(path) if path else None,
        )
        typer.echo(
            f"Ingested {result.row_count} rows -> {result.ingestion_id}"
        )

    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


# ── Snapshot ──────────────────────────────────────────────────


@snapshot_app.command("create")
def snapshot_create(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """Create a normalized snapshot from the latest raw ingestion."""
    from adp.config import load_datasets_config
    from adp.storage.snapshot import SnapshotEngine

    try:
        datasets_config = load_datasets_config(config_dir / "datasets.yaml")
        if dataset_name not in datasets_config.datasets:
            typer.echo(
                f"Error: Dataset '{dataset_name}' not found in config.",
                err=True,
            )
            raise typer.Exit(1) from None

        ds_config = datasets_config.datasets[dataset_name]
        registry = _get_registry(metadata_dir)
        engine = SnapshotEngine(data_dir, registry)

        ingestions = registry.list_ingestions(dataset_name)
        if not ingestions:
            typer.echo(
                f"Error: No ingestions for '{dataset_name}'. "
                "Run 'adp ingest' first.",
                err=True,
            )
            raise typer.Exit(1) from None

        ingestion_ids = [ing.ingestion_id for ing in ingestions]
        snapshot_id = engine.create_snapshot(
            dataset_name, ds_config, ingestion_ids
        )
        typer.echo(f"Created snapshot: {snapshot_id}")

    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


@snapshot_app.command("list")
def snapshot_list(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """List all snapshots for a dataset."""
    registry = _get_registry(metadata_dir)
    snapshots = registry.list_snapshots(dataset_name)
    if not snapshots:
        typer.echo(f"No snapshots found for '{dataset_name}'.")
        return
    for snap in snapshots:
        ds = registry.get_dataset(dataset_name)
        is_current = ds and ds.current_snapshot == snap.snapshot_id
        tag = " (current)" if is_current else ""
        typer.echo(
            f"  {snap.snapshot_id}  rows={snap.row_count}  "
            f"created={snap.created_at}{tag}"
        )


@snapshot_app.command("show")
def snapshot_show(
    snapshot_id: str = typer.Argument(..., help="Snapshot ID"),
    lineage: bool = typer.Option(False, help="Show lineage"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """Show details of a specific snapshot."""
    registry = _get_registry(metadata_dir)
    snap = registry.get_snapshot(snapshot_id)
    if not snap:
        typer.echo(f"Snapshot '{snapshot_id}' not found.", err=True)
        raise typer.Exit(1) from None

    typer.echo(f"Snapshot: {snap.snapshot_id}")
    typer.echo(f"  Dataset:    {snap.dataset_name}")
    typer.echo(f"  Rows:       {snap.row_count}")
    typer.echo(f"  Schema:     {snap.schema_hash[:16]}...")
    typer.echo(f"  Version:    {snap.normalization_version}")
    typer.echo(f"  Path:       {snap.storage_path}")
    typer.echo(f"  Created:    {snap.created_at}")

    if lineage:
        lineage_records = registry.get_snapshot_lineage(snapshot_id)
        if lineage_records:
            typer.echo("  Lineage:")
            for lr in lineage_records:
                typer.echo(f"    <- {lr.ingestion_id}")


# ── Features ──────────────────────────────────────────────────


@features_app.command("build")
def features_build(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    feature_set: str = typer.Argument(..., help="Feature set name"),
    snapshot: str | None = typer.Option(
        None, help="Specific snapshot ID"
    ),
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """Build features for a dataset."""
    from adp.config import load_features_config
    from adp.features.definitions import parse_feature_set
    from adp.features.materializer import FeatureMaterialiser

    try:
        features_config = load_features_config(config_dir / "features.yaml")
        if dataset_name not in features_config:
            typer.echo(
                f"Error: No features for dataset '{dataset_name}'.",
                err=True,
            )
            raise typer.Exit(1) from None
        if feature_set not in features_config[dataset_name]:
            typer.echo(
                f"Error: Feature set '{feature_set}' not found "
                f"for '{dataset_name}'.",
                err=True,
            )
            avail = sorted(features_config[dataset_name].keys())
            typer.echo(f"Available: {avail}", err=True)
            raise typer.Exit(1) from None

        fs_config = features_config[dataset_name][feature_set]
        fs_def = parse_feature_set(dataset_name, feature_set, fs_config)

        registry = _get_registry(metadata_dir)
        materialiser = FeatureMaterialiser(data_dir, registry)
        fsnap_id = materialiser.materialise(fs_def, snapshot_id=snapshot)
        typer.echo(f"Built features: {fsnap_id}")

    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


@features_app.command("list")
def features_list(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """List feature sets for a dataset."""
    registry = _get_registry(metadata_dir)
    defs = registry.list_feature_definitions(dataset_name)
    if not defs:
        typer.echo(f"No feature sets found for '{dataset_name}'.")
        return
    seen: set[tuple[str, int]] = set()
    for d in defs:
        key = (d.feature_name, d.version)
        if key not in seen:
            seen.add(key)
            typer.echo(
                f"  {d.feature_name} v{d.version}  "
                f"hash={d.definition_hash[:16]}..."
            )


@features_app.command("show")
def features_show(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    feature_set: str = typer.Argument(..., help="Feature set name"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
) -> None:
    """Show feature set details and snapshots."""
    registry = _get_registry(metadata_dir)
    snapshots = registry.list_feature_snapshots(dataset_name, feature_set)
    if not snapshots:
        typer.echo(
            f"No snapshots for '{feature_set}' on '{dataset_name}'."
        )
        return
    for fs in snapshots:
        typer.echo(
            f"  {fs.feature_snapshot_id}  v{fs.feature_version}  "
            f"rows={fs.row_count}  created={fs.created_at}"
        )


@features_app.command("load")
def features_load(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    feature_set: str = typer.Argument(..., help="Feature set name"),
    head: int = typer.Option(10, help="Number of rows to show"),
    metadata_dir: Path = typer.Option(
        Path("metadata"), help="Metadata directory"
    ),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
) -> None:
    """Load and display feature data."""
    from adp.features.feature_store import load_feature_snapshot

    try:
        registry = _get_registry(metadata_dir)
        lf = load_feature_snapshot(
            dataset_name, feature_set, registry=registry
        )
        df = lf.head(head).collect()
        typer.echo(str(df))
    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
