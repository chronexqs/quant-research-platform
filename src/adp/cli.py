"""Typer CLI for the Athena Data Platform.

Provides the ``adp`` command-line interface with subcommands for
initialisation, ingestion, snapshot management, and feature
materialisation.  Run ``adp --help`` for usage details.

Subcommand groups:

* **adp init / ingest** -- top-level platform commands.
* **adp snapshot create|list|show** -- dataset snapshot management.
* **adp features build|list|show|load** -- feature lifecycle management.
"""

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
    """Athena Data Platform CLI -- top-level callback.

    Configures the root logger before any subcommand runs.

    Args:
        verbose: When ``True``, sets the log level to ``DEBUG``;
            otherwise defaults to ``WARNING``.
    """
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


def _get_registry(metadata_dir: Path) -> MetadataRegistry:
    """Construct a :class:`MetadataRegistry` from a metadata directory.

    Args:
        metadata_dir: Directory containing (or that will contain) the
            ``adp_registry.db`` SQLite file.

    Returns:
        A connected :class:`MetadataRegistry` instance.
    """
    db_path = metadata_dir / "adp_registry.db"
    return MetadataRegistry(db_path)


# ── Init ──────────────────────────────────────────────────────


@app.command()
def init(
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """Initialize the ADP platform directory structure and metadata database.

    Creates the ``data/{raw,staged,normalized,features}``, ``metadata/``,
    and ``config/`` directories if they do not already exist, then
    initialises the SQLite registry.

    Args:
        config_dir: Path to the configuration directory.
        data_dir: Root path for lakehouse data storage.
        metadata_dir: Path to the metadata / registry directory.
    """
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
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """Ingest raw data for a named dataset.

    Reads the dataset configuration from ``datasets.yaml``, registers
    the dataset in the metadata registry if it is new, and then runs
    the ingestion pipeline.

    Args:
        dataset_name: Name of the dataset as declared in the config.
        source: Override the configured source type.
        path: Override the configured file path.
        force: Force re-ingestion even if data has already been ingested.
        config_dir: Path to the configuration directory.
        data_dir: Root path for lakehouse data storage.
        metadata_dir: Path to the metadata / registry directory.

    Raises:
        typer.Exit: On configuration or ingestion errors (exit code 1).
    """
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
            schema_hash = compute_schema_hash_from_defs(ds_config.schema_def.columns)
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
        typer.echo(f"Ingested {result.row_count} rows -> {result.ingestion_id}")

    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


# ── Snapshot ──────────────────────────────────────────────────


@snapshot_app.command("create")
def snapshot_create(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """Create a normalised snapshot from the latest raw ingestion.

    Loads the dataset config, collects all ingestion IDs for the named
    dataset, and passes them through the :class:`SnapshotEngine` to
    produce a normalised, deduplicated Parquet snapshot.

    Args:
        dataset_name: Name of the dataset as declared in the config.
        config_dir: Path to the configuration directory.
        data_dir: Root path for lakehouse data storage.
        metadata_dir: Path to the metadata / registry directory.

    Raises:
        typer.Exit: On configuration, missing-ingestion, or snapshot
            errors (exit code 1).
    """
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
                f"Error: No ingestions for '{dataset_name}'. Run 'adp ingest' first.",
                err=True,
            )
            raise typer.Exit(1) from None

        ingestion_ids = [ing.ingestion_id for ing in ingestions]
        snapshot_id = engine.create_snapshot(dataset_name, ds_config, ingestion_ids)
        typer.echo(f"Created snapshot: {snapshot_id}")

    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


@snapshot_app.command("list")
def snapshot_list(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """List all snapshots for a dataset, marking the current one.

    Args:
        dataset_name: Name of the dataset whose snapshots to list.
        metadata_dir: Path to the metadata / registry directory.
    """
    registry = _get_registry(metadata_dir)
    snapshots = registry.list_snapshots(dataset_name)
    if not snapshots:
        typer.echo(f"No snapshots found for '{dataset_name}'.")
        return
    for snap in snapshots:
        ds = registry.get_dataset(dataset_name)
        is_current = ds and ds.current_snapshot == snap.snapshot_id
        tag = " (current)" if is_current else ""
        typer.echo(f"  {snap.snapshot_id}  rows={snap.row_count}  created={snap.created_at}{tag}")


@snapshot_app.command("show")
def snapshot_show(
    snapshot_id: str = typer.Argument(..., help="Snapshot ID"),
    lineage: bool = typer.Option(False, help="Show lineage"),
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """Show details of a specific snapshot, optionally including lineage.

    Args:
        snapshot_id: The unique identifier of the snapshot to inspect.
        lineage: When ``True``, also prints the ingestion IDs that
            contributed to the snapshot.
        metadata_dir: Path to the metadata / registry directory.

    Raises:
        typer.Exit: If the snapshot is not found (exit code 1).
    """
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
    snapshot: str | None = typer.Option(None, help="Specific snapshot ID"),
    config_dir: Path = typer.Option(Path("config"), help="Config directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """Build (materialise) features for a dataset from a feature set definition.

    Loads the feature set configuration, parses it into a feature-set
    definition, and delegates to the :class:`FeatureMaterialiser` to
    compute and persist the feature snapshot.

    Args:
        dataset_name: Name of the dataset as declared in the config.
        feature_set: Name of the feature set to build.
        snapshot: Optional snapshot ID to build features against.
            Defaults to the current snapshot of the dataset.
        config_dir: Path to the configuration directory.
        data_dir: Root path for lakehouse data storage.
        metadata_dir: Path to the metadata / registry directory.

    Raises:
        typer.Exit: On configuration or materialisation errors
            (exit code 1).
    """
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
                f"Error: Feature set '{feature_set}' not found for '{dataset_name}'.",
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
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """List registered feature sets for a dataset.

    Args:
        dataset_name: Name of the dataset whose feature sets to list.
        metadata_dir: Path to the metadata / registry directory.
    """
    registry = _get_registry(metadata_dir)
    defs = registry.list_feature_definitions(dataset_name)
    if not defs:
        typer.echo(f"No feature sets found for '{dataset_name}'.")
        return
    # Deduplicate by (name, version) since the registry may store
    # multiple snapshots for the same feature set definition.
    seen: set[tuple[str, int]] = set()
    for d in defs:
        key = (d.feature_name, d.version)
        if key not in seen:
            seen.add(key)
            typer.echo(f"  {d.feature_name} v{d.version}  hash={d.definition_hash[:16]}...")


@features_app.command("show")
def features_show(
    dataset_name: str = typer.Argument(..., help="Dataset name"),
    feature_set: str = typer.Argument(..., help="Feature set name"),
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
) -> None:
    """Show feature set details and all associated snapshots.

    Args:
        dataset_name: Name of the dataset.
        feature_set: Name of the feature set to inspect.
        metadata_dir: Path to the metadata / registry directory.
    """
    registry = _get_registry(metadata_dir)
    snapshots = registry.list_feature_snapshots(dataset_name, feature_set)
    if not snapshots:
        typer.echo(f"No snapshots for '{feature_set}' on '{dataset_name}'.")
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
    metadata_dir: Path = typer.Option(Path("metadata"), help="Metadata directory"),
    data_dir: Path = typer.Option(Path("data"), help="Data directory"),
) -> None:
    """Load and display the first *head* rows of a feature snapshot.

    Args:
        dataset_name: Name of the dataset.
        feature_set: Name of the feature set to load.
        head: Number of rows to display.
        metadata_dir: Path to the metadata / registry directory.
        data_dir: Root path for lakehouse data storage.

    Raises:
        typer.Exit: If the feature snapshot cannot be loaded (exit code 1).
    """
    from adp.features.feature_store import load_feature_snapshot

    try:
        registry = _get_registry(metadata_dir)
        lf = load_feature_snapshot(dataset_name, feature_set, registry=registry)
        df = lf.head(head).collect()
        typer.echo(str(df))
    except ADPError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1) from None


if __name__ == "__main__":
    app()
