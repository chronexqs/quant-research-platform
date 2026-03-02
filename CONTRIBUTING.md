# Contributing to Athena Data Platform

Thank you for your interest in contributing to ADP. This guide covers everything you need to get started.

## Development Setup

```bash
# Clone the repository
git clone https://github.com/chronexqs/quant-research-platform.git
cd quant-research-platform

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Linux/macOS

# Install with development dependencies
pip install -e ".[dev]"

# Verify installation
adp --help
python -m pytest tests/ -x -q
```

### Requirements

- Python 3.11, 3.12, or 3.13
- All dependencies are managed via `pyproject.toml`

## Code Style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [mypy](https://mypy-lang.org/) for type checking.

```bash
# Lint
ruff check src/ tests/

# Format
ruff format src/ tests/

# Type check
mypy src/
```

### Style Guidelines

- **Line length**: 100 characters
- **Type hints**: Required on all public functions
- **Imports**: Sorted by `isort` rules (via Ruff)
- **Docstrings**: Required on public modules, classes, and functions

## Testing

Tests are organized into three tiers:

```bash
# Run all tests
python -m pytest tests/

# Run by category
python -m pytest tests/unit/          # Fast, isolated tests
python -m pytest tests/integration/   # File I/O and SQLite tests
python -m pytest tests/e2e/           # Full pipeline tests

# Run with coverage
python -m pytest tests/ --cov=adp --cov-report=term-missing
```

### Writing Tests

- Place unit tests in `tests/unit/`, integration tests in `tests/integration/`, and end-to-end tests in `tests/e2e/`
- Use the shared fixtures from `tests/conftest.py` (temporary directories, mock registries, sample data)
- All tests must be deterministic — no randomness, no network calls
- Use `time-machine` for any datetime-dependent tests

## Pull Request Process

1. **Fork** the repository and create a feature branch from `main`
2. **Write tests** for any new functionality
3. **Ensure all checks pass** locally:
   ```bash
   python -m pytest tests/ -x
   ruff check src/ tests/
   ruff format --check src/ tests/
   mypy src/
   ```
4. **Commit** with a clear, descriptive message
5. **Open a pull request** against `main` with:
   - A summary of changes
   - Motivation / context
   - Testing approach

## Project Structure

```text
src/adp/
  api.py              # Public Python API
  cli.py              # Typer CLI
  config.py           # YAML configuration loading
  exceptions.py       # Exception hierarchy
  ingestion/          # Raw data ingestion
  processing/         # Schema validation, dedup, normalization
  storage/            # Parquet reader/writer, snapshot engine
  features/           # Feature strategies, definitions, materializer
  metadata/           # SQLite registry, models, schema
```

## Reporting Issues

Use [GitHub Issues](https://github.com/chronexqs/quant-research-platform/issues) to report bugs or request features. Include:

- Steps to reproduce (for bugs)
- Expected vs actual behavior
- Python version and OS
- Relevant log output

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
