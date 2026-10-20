# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Publishes the HDX dataset `fao-eve-global-flood-monitoring-system` from FAO DIEM's EVE (Events
Visualization in Emergencies) flood feature table on ArcGIS Online. Produces one global CSV
resource plus one CSV resource per country. Requires DIEM account credentials (`DIEM_USERNAME`,
`DIEM_PASSWORD`, read from a local `.env` file or the environment).

## Commands

Environment setup (Python 3.13, managed with `uv`):

```shell
uv sync
```

Run the pipeline (requires `~/.hdx_configuration.yaml` with an HDX key, and `~/.useragents.yaml`
with a `hdx-scraper-eve` entry — see README.md):

```shell
uv run python -m hdx.scraper.eve
```

Run all tests (with coverage, configured in `pyproject.toml`):

```shell
uv run pytest
```

Lint and format:

```shell
uv run ruff check
uv run ruff format
```

Pre-commit (runs ruff on every commit once installed):

```shell
pre-commit install
pre-commit run --all-files
```

Build:

```shell
uv build
```

Adding a dependency: add it to `project.dependencies` in `pyproject.toml` (or `[dependency-groups]`
for test-only deps), then run `uv lock --upgrade` to refresh `uv.lock` (pre-commit also does this
automatically on commit).

## Architecture

- `__main__.py` — loads DIEM credentials, builds the `Retrieve`, calls `Eve.generate_dataset()`,
  merges `config/hdx_dataset_static.yaml` and calls `create_in_hdx`.
- `eve.py` — `Eve` class: logs in to AGOL with the `arcgis` SDK (`get_arcgis_data`), renames and
  drops columns (`process_data`), and builds the dataset with global and per-country resources
  (`generate_dataset`).
- `config/project_configuration.yaml` — AGOL base URL, feature table id, title/description/tag
  strings.
- `tests/test_eve.py` — monkeypatches `get_arcgis_data` to read `tests/fixtures/input/filtered-data.json`.

## Code Style

- Formatted with `ruff` via pre-commit hooks. After changing any Python code, run:

```bash
pre-commit run --all-files
```

- Python ≥ 3.13

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag
  tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions,
  external systems). Search or read the actual source first. If you lack verified information, say
  so rather than speculate.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific
issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements
in the same commit unless they are directly required by the fix. Unrelated changes obscure the
intent of the fix and complicate review and blame.

## Decision Records

Non-trivial design decisions are recorded in `docs/decisions/` (see `docs/decisions/README.md`) —
the distilled decision, not the full planning narrative, belongs here.
