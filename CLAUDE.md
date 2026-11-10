# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Publishes the HDX dataset `fao-eve-flood-exposure` from FAO DIEM's EVE 2.0 flood exposure tables,
read from three public ArcGIS FeatureServers (ADM0, ADM1 and ADM2 by dekad, keyed by FAO GAUL 2024
codes). The output is one dataset with `global_fao_eve_flood_exposure_{adm0,adm1,adm2}.csv` plus
one `<iso3>_fao_eve_flood_exposure_adm2.csv` per country. HDX adds `admin1_pcode`/`admin2_pcode`
columns by name-matching GAUL units to COD p-codes. The EVE 1.0 dataset
`fao-eve-global-flood-monitoring-system` is archived. See `docs/decisions/` 0001-0003.

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

- `__main__.py`: standard copier orchestration: builds the `Retrieve`, calls
  `Pipeline.generate_dataset()`, merges `config/hdx_dataset_static.yaml` and calls `create_in_hdx`.
- `pipeline.py` (`Pipeline`):
  - `get_level_data(level)`: count query, then object-id keyset pagination with retries on ArcGIS
    error bodies. Drops the oid and `disp_en` fields, validates the row count, and sorts latest
    dekad first.
  - `add_pcodes(level_data)`: loads the global p-codes CSV once and sets up `AdminLevel` for
    ADM1/ADM2 (ADM2 parent-constrained). Matches each GAUL unit once and inserts the p-code columns.
    Countries in `pcode_exclude` are skipped.
  - `generate_dataset()`: builds the dataset and resources. `p_coded` is set on a resource only if
    it has p-codes.
- `config/project_configuration.yaml`: base URL, per-level table/oid field/page size, `where`
  filter, `pcode_exclude`, `admin_name_mappings`, `admin_fuzzy_dont`, and dataset/resource names,
  descriptions and tags.
- `tests/`: `conftest.py` narrows `where` to MMR/MOZ/NGA for October 2026 and sets the ADM2 page
  size to 500. `test_pipeline.py` replays `tests/fixtures/input/` (raw ArcGIS responses and the
  global p-codes CSV) and compares output CSVs with `tests/fixtures/*.csv`. To recapture fixtures,
  rerun the same queries with `curl` (or `Retrieve(save=True)`) using the conftest overrides.

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
