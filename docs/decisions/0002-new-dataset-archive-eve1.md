# 0002: Publish EVE 2.0 as a new dataset and archive the EVE 1.0 dataset

## Status

Accepted — 2026-10-20

## Context

The EVE 1.0 dataset `fao-eve-global-flood-monitoring-system` has 37 resources, with biweekly
periods and `pop_exposed`/`cropland_flooded_ha`-style columns covering 2024-01-07 to 2026-07-15.
EVE 2.0 changes the schema: dekads, GAUL codes, and exceptional/seasonal/total split for area,
cropland, population and six livestock species. It also adds ADM0 and ADM1 tables. None of the
old resources have TDE (`datastore_active: False`). Overwriting in place with
`remove_additional_resources=True` would delete the EVE 1.0 history.
FAO agreed (2026-10-23) to retire EVE 1.0.

## Decision

Create a new dataset, `fao-eve-flood-exposure` ("FAO EVE Flood Exposure"), with:

- `global_fao_eve_flood_exposure_{adm0,adm1,adm2}.csv`
- one `<iso3>_fao_eve_flood_exposure_adm2.csv` per country

Archive the old dataset by hand in the HDX UI after the first successful prod run.

## Consequences

- The EVE 1.0 history stays available, frozen, on the archived dataset.
- Links to the old dataset URL don't redirect to the new one.
- The global ADM2 CSV is 91 MB (319,554 rows as of 2026-10-20) and grows about 68 MB a year. At that
  rate it reaches the 300 MB HDX resource limit in about 3 years.
- FAO is considering back-processing EVE 2.0 over the EVE 1.0 period (from 2024-01). That would
  bring the 300 MB limit closer.
