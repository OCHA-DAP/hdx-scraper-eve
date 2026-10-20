# 0001: Read EVE 2.0 from the public ArcGIS REST API

## Status

Accepted — 2026-10-20

## Context

EVE 1.0 was read from an AGOL feature table (`98265cbe82e344f88552d4c2d3615c3c`) through the
`arcgis` SDK, with a DIEM account login (`DIEM_USERNAME`/`DIEM_PASSWORD`). Its data stopped at
2026-07-15. FAO replaced it with EVE 2.0 (HDXPIPE-180): three tables (ADM0, ADM1, ADM2) on public
FeatureServers under `services5.arcgis.com/sjP4Ugu5s0dZWLjd`. Anonymous queries succeed against
all three (checked 2026-10-20). `arcgis` pulls in `krb5`, which has no Linux wheels, and `pylerc`,
which has no musllinux wheel, so the Alpine Docker image could not be built.

## Decision

Query the three FeatureServer `query` endpoints with `Retrieve.download_json`, with no credentials.
Paginate with an object-id keyset (`<oid> > last`, ordered by oid, `exceededTransferLimit` to stop),
following `hdx-scraper-portwatch`. Retry error bodies returned with HTTP 200 three times. Get each
level's row count and maximum oid in one `outStatistics` query, page only up to that oid, and validate
the row count, so rows FAO appends mid-run are excluded. Levels are fetched minutes apart, so dekads
later than the latest dekad common to all levels are dropped. Refresh the full history every run:
FAO treats published dekads as immutable, and a full run is about 350 requests (about 7 minutes).

## Consequences

- `arcgis`, `krb5` and `dotenv` are removed, along with the DIEM secrets and the Kerberos CI steps.
  The Docker image can build again.
- Tests replay real responses with `Retrieve(use_saved=True)` rather than monkeypatching an SDK call.
- If FAO puts the tables behind authentication, the pipeline needs credentials again.
