# 0003: Add p-codes by matching GAUL names, with a per-country exclusion list

## Status

Accepted — 2026-10-20

## Context

EVE 2.0 identifies admin units only by FAO GAUL 2024 codes and names, and FAO has no GAUL-to-COD
crosswalk. A test with hdx-python-country 4.2.3 `AdminLevel` matched all 705 ADM1 and 7,794 ADM2
GAUL units against `global_pcodes_adm_1_2.csv`, with ADM2 constrained to its ADM1 parent:

| Level | Exact | Fuzzy | None | No COD |
|---|---|---|---|---|
| ADM1 | 563 | 47 | 64 | 31 (TZA) |
| ADM2 | 6,307 | 411 | 208 | 868 (TZA, VNM) |

Fuzzy matches are right where GAUL and COD structures agree, e.g. LAO "Xekong" → "Sekong". They
are wrong where the structures diverge:

- BDI: 18 GAUL ADM1 vs 5 COD; "Mugamba" → "Makamba"
- MLI: "Bougouni" → "Loulouni"
- CAF: "Vakaga" → "Bakala"
- MDG: "Atsinanana" → "Antsiranana I"
- MMR: GAUL ADM1 is literally "Region"/"State"

## Decision

Add `admin1_pcode` (ADM1, ADM2) and `admin2_pcode` (ADM2) columns after the GAUL name columns, and
keep the GAUL columns. Skip matching for the countries in `pcode_exclude`, which leaves their
p-codes blank:

- ADM1: BDI, BFA, CAF, MDG, MLI, MMR, VNM
- ADM2: BDI, BFA, CAF, HTI, MDG, MLI, MMR

Fix known misses and mismatches with `admin_name_mappings` (e.g. CMR "Ouest" fuzzy matched "East")
and `admin_fuzzy_dont` (PHL "Maguindanao", which split in two). Set `p_coded` only on resources
with at least one p-code.

## Consequences

- After the mappings, 553/555 ADM1 and 6,519/6,605 ADM2 units in the included countries have
  p-codes (checked 2026-10-20). The rest have no COD equivalent, e.g. waterbodies, Kinshasa
  communes, and VEN Federal Dependencies islands.
- New fuzzy matches appear in the run log (`Matching (fuzzy)`) and should be spot-checked when
  FAO adds countries.
- The exclusion list needs revisiting when a COD or GAUL boundary set is updated.
