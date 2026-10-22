#!/usr/bin/python
"""EVE scraper"""

import json
import logging
import time

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

_LEVELS = ("adm0", "adm1", "adm2")


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def _download_json(self, url: str, params: dict, filename: str) -> dict:
        # ArcGIS returns HTTP 200 with an error body on failure
        error = None
        for attempt in range(3):
            data = self._retriever.download_json(
                url, parameters=params, filename=filename
            )
            error = data.get("error")
            if not error:
                return data
            if attempt < 2:
                logger.warning(
                    f"Attempt {attempt + 1}/3 failed for {filename}: {error.get('message')}. Retrying..."
                )
                time.sleep(2 * (attempt + 1))
        raise ValueError(f"{filename}: {error.get('code')} {error.get('message')}")

    def get_level_data(self, level: str) -> list[dict]:
        """Get all rows of an EVE admin level table using keyset pagination on
        the object id field up to the maximum object id at the time of the count
        query, so rows appended during the run are excluded. Validates the row
        count against the server count.

        Args:
            level: adm0, adm1 or adm2

        Returns:
            List of rows
        """
        level_info = self._configuration["levels"][level]
        url = f"{self._configuration['base_url']}/{level_info['table']}/query"
        where = self._configuration["where"]
        oid_field = level_info["oid_field"]
        page_size = level_info["page_size"]

        statistics = [
            {
                "statisticType": statistic_type,
                "onStatisticField": oid_field,
                "outStatisticFieldName": statistic_type,
            }
            for statistic_type in ("count", "max")
        ]
        data = self._download_json(
            url,
            {"where": where, "outStatistics": json.dumps(statistics), "f": "json"},
            f"eve_{level}_count.json",
        )
        attributes = data["features"][0]["attributes"]
        expected_count = attributes["count"]
        if expected_count == 0:
            logger.warning(f"{level}: no rows")
            return []
        max_oid = attributes["max"]

        drop_fields = set(self._configuration["drop_fields"])
        drop_fields.add(oid_field)
        rows = []
        last_oid = 0
        while True:
            params = {
                "where": f"({where}) AND {oid_field}>{last_oid} AND {oid_field}<={max_oid}",
                "outFields": "*",
                "returnGeometry": "false",
                "orderByFields": oid_field,
                "resultRecordCount": page_size,
                "f": "json",
            }
            data = self._download_json(url, params, f"eve_{level}_{last_oid}.json")
            features = data.get("features", [])
            for feature in features:
                attributes = feature["attributes"]
                row = {}
                for key, value in attributes.items():
                    if key in drop_fields:
                        continue
                    if isinstance(value, str):
                        value = value.strip()
                    row[key] = value
                rows.append(row)
            if not features or not data.get("exceededTransferLimit"):
                break
            last_oid = features[-1]["attributes"][oid_field]

        if len(rows) != expected_count:
            raise ValueError(
                f"{level}: retrieved {len(rows)} rows but server reports {expected_count}"
            )
        logger.info(f"{level}: retrieved {len(rows)} rows")
        for row in rows:
            if not row["iso3_code"]:
                raise ValueError(f"{level}: row with no iso3_code: {row}")
        # Latest dekad first, then by country and admin units (sort is stable)
        rows.sort(
            key=lambda row: (
                row["iso3_code"],
                row.get("gaul1_code") or 0,
                row.get("gaul2_code") or 0,
            )
        )
        rows.sort(key=lambda row: row["dekad_start"], reverse=True)
        return rows

    def add_pcodes(self, level_data: dict[str, list[dict]]) -> None:
        """Add admin1_pcode and admin2_pcode values to the rows by matching GAUL
        names to the HDX global p-codes dataset. _get_headers places them after
        the GAUL name columns.

        Args:
            level_data: Rows by admin level

        Returns:
            None
        """
        countryiso3s = sorted({row["iso3_code"] for row in level_data["adm0"]})
        admin_name_mappings = self._configuration["admin_name_mappings"]
        admin_fuzzy_dont = self._configuration["admin_fuzzy_dont"]
        _, iterator = self._retriever.get_tabular_rows(
            AdminLevel.admin_url, dict_form=True
        )
        pcode_rows = list(iterator)
        admin1 = AdminLevel(
            {
                "admin_name_mappings": admin_name_mappings["adm1"],
                "admin_fuzzy_dont": admin_fuzzy_dont,
            },
            admin_level=1,
        )
        admin1.setup_from_iterable(pcode_rows, countryiso3s)
        admin2 = AdminLevel(
            {
                "admin_name_mappings": admin_name_mappings["adm2"],
                "admin_fuzzy_dont": admin_fuzzy_dont,
            },
            admin_level=2,
        )
        admin2.setup_from_iterable(pcode_rows, countryiso3s)
        admin2.set_parent_admins_from_adminlevels([admin1])
        exclude = self._configuration["pcode_exclude"]

        gaul1_to_pcode = {}
        gaul2_to_pcode = {}
        for level in ("adm1", "adm2"):
            for row in level_data[level]:
                iso3 = row["iso3_code"]
                gaul1_code = row["gaul1_code"]
                if gaul1_code not in gaul1_to_pcode:
                    pcode = None
                    if iso3 not in exclude["adm1"]:
                        pcode, _ = admin1.get_pcode(
                            iso3, row["gaul1_name"], logname="EVE"
                        )
                    gaul1_to_pcode[gaul1_code] = pcode
                if level == "adm1":
                    continue
                gaul2_code = row["gaul2_code"]
                if gaul2_code not in gaul2_to_pcode:
                    pcode = None
                    parent = gaul1_to_pcode[gaul1_code]
                    # Without a parent, AdminLevel matches against all ADM2s in the
                    # country, so a unit could get a p-code under another ADM1
                    if parent and iso3 not in exclude["adm2"]:
                        pcode, _ = admin2.get_pcode(
                            iso3,
                            row["gaul2_name"],
                            parent=parent,
                            logname="EVE",
                        )
                    gaul2_to_pcode[gaul2_code] = pcode
        # AdminLevel logs non-exact matches as they happen but unmatched names only here
        admin1.output_errors()
        admin2.output_errors()

        for level in ("adm1", "adm2"):
            for row in level_data[level]:
                row["admin1_pcode"] = gaul1_to_pcode[row["gaul1_code"]]
                if level == "adm2":
                    row["admin2_pcode"] = gaul2_to_pcode[row["gaul2_code"]]

    @staticmethod
    def _get_headers(row: dict) -> list[str]:
        headers = []
        for key in row:
            if key in ("admin1_pcode", "admin2_pcode"):
                continue
            headers.append(key)
            if key == "gaul1_name":
                headers.append("admin1_pcode")
            elif key == "gaul2_name":
                headers.append("admin2_pcode")
        return headers

    def _generate_resource(
        self, dataset: Dataset, rows: list[dict], level: str, iso3: str | None
    ) -> None:
        configuration = self._configuration
        if iso3:
            location = iso3.lower()
            location_desc = Country.get_country_name_from_iso3(iso3)
        else:
            location = "global"
            location_desc = "all covered countries"
        filename = configuration["resource_filename"].format(
            location=location, level=level
        )
        resourcedata = {
            "name": filename,
            "description": configuration["resource_description"].format(
                level_desc=configuration["level_descriptions"][level],
                location_desc=location_desc,
            ),
        }
        pcode_field = "admin2_pcode" if level == "adm2" else "admin1_pcode"
        if any(row.get(pcode_field) for row in rows):
            resourcedata["p_coded"] = True
        dataset.generate_resource(
            folder=self._tempdir,
            filename=filename,
            rows=rows,
            resourcedata=resourcedata,
            headers=self._get_headers(rows[0]),
        )

    def generate_dataset(self) -> Dataset | None:
        level_data = {level: self.get_level_data(level) for level in _LEVELS}
        for level in _LEVELS:
            if not level_data[level]:
                logger.error(f"No EVE {level} data found")
                return None
        # Levels are fetched at different times so FAO may have added a dekad to
        # some levels only. Rows are sorted latest dekad first.
        latest_dekad = min(level_data[level][0]["dekad_start"] for level in _LEVELS)
        for level in _LEVELS:
            rows = level_data[level]
            if rows[0]["dekad_start"] > latest_dekad:
                level_data[level] = [
                    row for row in rows if row["dekad_start"] <= latest_dekad
                ]
                logger.warning(f"{level}: dropped dekads after {latest_dekad}")
        self.add_pcodes(level_data)

        configuration = self._configuration
        dataset = Dataset(
            {
                "name": configuration["dataset_name"],
                "title": configuration["dataset_title"],
            }
        )
        adm0_rows = level_data["adm0"]
        countryiso3s = sorted({row["iso3_code"] for row in adm0_rows})
        try:
            dataset.add_country_locations(countryiso3s)
        except HDXError:
            logger.exception("Could not add country locations")
            return None
        dataset.set_subnational(True)
        dataset.add_tags(configuration["tags"])
        dataset.set_time_period(
            min(row["dekad_start"] for row in adm0_rows),
            max(row["dekad_end"] for row in adm0_rows),
        )

        for level in _LEVELS:
            self._generate_resource(dataset, level_data[level], level, None)
        adm2_by_country = {}
        for row in level_data["adm2"]:
            adm2_by_country.setdefault(row["iso3_code"], []).append(row)
        for iso3 in sorted(adm2_by_country):
            self._generate_resource(dataset, adm2_by_country[iso3], "adm2", iso3)
        return dataset
