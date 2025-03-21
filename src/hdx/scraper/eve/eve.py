#!/usr/bin/python
"""eve scraper"""

import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import List, Optional

from arcgis.gis import GIS
from dotenv import load_dotenv
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)

# Load local .env file if not running in GitHub Actions
if os.environ.get("GITHUB_ACTIONS") is None:
    load_dotenv()

USERNAME = os.environ.get("DIEM_USERNAME")
PASS = os.environ.get("DIEM_PASSWORD")
if not USERNAME or not PASS:
    logger.error("DIEM_USERNAME or DIEM_PASSWORD environment variables are missing.")

# Query settings
PERIOD_NUMBER = None  # "latest"
FILE_FORMAT = "CSV"
START_YEAR = 2024


class Eve:
    def __init__(self, configuration: Configuration, retriever: Retrieve, temp_dir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir

    def calculate_current_period(self) -> int:
        """
        Calculate the latest period number dynamically
        https://github.com/Andrampa/DIEM_API/blob/main/DIEM_API_get_EVE_data.ipynb
        """
        # Get the current date and time
        current_datetime = datetime.now()
        start_year = START_YEAR
        current_year = current_datetime.year
        current_month = current_datetime.month

        # Each year has 24 periods (2 per month)
        total_periods_since_start = (current_year - start_year) * 24 + (current_month - 1) * 2

        # Determine if we are in the first or second period of the current month
        if current_datetime.day >= 15:
            total_periods_since_start += 2  # Second period of the month
        else:
            total_periods_since_start += 1  # First period of the month

        return total_periods_since_start

    def get_arcgis_data(self) -> List:
        """
        Connect to ArcGIS to query the feature layer
        https://github.com/Andrampa/DIEM_API/blob/main/DIEM_API_get_EVE_data.ipynb
        """
        config = self._configuration
        where_clauses = []
        period_num = PERIOD_NUMBER

        # Connect to AGOL
        gis = GIS(config["base_url"], USERNAME, PASS)

        # Get the feature layer
        item = gis.content.get(config["feature_table_id"])
        if not item:
            raise ValueError("Feature table not found. Check the ID.")
        feature_layer = item.tables[0]

        # Determine the period to query
        if period_num == "latest":
            period_num = self.calculate_current_period()
            while True:
                print(f"Trying period_number: {period_num}")
                query_result = feature_layer.query(
                    where=f"period_number = {period_num}",
                    out_fields="period_number",
                    return_geometry=False,
                    as_df=False,
                )

                # Check if data exists by looking at the features list
                if query_result.features:
                    print(f"Data found for period_number: {period_num}")
                    break  # Exit loop when data is found
                else:
                    print(f"No data for period_num: {period_num}, trying previous period...")
                    period_num -= 1  # Try the previous period

        if period_num is not None:
            where_clauses.append(f"period_number = {period_num}")

        # Combine conditions or select all
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Query the feature table but exclude the ObjectId field
        fields = [
            f["name"] for f in feature_layer.properties.fields if f["name"].lower() != "objectid"
        ]
        result = feature_layer.query(
            where=where_clause, out_fields=",".join(fields), return_geometry=False, as_df=False
        )
        results_list = [feature.attributes for feature in result.features]
        return results_list

    def get_country_list(self, data: List) -> List[str]:
        """
        Get a unique list of country names from the data.
        """
        country_set = {item["adm0_name"] for item in data}
        return sorted(country_set)

    def get_locations(self, country_name: str) -> dict:
        """
        Get HDX location by fuzzy matching the country name.
        """
        iso3 = Country.get_iso3_country_code_fuzzy(country_name)

        if iso3[0] is None:
            logger.error("Could not match country", country_name)
            return {}
        location_name = Country.get_country_name_from_iso3(iso3[0])
        return {"code": iso3[0], "name": location_name}

    def parse_dates(self, record: dict) -> (datetime, datetime):
        """
        Parse the 'start_date' and 'end_date' from a record.
        """
        start = record["start_date"]  # datetime.strptime(record["start_date"], "%Y-%m-%d").date()
        end = datetime.fromtimestamp(record["end_date"] / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        return start, end

    def reorder_dict(self, d: dict) -> dict:
        """
        Reorders dictionary so key "end_date" appears after "start_date".
        """
        if "start_date" not in d or "end_date" not in d:
            return d

        end_date_value = d.pop("end_date")

        new_dict = {}
        for key, value in d.items():
            new_dict[key] = value
            if key == "start_date":
                new_dict["end_date"] = end_date_value

        return new_dict

    def process_data(self, data: List) -> List:
        """
        Process data by filtering out unwanted columns and sorting data
        """
        processed_data = []
        for row in data:
            # Filter out unnecessary columns from data
            filtered_record = {
                k: v for k, v in row.items() if k not in ("ObjectId", "biweekly_group")
            }

            # Rename key if it exists
            if "pop_affected" in filtered_record:
                filtered_record["pop_exposed"] = filtered_record.pop("pop_affected")

            processed_data.append(filtered_record)

        # Reorder keys in data so start date and end date are together
        processed_data = [self.reorder_dict(d) for d in processed_data]

        # Sort data by descending period number and by country name
        processed_data.sort(key=lambda item: (-item["period_number"], item["adm0_name"]))
        return processed_data

    def generate_dataset(self) -> Optional[Dataset]:
        """
        Generate dataset from the data.
        """
        eve_data = self.get_arcgis_data()
        data = self.process_data(eve_data)
        countries = self.get_country_list(data)

        start_dates = []
        end_dates = []
        grouped = defaultdict(list)
        for record in data:
            # Get all start and end dates
            start, end = self.parse_dates(record)
            record["start_date"] = start
            record["end_date"] = end
            start_dates.append(start)
            end_dates.append(end)

            # Group records by 'adm0_iso3'
            key = record["adm0_iso3"]
            grouped[key].append(record)

        # Create dataset
        dataset_info = self._configuration
        dataset_title = dataset_info["title"]
        slugified_name = slugify(dataset_title)
        dataset = Dataset({"name": slugified_name, "title": dataset_title})

        # Add dataset info
        dataset.add_country_locations(countries)
        dataset.add_tags(dataset_info["tags"])
        dataset.set_time_period(min(start_dates), max(end_dates))

        # Create global resource
        resource_name = f"global-{slugify(dataset_info['resource_title'])}.csv"
        resource_description = dataset_info["description"].replace("(country)", "all countries")
        resource = {
            "name": resource_name,
            "description": resource_description,
        }

        dataset.generate_resource_from_iterable(
            headers=list(data[0].keys()),
            iterable=data,
            hxltags=dataset_info["hxl_tags"],
            folder=self._temp_dir,
            filename=resource_name,
            resourcedata=resource,
            quickcharts=None,
            encoding="utf-8-sig",
        )

        # Generate resource by country
        for i, (iso3, records) in enumerate(grouped.items()):
            if i == 5:  # for testing
                break
            resource_name = f"{iso3.lower()}-{slugify(dataset_info['resource_title'])}.csv"
            resource_description = dataset_info["description"].replace(
                "(country)", Country.get_country_name_from_iso3(iso3)
            )
            resource = {
                "name": resource_name,
                "description": resource_description,
            }
            dataset.generate_resource_from_iterable(
                headers=list(records[0].keys()),
                iterable=records,
                hxltags=dataset_info["hxl_tags"],
                folder=self._temp_dir,
                filename=resource_name,
                resourcedata=resource,
                quickcharts=None,
                encoding="utf-8-sig",
            )
        return dataset
