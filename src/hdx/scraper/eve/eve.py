#!/usr/bin/python
"""eve scraper"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime
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
# Set to desired period number, None for all periods, or "latest" for the most recent available
PERIOD_NUMBER = "latest"
FILE_FORMAT = "CSV"


class Eve:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str, use_saved: bool
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self._use_saved = use_saved

    def get_locations(self, country_name: str) -> dict:
        """
        Get location by fuzzy matching the country name.
        """
        iso3 = Country.get_iso3_country_code_fuzzy(country_name)

        if iso3[0] is None:
            logger.error("Could not match country", country_name)
            return {}
        location_name = Country.get_country_name_from_iso3(iso3[0])
        return {"code": iso3[0], "name": location_name}

    def calculate_current_period(self) -> int:
        """
        Calculate the current period based on the current date.
        Each year has 24 periods (2 per month).
        """
        # Calculate the latest period number dynamically
        # Get the current date and time
        current_datetime = datetime.now()
        start_year = 2024  # make this dynamic?
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
        Connect to ArcGIS, query the feature layer, and return the data.
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

    def get_data(self) -> List:
        """
        Retrieve data either from a saved JSON file or from ArcGIS.
        """
        if self._use_saved:
            try:
                with open("saved_data/data-latest.json", "r") as file:
                    json_data = json.load(file)
                    return [feature["attributes"] for feature in json_data["features"]]
            except FileNotFoundError:
                logger.error("Saved data file not found.")
                return []
        else:
            return self.get_arcgis_data()

    def get_country_list(self, data: List) -> List[str]:
        """
        Get a unique list of country names from the data.
        """
        country_set = {item["adm0_name"] for item in data}
        return list(country_set)

    def parse_dates(sslf, record: dict) -> (datetime, datetime):
        """
        Parse the 'start_date' and 'end_date' from a record.
        """
        start = datetime.strptime(record["start_date"], "%Y-%m-%d")
        end = datetime.utcfromtimestamp(record["end_date"] / 1000)
        return start, end

    def generate_dataset(self, data: List) -> Optional[Dataset]:
        """
        Generate dataset from the data.
        """
        countries = self.get_country_list(data)
        start_dates = []
        end_dates = []
        grouped = defaultdict(list)
        for record in data:
            # get all start and end dates
            start, end = self.parse_dates(record)
            start_dates.append(start)
            end_dates.append(end)

            # Group records by 'adm0_iso3' while excluding the 'ObjectId' field.
            key = record["adm0_iso3"]
            filtered_record = {k: v for k, v in record.items() if k != "ObjectId"}
            grouped[key].append(filtered_record)

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
        resource_name = f"global-{slugify(dataset_info["resource_title"])}.csv"
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
        )

        # Generate resource by country
        for i, (iso3, records) in enumerate(grouped.items()):
            if i == 5:  # for testing
                break
            resource_name = f"{iso3.lower()}-{slugify(dataset_info["resource_title"])}.csv"
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
            )

        return dataset
