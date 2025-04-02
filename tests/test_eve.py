import os
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent

from hdx.scraper.eve.eve import Eve


class TestEve:
    @pytest.fixture(scope="function")
    def configuration(self, config_dir):
        UserAgent.set_global("test")
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            project_config_yaml=join(config_dir, "project_configuration.yaml"),
        )
        return Configuration.read()

    @pytest.fixture(scope="function")
    def test_get_arcgis_data(self, monkeypatch):
        def test_get_arcgis_data(self):
            result = Dataset.load_from_json(
                join(
                    "tests",
                    "fixtures",
                    "input",
                    "filtered-data.json",
                )
            )
            results_list = [feature["attributes"] for feature in result["features"]]
            return results_list

        monkeypatch.setattr(Eve, "get_arcgis_data", test_get_arcgis_data)

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "eve", "config")

    def test_eve(self, configuration, test_get_arcgis_data, fixtures_dir, input_dir, config_dir):
        USERNAME = os.getenv("DIEM_USERNAME")
        PASS = os.getenv("DIEM_PASSWORD")

        with temp_dir(
            "TestEve",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                eve = Eve(configuration, retriever, tempdir, USERNAME, PASS)

                eve_data = eve.get_arcgis_data()
                assert eve_data[0] == {
                    "ObjectId": 120798,
                    "adm0_iso3": "THA",
                    "adm0_name": "Thailand",
                    "adm1_name": "Phangnga",
                    "adm1_pcode": "TH82",
                    "adm2_name": "Thai Mueang",
                    "adm2_pcode": "TH8208",
                    "admin_level": "admin2",
                    "biweekly_group": "Period #28 (16/02/2025 to 28/02/2025)",
                    "cropland_flooded_ha": 7,
                    "cropland_flooded_sq_km": 0.07,
                    "end_date": 1740700800000,
                    "perc_cropland_flooded": 4.046242774566474,
                    "perc_total_area_flooded": 5.097858099306593,
                    "period_number": 28,
                    "pop_affected": 2550,
                    "start_date": "2025-02-16",
                    "total_area_flooded_ha": 3185,
                    "total_area_flooded_sq_km": 31.849999999999998,
                }

                processed_data = eve.process_data(eve_data)
                assert processed_data[0] == {
                    "adm0_iso3": "NGA",
                    "adm0_name": "Nigeria",
                    "adm1_name": "Abia",
                    "adm1_pcode": "NG001",
                    "adm2_name": "Arochukwu",
                    "adm2_pcode": "NG001003",
                    "admin_level": "admin2",
                    "cropland_flooded_ha": 0,
                    "cropland_flooded_sq_km": 0,
                    "end_date": 1740700800000,
                    "perc_cropland_flooded": 0,
                    "perc_total_area_flooded": 0.35772245159730026,
                    "period_number": 28,
                    "pop_exposed": 451,
                    "start_date": "2025-02-16",
                    "total_area_flooded_ha": 184,
                    "total_area_flooded_sq_km": 1.84,
                }

                countries = eve.get_country_list(processed_data)
                assert countries == ["Nigeria", "Thailand", "Yemen"]

                dataset = eve.generate_dataset()
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))

                assert dataset == {
                    "name": "fao-flood-events-visualization-in-emergencies-eve",
                    "title": "FAO Flood Events Visualization in Emergencies (EVE)",
                    "dataset_date": "[2025-02-16T00:00:00 TO 2025-02-28T23:59:59]",
                    "tags": [
                        {
                            "name": "affected area",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "affected population",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "climate hazards",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "flooding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "license_id": "cc-by",
                    "methodology": "https://data-in-emergencies.fao.org/documents/3335ee769a4e45708e27d2ee25d13bef/about",
                    "caveats": "EVE analyses are based on global datasets to ensure broad "
                    "coverage, rapid scalability, and integration into international "
                    "humanitarian workflows. However, these datasets cannot always "
                    "capture national or local specificities that would enhance "
                    "analytical precision.\n"
                    "\n"
                    "While EVE can estimate the extent of flooded cropland, it does "
                    "not determine whether the affected cropland was actively "
                    "cultivated at the time of the flood.\n"
                    "\n"
                    "EVE does not inherently differentiate between hazardous and "
                    "non-hazardous floods, which can lead to false detections in "
                    "agricultural regions. Some flood events result from regular "
                    "seasonal patterns or controlled agricultural activities, such as "
                    "rice paddy planting or aquaculture, rather than actual "
                    "hazard-related floods.\n",
                    "dataset_source": "Food and Agriculture Organization (FAO)",
                    "groups": [{"name": "nga"}, {"name": "tha"}, {"name": "yem"}],
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "169c44fe-3043-4616-8719-59cf9f101270",
                    "owner_org": "ed727a5b-3e6e-4cd6-b97e-4a71532085e6",
                    "data_update_frequency": 14,
                    "notes": "The DIEM Events Visualization in Emergencies (EVE) system "
                    "provides resources to enhance the understanding of flood events and "
                    "their impact on different land cover types, with a particular focus "
                    "on agricultural areas. EVE provides a flood persistence analysis as "
                    "well as an estimation of the population exposed to such events.\n"
                    "\n"
                    "EVE utilizes satellite-derived data from the NOAA Visible Infrared "
                    "Imaging Radiometer Suite (VIIRS) at a 375-meter resolution, alongside "
                    "land cover data from the European Space Agency’s WorldCover 10m 2021 "
                    "dataset. Covering approximately 40 countries, the system offers daily "
                    "and biweekly insights, providing a continuously updated view of flood "
                    "dynamics and their effects.\n"
                    "\n"
                    "The platform presents results through interactive maps, charts, and "
                    "tables, supporting decision-making in disaster management, agricultural "
                    "planning, and environmental monitoring. Most resources are publicly "
                    "accessible, though downloading aggregated data at the admin2 level "
                    "requires a [DIEM account](https://hqfao.maps.arcgis.com/sharing/rest/"
                    "oauth2/signup?client_id=aEXLMtXxljlIrgPN&response_type=token&expiration="
                    "20160&showSocialLogins=true&locale=en-us&redirect_uri=https%3A%2F%2Fdata-"
                    "in-emergencies.fao.org%2Ftorii-provider-arcgis%2Fhub-redirect.html).\n"
                    "\n"
                    "EVE products are preliminary analyses and have not yet undergone field "
                    "validation. Users are encouraged to provide ground feedback to the [FAO "
                    "Data in Emergencies (DIEM) team](https://data-in-emergencies.fao.org/"
                    "pages/contactus) to enhance the accuracy and utility of the data.\n",
                }

                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "global-events-visualization-in-emergencies.csv",
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for all countries from 1 "
                        "July 2024 (when available) to date.",
                        "format": "csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Nigeria "
                        "from 1 July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "nga-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Thailand from 1 "
                        "July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "tha-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Yemen "
                        "from 1 July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "yem-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
