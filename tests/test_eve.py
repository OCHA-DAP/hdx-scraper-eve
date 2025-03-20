from os.path import join

import pytest
from hdx.api.configuration import Configuration
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

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    @pytest.fixture(scope="class")
    def config_dir(self, fixtures_dir):
        return join("src", "hdx", "scraper", "eve", "config")

    def test_eve(self, configuration, fixtures_dir, input_dir, config_dir):
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
                eve = Eve(configuration, retriever, tempdir)

                dataset = eve.generate_dataset()
                dataset.update_from_yaml(path=join(config_dir, "hdx_dataset_static.yaml"))

                assert dataset == {
                    "name": "fao-flood-events-visualization-in-emergencies-eve",
                    "title": "FAO Flood Events Visualization in Emergencies (EVE)",
                    "dataset_date": "[2024-01-07T00:00:00 TO 2025-03-15T23:59:59]",
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
                    "methodology": "methodology here",
                    "caveats": "None",
                    "dataset_source": "Food and Agriculture Organization (FAO)",
                    "groups": [
                        {"name": "afg"},
                        {"name": "ago"},
                        {"name": "bgd"},
                        {"name": "bfa"},
                        {"name": "bdi"},
                        {"name": "khm"},
                        {"name": "cmr"},
                        {"name": "caf"},
                        {"name": "tcd"},
                        {"name": "col"},
                        {"name": "cod"},
                        {"name": "hti"},
                        {"name": "hnd"},
                        {"name": "irq"},
                        {"name": "lao"},
                        {"name": "mdg"},
                        {"name": "mwi"},
                        {"name": "mli"},
                        {"name": "moz"},
                        {"name": "mmr"},
                        {"name": "nam"},
                        {"name": "npl"},
                        {"name": "ner"},
                        {"name": "nga"},
                        {"name": "pak"},
                        {"name": "phl"},
                        {"name": "som"},
                        {"name": "ssd"},
                        {"name": "lka"},
                        {"name": "sdn"},
                        {"name": "tha"},
                        {"name": "tza"},
                        {"name": "vnm"},
                        {"name": "yem"},
                        {"name": "zmb"},
                        {"name": "zwe"},
                    ],
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
                        "population and various land cover types - for Afghanistan "
                        "from 1 July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "afg-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Angola from 1 "
                        "July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "ago-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Bangladesh "
                        "from 1 July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "bgd-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Burkina Faso "
                        "from 1 July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "bfa-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "description": "Biweekly insights on flood events - their impacts on "
                        "population and various land cover types - for Burundi from "
                        "1 July 2024 (when available) to date.",
                        "format": "csv",
                        "name": "bdi-events-visualization-in-emergencies.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]
