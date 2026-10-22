from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def config_dir(fixtures_dir):
    return join("src", "hdx", "scraper", "eve", "config")


@pytest.fixture(scope="session")
def configuration(config_dir):
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=join(config_dir, "project_configuration.yaml"),
    )
    configuration = Configuration.read()
    # Restrict fixtures to 1 dekad for 2 countries plus MMR, which is excluded from
    # p-code matching. ADM2 page size is reduced so that pagination is exercised.
    configuration["where"] = (
        "iso3_code IN ('MMR','MOZ','NGA') AND dekad_year=2026 AND dekad_month=10"
    )
    configuration["levels"]["adm2"]["page_size"] = 500
    Locations.set_validlocations(
        [
            {"name": "mmr", "title": "Myanmar"},
            {"name": "moz", "title": "Mozambique"},
            {"name": "nga", "title": "Nigeria"},
            {"name": "world", "title": "World"},
        ]
    )
    Country.countriesdata(False)
    tags = (
        "affected area",
        "affected population",
        "agriculture-livestock",
        "climate hazards",
        "flooding",
    )
    Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
    Vocabulary._approved_vocabulary = {
        "tags": [{"name": tag} for tag in tags],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return configuration
