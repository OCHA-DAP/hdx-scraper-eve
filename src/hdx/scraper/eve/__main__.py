#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
import os
from os.path import dirname, expanduser, join

from dotenv import load_dotenv
from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.eve.eve import Eve

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-eve"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: eve"

# Load local .env file if not running in GitHub Actions
if os.getenv("GITHUB_ACTIONS") is None:
    load_dotenv()

USERNAME = os.getenv("DIEM_USERNAME")
PASS = os.getenv("DIEM_PASSWORD")
if not USERNAME or not PASS:
    logger.error("DIEM_USERNAME or DIEM_PASSWORD environment variables are missing.")


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            #
            # Steps to generate dataset
            #
            configuration = Configuration.read()
            eve = Eve(configuration, retriever, temp_dir, USERNAME, PASS)
            dataset = eve.generate_dataset()
            dataset.update_from_yaml(
                path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
            )
            dataset.create_in_hdx(
                remove_additional_resources=True,
                match_resource_order=False,
                hxl_update=False,
                updated_by_script=_UPDATED_BY_SCRIPT,
                batch=info["batch"],
            )


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(dirname(__file__), "config", "project_configuration.yaml"),
    )
