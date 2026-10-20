from os.path import join

import pytest
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.eve import pipeline as pipeline_module
from hdx.scraper.eve.pipeline import Pipeline


class FakeRetriever:
    def __init__(self, responses):
        self.responses = responses
        self.calls = 0

    def download_json(self, url, parameters, filename):
        response = self.responses[min(self.calls, len(self.responses) - 1)]
        self.calls += 1
        return response


class TestPipeline:
    def test_pipeline(self, configuration, fixtures_dir, input_dir, config_dir):
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
                pipeline = Pipeline(configuration, retriever, tempdir)

                level_data = {
                    level: pipeline.get_level_data(level)
                    for level in ("adm0", "adm1", "adm2")
                }
                assert [len(rows) for rows in level_data.values()] == [3, 51, 948]
                adm2_row = level_data["adm2"][0]
                assert list(adm2_row.keys())[:12] == [
                    "gaul0_code",
                    "gaul0_name",
                    "iso3_code",
                    "gaul1_code",
                    "gaul1_name",
                    "gaul2_code",
                    "gaul2_name",
                    "dekad_year",
                    "dekad_month",
                    "dekad_number",
                    "dekad_start",
                    "dekad_end",
                ]
                assert "ObjectId" not in adm2_row
                assert [row["iso3_code"] for row in level_data["adm0"]] == [
                    "MMR",
                    "MOZ",
                    "NGA",
                ]

                pipeline.add_pcodes(level_data)
                adm1_by_name = {
                    (row["iso3_code"], row["gaul1_name"]): row["admin1_pcode"]
                    for row in level_data["adm1"]
                }
                assert adm1_by_name[("MOZ", "Cabo Delgado")] == "MZ02"
                assert adm1_by_name[("NGA", "Abia")] == "NG001"
                # MMR is excluded from p-code matching
                assert adm1_by_name[("MMR", "Region")] is None
                adm2_by_name = {
                    (row["iso3_code"], row["gaul2_name"]): row["admin2_pcode"]
                    for row in level_data["adm2"]
                }
                assert adm2_by_name[("MOZ", "Ancuabe")] == "MZ0202"
                # Fuzzy match
                assert adm2_by_name[("NGA", "Barkin Ladi")] == "NG032001"
                assert adm2_by_name[("MOZ", "Waterbody")] is None
                assert adm2_by_name[("MMR", "Ayeyarwady")] is None

                dataset = pipeline.generate_dataset()
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset["name"] == "fao-eve-flood-exposure"
                assert dataset["title"] == "FAO EVE Flood Exposure"
                assert dataset["groups"] == [
                    {"name": "mmr"},
                    {"name": "moz"},
                    {"name": "nga"},
                ]
                assert dataset["subnational"] == "1"
                assert dataset["dataset_date"] == (
                    "[2026-10-01T00:00:00 TO 2026-10-10T23:59:59]"
                )
                assert [tag["name"] for tag in dataset["tags"]] == [
                    "affected area",
                    "affected population",
                    "agriculture-livestock",
                    "climate hazards",
                    "flooding",
                ]
                assert dataset["owner_org"] == "ed727a5b-3e6e-4cd6-b97e-4a71532085e6"
                assert dataset["maintainer"] == "169c44fe-3043-4616-8719-59cf9f101270"
                assert dataset["methodology"] == "Other"

                description = (
                    "Dekadal (10-day) flood exposure estimates - flooded area, cropland, "
                    "population and livestock, split into exceptional and seasonal "
                    "flooding - aggregated to "
                )
                adm0_desc = "country (ADM0) level"
                adm1_desc = (
                    "first-level administrative (ADM1) units, with FAO GAUL codes and, "
                    "where matched, COD p-codes,"
                )
                adm2_desc = adm1_desc.replace("first", "second").replace("ADM1", "ADM2")
                assert dataset.get_resources() == [
                    {
                        "name": "global_fao_eve_flood_exposure_adm0.csv",
                        "description": f"{description}{adm0_desc} for all covered countries.",
                        "format": "csv",
                    },
                    {
                        "name": "global_fao_eve_flood_exposure_adm1.csv",
                        "description": f"{description}{adm1_desc} for all covered countries.",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "global_fao_eve_flood_exposure_adm2.csv",
                        "description": f"{description}{adm2_desc} for all covered countries.",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "mmr_fao_eve_flood_exposure_adm2.csv",
                        "description": f"{description}{adm2_desc} for Myanmar.",
                        "format": "csv",
                    },
                    {
                        "name": "moz_fao_eve_flood_exposure_adm2.csv",
                        "description": f"{description}{adm2_desc} for Mozambique.",
                        "p_coded": True,
                        "format": "csv",
                    },
                    {
                        "name": "nga_fao_eve_flood_exposure_adm2.csv",
                        "description": f"{description}{adm2_desc} for Nigeria.",
                        "p_coded": True,
                        "format": "csv",
                    },
                ]

                for filename in (
                    "global_fao_eve_flood_exposure_adm0.csv",
                    "global_fao_eve_flood_exposure_adm1.csv",
                    "mmr_fao_eve_flood_exposure_adm2.csv",
                ):
                    assert_files_same(
                        join(fixtures_dir, filename), join(tempdir, filename)
                    )

    def test_error_retry(self, configuration, monkeypatch):
        monkeypatch.setattr(pipeline_module.time, "sleep", lambda _: None)
        error = {"error": {"code": 500, "message": "Internal error"}}
        retriever = FakeRetriever([error, error, {"count": 0}])
        pipeline = Pipeline(configuration, retriever, "")
        assert pipeline._download_json("url", {}, "file.json") == {"count": 0}
        assert retriever.calls == 3

        retriever = FakeRetriever([error])
        pipeline = Pipeline(configuration, retriever, "")
        with pytest.raises(ValueError, match="file.json: 500 Internal error"):
            pipeline._download_json("url", {}, "file.json")
        assert retriever.calls == 3

    def test_count_mismatch(self, configuration):
        row = {"OBJECTID": 1, "iso3_code": "MOZ", "dekad_start": "2026-10-01"}
        count = {"features": [{"attributes": {"count": 2, "max": 2}}]}
        retriever = FakeRetriever([count, {"features": [{"attributes": row}]}])
        pipeline = Pipeline(configuration, retriever, "")
        with pytest.raises(ValueError, match="retrieved 1 rows but server reports 2"):
            pipeline.get_level_data("adm0")

    def test_level_consistency(self, configuration, monkeypatch):
        def rows(*dekads):
            return [
                {"iso3_code": "MOZ", "dekad_start": dekad, "dekad_end": dekad}
                for dekad in dekads
            ]

        level_data = {
            "adm0": rows("2026-10-11", "2026-10-01"),
            "adm1": rows("2026-10-11", "2026-10-01"),
            "adm2": rows("2026-10-01"),
        }
        resources = {}
        pipeline = Pipeline(configuration, None, "")
        monkeypatch.setattr(pipeline, "get_level_data", lambda level: level_data[level])
        monkeypatch.setattr(pipeline, "add_pcodes", lambda _: None)
        monkeypatch.setattr(
            pipeline_module.Dataset, "add_country_locations", lambda *_: None
        )
        monkeypatch.setattr(
            pipeline,
            "_generate_resource",
            lambda _, rows, level, iso3: resources.setdefault(level, rows),
        )
        pipeline.generate_dataset()
        for level in ("adm0", "adm1", "adm2"):
            assert [row["dekad_start"] for row in resources[level]] == ["2026-10-01"]

        level_data["adm2"] = []
        assert pipeline.generate_dataset() is None
