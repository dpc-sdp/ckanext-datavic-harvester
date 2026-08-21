from __future__ import annotations

import json
from typing import Any
from unittest import mock

import pytest

from ckanext.datavic_harvester.harvesters import DataVicODSHarvester
from ckanext.datavic_harvester.harvesters.ods import MOCK_FILE


@pytest.fixture
def harvester() -> DataVicODSHarvester:
    return DataVicODSHarvester()


@pytest.fixture
def test_harvester() -> DataVicODSHarvester:
    """Harvester with offline fixture mode engaged."""
    harvester = DataVicODSHarvester()
    harvester._set_config(json.dumps({"test": True}))

    return harvester


class TestODSHarvester(object):
    def test_description_refine_markdown(self):
        harvester = DataVicODSHarvester()

        assert not harvester._description_refine(None)
        assert not harvester._description_refine("")
        assert (
            harvester._description_refine(
                "'<b>Yay</b> <a href=\"http://github.com\">GitHub</a>'"
            )
            == "'**Yay** [GitHub](http://github.com)'"
        )


class TestTestModeConfig:
    """The flag is ``test``, not ``debug`` - nothing in the ODS chain
    implements validate_config, so a wrong key fails silently."""

    def test_bare_instantiation_defaults_to_false(self):
        """Guards make_checkup(), which assigns self.config directly without
        going through _set_config."""
        assert DataVicODSHarvester().test is False

    def test_kwarg_enables_test_mode(self):
        assert DataVicODSHarvester(test=True).test is True

    @pytest.mark.parametrize(
        "config, expected",
        [
            ({"test": True}, True),
            ({"test": "true"}, True),
            ({"test": "True"}, True),
            ({"test": False}, False),
            ({"test": "false"}, False),
            ({"test": None}, False),
            ({}, False),
        ],
    )
    def test_set_config_reads_flag(
        self, harvester: DataVicODSHarvester, config: dict[str, Any], expected: bool
    ):
        harvester._set_config(json.dumps(config))

        assert harvester.test is expected

    def test_set_config_handles_empty_config(self, harvester: DataVicODSHarvester):
        assert harvester._set_config("") == {}
        assert harvester.test is False

    def test_set_config_returns_config_dict(self, harvester: DataVicODSHarvester):
        """import_stage uses the return value, so it must not be dropped."""
        config = {"test": True, "max_datasets": 5}

        assert harvester._set_config(json.dumps(config)) == config

    def test_debug_key_does_not_enable_test_mode(
        self, harvester: DataVicODSHarvester
    ):
        """Regression guard: "debug" is not the flag."""
        harvester._set_config(json.dumps({"debug": True}))

        assert harvester.test is False


class TestFixture:
    def test_fixture_is_valid_and_has_expected_shape(
        self, test_harvester: DataVicODSHarvester
    ):
        fixture = test_harvester._get_mocked_fixture()

        assert fixture["datasets"]
        assert fixture["exports"]

        for entry in fixture["datasets"]:
            # gather_stage reads these two paths directly.
            assert entry["dataset"]["dataset_id"]
            assert entry["dataset"]["metas"]["default"]["title"]
            # _pre_map_stage -> _get_dataset_links_data needs rel="self".
            assert any(link["rel"] == "self" for link in entry["links"])

    def test_every_dataset_has_an_exports_entry(
        self, test_harvester: DataVicODSHarvester
    ):
        fixture = test_harvester._get_mocked_fixture()
        dataset_ids = {d["dataset"]["dataset_id"] for d in fixture["datasets"]}

        assert dataset_ids <= set(fixture["exports"])

    def test_exports_only_contain_harvestable_formats(
        self, test_harvester: DataVicODSHarvester
    ):
        """The parent filters to these formats, so the fixture must not carry
        rdfxml/parquet/etc or it would misrepresent a real run."""
        formats = {"csv", "json", "xls", "geojson", "shp", "kml"}
        exports = test_harvester._get_mocked_fixture()["exports"]

        for links in exports.values():
            for link in links:
                assert link["rel"].lower() in formats
                assert link["href"]

    def test_fixture_reread_on_every_call(
        self, test_harvester: DataVicODSHarvester
    ):
        """Not cached: each call re-opens and re-parses the file."""
        first = test_harvester._get_mocked_fixture()
        second = test_harvester._get_mocked_fixture()

        assert first == second
        assert first is not second


class TestSearchDatasetsTestMode:
    def test_returns_fixture_datasets(self, test_harvester: DataVicODSHarvester):
        expected = test_harvester._get_mocked_fixture()["datasets"]

        assert test_harvester._search_datasets("https://example.com") == expected

    def test_makes_no_request(self, test_harvester: DataVicODSHarvester):
        with mock.patch.object(test_harvester, "_make_request") as mock_request:
            test_harvester._search_datasets("https://example.com")

        mock_request.assert_not_called()

    def test_max_datasets_is_not_applied_in_test_mode(
        self, test_harvester: DataVicODSHarvester
    ):
        """Known limitation: unlike the live path, the fixture path ignores
        max_datasets and always returns every fixture entry."""
        total = len(test_harvester._get_mocked_fixture()["datasets"])
        test_harvester.config["max_datasets"] = 1

        assert len(test_harvester._search_datasets("https://example.com")) == total

    def test_max_datasets_zero_returns_everything(
        self, test_harvester: DataVicODSHarvester
    ):
        total = len(test_harvester._get_mocked_fixture()["datasets"])
        test_harvester.config["max_datasets"] = 0

        assert len(test_harvester._search_datasets("https://example.com")) == total

    def test_delegates_to_parent_when_not_in_test_mode(
        self, harvester: DataVicODSHarvester
    ):
        harvester._set_config(json.dumps({}))

        with mock.patch(
            "ckanext.harvest_basket.harvesters.ods_harvester.ODSHarvester"
            "._search_datasets",
            return_value=[],
        ) as mock_super:
            harvester._search_datasets("https://example.com")

        mock_super.assert_called_once_with("https://example.com")


class TestGetAllResourceUrlsTestMode:
    def _exports_url(self, dataset_id: str) -> str:
        return (
            "https://data.ballarat.vic.gov.au/api/v2/catalog/datasets/"
            f"{dataset_id}/exports"
        )

    def test_extracts_dataset_id_from_exports_url(
        self, test_harvester: DataVicODSHarvester
    ):
        fixture = test_harvester._get_mocked_fixture()
        dataset_id = next(iter(fixture["exports"]))

        result = test_harvester._get_all_resource_urls(
            self._exports_url(dataset_id)
        )

        assert result == fixture["exports"][dataset_id]

    def test_handles_trailing_slash(self, test_harvester: DataVicODSHarvester):
        fixture = test_harvester._get_mocked_fixture()
        dataset_id = next(iter(fixture["exports"]))

        result = test_harvester._get_all_resource_urls(
            self._exports_url(dataset_id) + "/"
        )

        assert result == fixture["exports"][dataset_id]

    def test_unknown_dataset_returns_empty_list(
        self, test_harvester: DataVicODSHarvester
    ):
        assert (
            test_harvester._get_all_resource_urls(
                self._exports_url("no-such-dataset")
            )
            == []
        )

    def test_empty_link_returns_empty_list(
        self, test_harvester: DataVicODSHarvester
    ):
        assert test_harvester._get_all_resource_urls("") == []

    def test_makes_no_request(self, test_harvester: DataVicODSHarvester):
        dataset_id = next(iter(test_harvester._get_mocked_fixture()["exports"]))

        with mock.patch.object(test_harvester, "_make_request") as mock_request:
            test_harvester._get_all_resource_urls(self._exports_url(dataset_id))

        mock_request.assert_not_called()

    def test_url_matches_parent_endpoint_builder(
        self, test_harvester: DataVicODSHarvester
    ):
        """The id is parsed out of the URL the parent builds, so the two must
        stay in agreement."""
        dataset_id = next(iter(test_harvester._get_mocked_fixture()["exports"]))
        built = test_harvester._get_export_resource_url(
            "https://data.ballarat.vic.gov.au", dataset_id
        )

        assert test_harvester._get_all_resource_urls(built)


class TestResourceSizeTestMode:
    """Resource size in _fetch_resources: hardcoded to 1KB in test mode so
    no network call is made; real lookup otherwise."""

    def test_does_not_call_through_to_network(
        self, test_harvester: DataVicODSHarvester
    ):
        with mock.patch(
            "ckanext.datavic_harvester.harvesters.ods.get_resource_size"
        ) as mock_size:
            resources = test_harvester._fetch_resources(
                "https://example.com", [], {"id": "x", "title": "T"}
            )

        mock_size.assert_not_called()
        assert resources == []

    def test_calls_through_when_not_in_test_mode(
        self, harvester: DataVicODSHarvester
    ):
        harvester._set_config(json.dumps({}))
        res_links = [{"href": "https://example.com/a.csv", "rel": "csv"}]

        with mock.patch(
            "ckanext.datavic_harvester.harvesters.ods.get_resource_size",
            return_value=123,
        ) as mock_size:
            resources = harvester._fetch_resources(
                "https://example.com", res_links, {"id": "x", "title": "T"}
            )

        mock_size.assert_called_once()
        assert resources[0]["size"] == 123
        assert resources[0]["filesize"] == 123


class TestFetchResourcesTestMode:
    def _pkg_data(self, entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": "some-ckan-id",
            "title": entry["dataset"]["metas"]["default"]["title"],
            "origin_id": entry["dataset"]["dataset_id"],
        }

    def test_builds_resources_fully_offline(
        self, test_harvester: DataVicODSHarvester
    ):
        """End-to-end over the two mocked fetch-stage calls: no request is
        made and every resource gets the hardcoded test-mode size."""
        entry = test_harvester._get_mocked_fixture()["datasets"][0]
        dataset_id = entry["dataset"]["dataset_id"]
        source_url = "https://data.ballarat.vic.gov.au"

        res_links = test_harvester._get_all_resource_urls(
            test_harvester._get_export_resource_url(source_url, dataset_id)
        )

        with (
            mock.patch.object(test_harvester, "_make_request") as mock_request,
            mock.patch(
                "ckanext.datavic_harvester.harvesters.ods.get_resource_size"
            ) as mock_size,
        ):
            resources = test_harvester._fetch_resources(
                source_url, res_links, self._pkg_data(entry)
            )

        mock_request.assert_not_called()
        mock_size.assert_not_called()

        assert len(resources) == len(res_links)

        for res in resources:
            assert res["size"] == 1024
            assert res["filesize"] == 1024

    def test_csv_resource_gets_delimiter_and_real_size(
        self, test_harvester: DataVicODSHarvester
    ):
        entry = test_harvester._get_mocked_fixture()["datasets"][0]
        dataset_id = entry["dataset"]["dataset_id"]
        source_url = "https://data.ballarat.vic.gov.au"

        res_links = test_harvester._get_all_resource_urls(
            test_harvester._get_export_resource_url(source_url, dataset_id)
        )
        resources = test_harvester._fetch_resources(
            source_url, res_links, self._pkg_data(entry)
        )

        csv_resources = [r for r in resources if r["format"] == "CSV"]

        assert csv_resources

        for res in csv_resources:
            assert res["url"].endswith("?delimiter=%2C")
            assert res["size"] == 1024


def test_mock_file_constant_matches_bundled_fixture():
    assert MOCK_FILE == "ods_records.json"
