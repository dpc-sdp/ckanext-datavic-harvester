from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.tests.helpers import call_action

import ckanext.harvest.model as harvest_model
from ckanext.harvest_basket.harvesters import ODSHarvester as BaseODSHarvester

from ckanext.datavic_harvester.harvesters import DataVicODSHarvester
from ckanext.datavic_harvester.harvesters.base import (
    PRESERVE_PKG_FIELDS,
    get_object_extra,
)
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


@pytest.fixture
def purge_missing_config() -> dict[str, Any]:
    return {"purge_missing": True}


def delete_object_stub(package_id: str = "x", **kwargs: Any) -> SimpleNamespace:
    """A stub delete harvest object, flagged the way gather_stage flags them.

    content is None: package_id and guid live on the object itself, so a
    delete object carries no payload (same as DELWP).
    """
    return SimpleNamespace(
        package_id=package_id,
        extras=[SimpleNamespace(key="status", value="delete")],
        **kwargs,
    )


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


class TestIsDeleteObject:
    """The marker lives in a harvest object extra, not in content, so remote
    payload can never be mistaken for a delete instruction."""

    def test_true_for_marker(self, harvester: DataVicODSHarvester):
        obj = delete_object_stub()

        assert harvester._is_delete_object(obj) is True

    def test_false_for_real_ods_envelope_with_no_extras(
        self, harvester: DataVicODSHarvester, test_harvester: DataVicODSHarvester
    ):
        """The regression guard that matters: a genuine remote payload is not
        an instruction, whatever it contains."""
        entry = test_harvester._get_mocked_fixture()["datasets"][0]
        obj = SimpleNamespace(content=json.dumps(entry), extras=[])

        assert harvester._is_delete_object(obj) is False

    def test_false_for_content_claiming_delete_status(
        self, harvester: DataVicODSHarvester
    ):
        """A remote payload with a top-level status=delete must be ignored -
        this is the collision the extras channel closes."""
        obj = SimpleNamespace(
            content=json.dumps({"status": "delete", "package_id": "x"}),
            extras=[],
        )

        assert harvester._is_delete_object(obj) is False

    def test_false_for_none_object(self, harvester: DataVicODSHarvester):
        assert harvester._is_delete_object(None) is False

    def test_false_for_no_extras(self, harvester: DataVicODSHarvester):
        assert harvester._is_delete_object(SimpleNamespace(extras=[])) is False

    def test_false_for_unrelated_extra_key(self, harvester: DataVicODSHarvester):
        obj = SimpleNamespace(
            extras=[SimpleNamespace(key="something_else", value="delete")]
        )

        assert harvester._is_delete_object(obj) is False

    def test_false_for_other_status_value(self, harvester: DataVicODSHarvester):
        """DELWP uses status=new/change on non-delete objects."""
        obj = SimpleNamespace(extras=[SimpleNamespace(key="status", value="change")])

        assert harvester._is_delete_object(obj) is False


class TestFetchStagePassesThroughDeleteObjects:
    def test_delete_object_skips_pre_map_stage(self, harvester: DataVicODSHarvester):
        obj = delete_object_stub()

        with mock.patch.object(harvester, "_pre_map_stage") as mock_pre_map:
            result = harvester.fetch_stage(obj)

        mock_pre_map.assert_not_called()
        assert result is True

    def test_non_delete_object_delegates_to_parent(
        self, harvester: DataVicODSHarvester
    ):
        obj = SimpleNamespace(
            content=json.dumps({"foo": "bar"}),
            extras=[],
            source=SimpleNamespace(id="src-1", title="Source 1", type="ods"),
        )

        with mock.patch.object(
            BaseODSHarvester, "fetch_stage", return_value=True
        ) as mock_super:
            result = harvester.fetch_stage(obj)

        mock_super.assert_called_once_with(obj)
        assert result is True

    def test_adds_harvest_source_extras_on_success(
        self, harvester: DataVicODSHarvester
    ):
        obj = SimpleNamespace(
            content=json.dumps({"foo": "bar"}),
            extras=[],
            source=SimpleNamespace(
                id="source-id-1", title="Test Source", type="ods"
            ),
        )

        with mock.patch.object(BaseODSHarvester, "fetch_stage", return_value=True):
            harvester.fetch_stage(obj)

        extras = {
            e["key"]: e["value"] for e in json.loads(obj.content)["extras"]
        }
        assert extras["harvest_source_id"] == "source-id-1"
        assert extras["harvest_source_title"] == "Test Source"
        assert extras["harvest_source_type"] == "ods"

    def test_does_not_add_harvest_source_extras_when_fetch_fails(
        self, harvester: DataVicODSHarvester
    ):
        obj = SimpleNamespace(
            content=json.dumps({"foo": "bar"}),
            extras=[],
            source=SimpleNamespace(id="source-id-1", title="Test Source", type="ods"),
        )

        with mock.patch.object(BaseODSHarvester, "fetch_stage", return_value=False):
            result = harvester.fetch_stage(obj)

        assert result is False
        assert "extras" not in json.loads(obj.content)

    def test_does_not_duplicate_existing_harvest_source_extras(
        self, harvester: DataVicODSHarvester
    ):
        obj = SimpleNamespace(
            content=json.dumps(
                {
                    "extras": [
                        {"key": "harvest_source_id", "value": "stale-id"}
                    ]
                }
            ),
            extras=[],
            source=SimpleNamespace(id="source-id-1", title="Test Source", type="ods"),
        )

        with mock.patch.object(BaseODSHarvester, "fetch_stage", return_value=True):
            harvester.fetch_stage(obj)

        extras = json.loads(obj.content)["extras"]
        matching = [e for e in extras if e["key"] == "harvest_source_id"]
        assert len(matching) == 1
        assert matching[0]["value"] == "stale-id"


class TestPreMapStageRestoresState:
    def test_forces_state_active(self, test_harvester: DataVicODSHarvester):
        entry = test_harvester._get_mocked_fixture()["datasets"][0]
        dataset_id = entry["dataset"]["dataset_id"]
        source_url = "https://data.ballarat.vic.gov.au"

        package_dict = json.loads(json.dumps(entry))

        with mock.patch.object(
            test_harvester, "_get_all_resource_urls", return_value=[]
        ):
            test_harvester._pre_map_stage(package_dict, source_url)

        assert package_dict["state"] == "active"
        assert package_dict["origin_id"] == dataset_id


class TestGatherStagePurgeMissing:
    def test_pass_through_when_purge_missing_absent(
        self, harvester: DataVicODSHarvester
    ):
        harvester._set_config(json.dumps({}))

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["obj-1"]
        ):
            result = harvester.gather_stage(SimpleNamespace(id="job-1"))

        assert result == ["obj-1"]

    def test_pass_through_when_purge_missing_false(
        self, harvester: DataVicODSHarvester
    ):
        harvester._set_config(json.dumps({"purge_missing": False}))

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["obj-1"]
        ):
            result = harvester.gather_stage(SimpleNamespace(id="job-1"))

        assert result == ["obj-1"]

    def test_pass_through_when_no_object_ids(self, harvester: DataVicODSHarvester):
        harvester._set_config(json.dumps({"purge_missing": True}))

        with mock.patch.object(BaseODSHarvester, "gather_stage", return_value=[]):
            result = harvester.gather_stage(SimpleNamespace(id="job-1"))

        assert result == []

    def test_runs_in_test_mode_too(self, harvester: DataVicODSHarvester):
        """purge_missing is not disabled in test mode: fixture dataset_ids
        are namespaced with "fixture-" so they can never collide with a
        production package id (see generate_ods_fixture.py)."""
        harvester._set_config(json.dumps({"purge_missing": True, "test": True}))

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["obj-1"]
        ):
            with mock.patch(
                "ckanext.datavic_harvester.harvesters.ods"
                ".get_existing_guids_to_package_ids",
                return_value={},
            ) as mock_existing:
                result = harvester.gather_stage(
                    SimpleNamespace(id="job-1", source_id="source-1")
                )

        mock_existing.assert_called_once()
        assert result == ["obj-1"]

    def test_fixture_dataset_ids_are_namespaced(
        self, test_harvester: DataVicODSHarvester
    ):
        """Regression guard for the collision this namespacing prevents."""
        fixture = test_harvester._get_mocked_fixture()

        for entry in fixture["datasets"]:
            assert entry["dataset"]["dataset_id"].startswith("fixture-")

        for dataset_id in fixture["exports"]:
            assert dataset_id.startswith("fixture-")

    def test_fixture_dataset_titles_are_namespaced(
        self, test_harvester: DataVicODSHarvester
    ):
        """The id prefix alone stops the package id/slug colliding with
        production; the title is what a person actually sees in the CKAN
        UI, so it must be prefixed too or a fixture dataset is
        indistinguishable from a real Ballarat one when triaging."""
        fixture = test_harvester._get_mocked_fixture()

        for entry in fixture["datasets"]:
            title = entry["dataset"]["metas"]["default"]["title"]
            assert title.startswith("fixture-")


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestGatherStagePurgeMissingDb:
    def test_end_to_end_in_test_mode_against_real_fixture(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
    ):
        """Full stack, test mode, no mocking of gather_stage: a guid that
        was harvested previously but is no longer in the bundled fixture is
        queued for delete; guids still present in the fixture are not."""
        source = harvest_source_factory(
            config=json.dumps({"test": True, "purge_missing": True}),
            source_type="ods",
        )
        harvester._set_config(source.config)
        fixture_ids = {
            d["dataset"]["dataset_id"]
            for d in harvester._get_mocked_fixture()["datasets"]
        }
        assert "fixture-removed-dataset" not in fixture_ids

        removed_dataset = dataset_factory()
        still_present_dataset = dataset_factory()
        still_present_guid = next(iter(fixture_ids))

        existing_job = harvest_job_factory(source=source)
        harvest_model.HarvestObject(
            guid="fixture-removed-dataset",
            job=existing_job,
            package_id=removed_dataset["id"],
            current=True,
            content=json.dumps({"dataset": {"dataset_id": "fixture-removed-dataset"}}),
            report_status="added",
        ).save()
        harvest_model.HarvestObject(
            guid=still_present_guid,
            job=existing_job,
            package_id=still_present_dataset["id"],
            current=True,
            content=json.dumps({"dataset": {"dataset_id": still_present_guid}}),
            report_status="added",
        ).save()
        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        object_ids = harvester.gather_stage(new_job)

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.guid == "fixture-removed-dataset")
            .all()
        )
        assert len(delete_objects) == 1
        assert harvester._is_delete_object(delete_objects[0])
        assert delete_objects[0].package_id == removed_dataset["id"]

        no_delete_for_present = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.guid == still_present_guid)
            .filter(harvest_model.HarvestObject.package_id == still_present_dataset["id"])
            .all()
        )
        assert no_delete_for_present == []

        result = harvester.import_stage(delete_objects[0])
        assert result is True
        assert model.Package.get(removed_dataset["id"]).state == "deleted"

    def test_missing_dataset_queued_for_delete(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        purge_missing_config,
    ):
        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(purge_missing_config),
            source_type="ods",
        )

        existing_job = harvest_job_factory(source=source)
        existing_object = harvest_model.HarvestObject(
            guid="missing-guid",
            job=existing_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps({"dataset_id": "missing-guid"}),
            report_status="added",
        )
        existing_object.save()
        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        harvester._set_config(source.config)

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["unrelated-obj"]
        ):
            object_ids = harvester.gather_stage(new_job)

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.package_id == dataset["id"])
            .all()
        )

        assert len(object_ids) == 2
        assert len(delete_objects) == 1
        assert harvester._is_delete_object(delete_objects[0])
        assert delete_objects[0].guid == "missing-guid"
        assert delete_objects[0].content is None

        model.Session.refresh(existing_object)
        assert existing_object.current is False

    def test_missing_dataset_does_not_clear_current_on_other_source(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        purge_missing_config,
    ):
        """ODS guids are the raw remote dataset_id, not source-namespaced, so
        two sources can share a guid. purge_missing on one source must not
        flip current=False on the other source's HarvestObject for it."""
        dataset = dataset_factory()
        other_dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(purge_missing_config),
            source_type="ods",
        )
        other_source = harvest_source_factory(
            config=json.dumps(purge_missing_config),
            source_type="ods",
        )

        existing_job = harvest_job_factory(source=source)
        existing_object = harvest_model.HarvestObject(
            guid="shared-guid",
            job=existing_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps({"dataset_id": "shared-guid"}),
            report_status="added",
        )
        existing_object.save()
        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)

        other_job = harvest_job_factory(source=other_source)
        other_object = harvest_model.HarvestObject(
            guid="shared-guid",
            job=other_job,
            package_id=other_dataset["id"],
            current=True,
            content=json.dumps({"dataset_id": "shared-guid"}),
            report_status="added",
        )
        other_object.save()
        other_job.status = "Finished"
        other_job.gather_started = datetime.now(timezone.utc)
        other_job.gather_finished = datetime.now(timezone.utc)
        other_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        harvester._set_config(source.config)

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["unrelated-obj"]
        ):
            harvester.gather_stage(new_job)

        model.Session.refresh(existing_object)
        model.Session.refresh(other_object)
        assert existing_object.current is False
        assert other_object.current is True

    def test_dataset_still_in_source_is_not_deleted(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        purge_missing_config,
    ):
        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(purge_missing_config),
            source_type="ods",
        )

        existing_job = harvest_job_factory(source=source)
        harvest_model.HarvestObject(
            guid="still-present-guid",
            job=existing_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps({"dataset_id": "still-present-guid"}),
            report_status="added",
        ).save()
        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        harvester._set_config(source.config)

        def fake_gather_stage(self, job):
            obj = harvest_model.HarvestObject(
                guid="still-present-guid",
                job=job,
                content=json.dumps({"dataset_id": "still-present-guid"}),
            )
            obj.save()
            return [obj.id]

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", fake_gather_stage
        ):
            object_ids = harvester.gather_stage(new_job)

        assert len(object_ids) == 1

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.package_id == dataset["id"])
            .all()
        )
        assert delete_objects == []

    def test_already_deleted_package_not_requeued(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        purge_missing_config,
    ):
        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(purge_missing_config),
            source_type="ods",
        )

        existing_job = harvest_job_factory(source=source)
        harvest_model.HarvestObject(
            guid="already-deleted-guid",
            job=existing_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps({"dataset_id": "already-deleted-guid"}),
            report_status="added",
        ).save()
        existing_job.status = "Finished"
        existing_job.gather_started = datetime.now(timezone.utc)
        existing_job.gather_finished = datetime.now(timezone.utc)
        existing_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        deleted_package = model.Package.get(dataset["id"])
        assert deleted_package is not None
        deleted_package.state = "deleted"
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        harvester._set_config(source.config)

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["unrelated-obj"]
        ):
            object_ids = harvester.gather_stage(new_job)

        assert object_ids == ["unrelated-obj"]

    def test_survives_unchanged_runs_interleaved(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        purge_missing_config,
    ):
        # "Not modified" runs leave current=False, package_id=NULL rows; a
        # naive ORDER BY gathered.desc would miss the original mapping.
        dataset = dataset_factory()
        source = harvest_source_factory(
            config=json.dumps(purge_missing_config),
            source_type="ods",
        )

        first_job = harvest_job_factory(source=source)
        first_object = harvest_model.HarvestObject(
            guid="missing-guid",
            job=first_job,
            package_id=dataset["id"],
            current=True,
            content=json.dumps({"dataset_id": "missing-guid"}),
            gathered=datetime.now(timezone.utc) - timedelta(hours=2),
            report_status="added",
        )
        first_object.save()
        first_job.status = "Finished"
        first_job.gather_started = datetime.now(timezone.utc)
        first_job.gather_finished = datetime.now(timezone.utc)
        first_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        unchanged_job = harvest_job_factory(source=source)
        unchanged_object = harvest_model.HarvestObject(
            guid="missing-guid",
            job=unchanged_job,
            package_id=None,
            current=False,
            content=json.dumps({"dataset_id": "missing-guid"}),
            gathered=datetime.now(timezone.utc) - timedelta(hours=1),
            report_status="not modified",
        )
        unchanged_object.save()
        unchanged_job.status = "Finished"
        unchanged_job.gather_started = datetime.now(timezone.utc)
        unchanged_job.gather_finished = datetime.now(timezone.utc)
        unchanged_job.finished = datetime.now(timezone.utc)
        model.Session.commit()

        new_job = harvest_model.HarvestJob(source=source)
        model.Session.add(new_job)
        model.Session.commit()

        harvester._set_config(source.config)

        with mock.patch.object(
            BaseODSHarvester, "gather_stage", return_value=["unrelated-obj"]
        ):
            object_ids = harvester.gather_stage(new_job)

        delete_objects = (
            model.Session.query(harvest_model.HarvestObject)
            .filter(harvest_model.HarvestObject.harvest_job_id == new_job.id)
            .filter(harvest_model.HarvestObject.package_id == dataset["id"])
            .all()
        )

        assert len(object_ids) == 2
        assert len(delete_objects) == 1
        assert harvester._is_delete_object(delete_objects[0])
        assert delete_objects[0].guid == "missing-guid"


@pytest.mark.usefixtures("with_plugins", "clean_db")
class TestImportStageDelete:
    def test_deletes_package(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
    ):
        dataset = dataset_factory()
        harvest_object = delete_object_stub(
            package_id=dataset["id"], id="delete-obj-1"
        )

        result = harvester.import_stage(harvest_object)

        assert result is True
        deleted_package = model.Package.get(dataset["id"])
        assert deleted_package.state == "deleted"

    def test_missing_package_id_is_a_noop(self, harvester: DataVicODSHarvester):
        harvest_object = delete_object_stub(package_id=None, id="delete-obj-2")

        assert harvester.import_stage(harvest_object) is True

    def test_already_purged_package_is_a_noop(
        self, harvester: DataVicODSHarvester
    ):
        harvest_object = delete_object_stub(
            package_id="no-such-package", id="delete-obj-3"
        )

        assert harvester.import_stage(harvest_object) is True

    def test_non_delete_object_delegates_to_parent(
        self, harvester: DataVicODSHarvester
    ):
        harvest_object = SimpleNamespace(
            content=json.dumps({"foo": "bar"}), extras=[]
        )

        with mock.patch.object(
            BaseODSHarvester, "import_stage", return_value=True
        ) as mock_super:
            result = harvester.import_stage(harvest_object)

        mock_super.assert_called_once_with(harvest_object)
        assert result is True


class TestFindExistingPackagePreservesFields:
    """package_update is a full replace, so PRESERVE_PKG_FIELDS must be
    carried forward from the existing package or they are silently dropped -
    most importantly syndicated_id, which breaks the syndication link."""

    def test_raises_object_not_found_on_create(
        self, harvester: DataVicODSHarvester
    ):
        """On create there is nothing to preserve, and the preserve loop
        must not swallow the ObjectNotFound the parent raises."""
        with mock.patch.object(
            BaseODSHarvester,
            "_find_existing_package",
            side_effect=tk.ObjectNotFound,
        ):
            with pytest.raises(tk.ObjectNotFound):
                harvester._find_existing_package({"id": "does-not-exist"})

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_preserves_fields_absent_from_incoming_dict(
        self, harvester: DataVicODSHarvester, dataset_factory
    ):
        dataset = dataset_factory(
            syndicated_id="remote-portal-uuid-abc123",
            skip_syndication="true",
            custom_licence_link="https://example.com/licence",
        )
        package_dict = {"id": dataset["id"]}

        harvester._find_existing_package(package_dict)

        assert package_dict["syndicated_id"] == "remote-portal-uuid-abc123"
        assert package_dict["skip_syndication"] == "true"
        assert package_dict["custom_licence_link"] == "https://example.com/licence"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_does_not_overwrite_fields_the_harvester_already_set(
        self, harvester: DataVicODSHarvester, dataset_factory
    ):
        """setdefault semantics: harvest data always wins over existing data
        for fields the harvester itself populated - e.g. workflow_status and
        custom_licence_* are both set by the Test-ODS tsm_schema."""
        dataset = dataset_factory(
            workflow_status="draft",
            custom_licence_text="Old licence text",
        )
        package_dict = {
            "id": dataset["id"],
            "workflow_status": "published",
            "custom_licence_text": "CC BY 3.0",
        }

        harvester._find_existing_package(package_dict)

        assert package_dict["workflow_status"] == "published"
        assert package_dict["custom_licence_text"] == "CC BY 3.0"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_extras_merge_keeps_existing_only_extras(
        self, harvester: DataVicODSHarvester, dataset_factory
    ):
        dataset = dataset_factory(
            extras=[{"key": "custom_free_form", "value": "keep-me"}]
        )
        package_dict = {
            "id": dataset["id"],
            "extras": [{"key": "harvester_set", "value": "harvester-wins"}],
        }

        harvester._find_existing_package(package_dict)

        extras_by_key = {e["key"]: e["value"] for e in package_dict["extras"]}
        assert extras_by_key["custom_free_form"] == "keep-me"
        assert extras_by_key["harvester_set"] == "harvester-wins"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_extras_merge_does_not_duplicate_harvester_keys(
        self, harvester: DataVicODSHarvester, dataset_factory
    ):
        dataset = dataset_factory(
            extras=[{"key": "shared_key", "value": "existing-value"}]
        )
        package_dict = {
            "id": dataset["id"],
            "extras": [{"key": "shared_key", "value": "harvester-value"}],
        }

        harvester._find_existing_package(package_dict)

        matching = [e for e in package_dict["extras"] if e["key"] == "shared_key"]
        assert len(matching) == 1
        assert matching[0]["value"] == "harvester-value"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_syndicated_id_survives_a_full_import_stage_update(
        self,
        harvester: DataVicODSHarvester,
        dataset_factory,
        harvest_source_factory,
        harvest_job_factory,
        organization_factory,
        group,
    ):
        """End-to-end regression for the reported bug: syndicated_id set
        after the first harvest must still be present after a second
        harvest updates the same dataset.

        No tsm_schema is configured (ckanext-transmute is not loaded in
        test.ini), so required fields the real Test-ODS tsm_schema would
        supply are patched directly onto the fetch_stage output instead.
        """
        org = organization_factory()
        required_fields = {
            "category": group["id"],
            "owner_org": org["id"],
            "license_id": "other-open",
            "date_created_data_asset": "2020-01-01T00:00:00",
            "update_frequency": "unknown",
            "dtv_preview": False,
            "personal_information": "no",
            "protective_marking": "official",
            "access": "yes",
            "organization_visibility": "all",
            "workflow_status": "published",
            "data_owner": "Test Owner",
            "contact_point": "test@example.com",
            "extract": "Test extract",
        }

        source = harvest_source_factory(
            config=json.dumps({"test": True}), source_type="ods"
        )
        harvester._set_config(source.config)
        entry = harvester._get_mocked_fixture()["datasets"][0]
        dataset_id = entry["dataset"]["dataset_id"]

        def _fetch_and_patch(job, package_id=None):
            obj = harvest_model.HarvestObject(
                guid=dataset_id,
                job=job,
                content=json.dumps(entry),
                package_id=package_id,
            )
            obj.save()

            assert harvester.fetch_stage(obj) is True

            package_dict = json.loads(obj.content)
            package_dict.update(required_fields)
            obj.content = json.dumps(package_dict)
            obj.save()

            return obj

        job = harvest_job_factory(source=source)
        first_object = _fetch_and_patch(job)

        result = harvester.import_stage(first_object)
        assert result is True, first_object.errors
        package_id = first_object.package_id
        assert package_id

        # Regression guard: without _add_harvest_source_extras, the harvest
        # source listing page (/harvest/<source>) never shows ODS datasets,
        # because it filters Solr on harvest_source_id and nothing persists
        # that field on the package (see ods.py:_add_harvest_source_extras).
        pkg_after_first = call_action("package_show", id=package_id)
        first_extras = {e["key"]: e["value"] for e in pkg_after_first["extras"]}
        assert first_extras["harvest_source_id"] == source.id
        assert first_extras["harvest_source_title"] == source.title
        assert first_extras["harvest_source_type"] == "ods"

        sysadmin = call_action("get_site_user", ignore_auth=True)
        call_action(
            "package_patch",
            {"user": sysadmin["name"]},
            id=package_id,
            syndicated_id="remote-portal-uuid-abc123",
        )
        assert (
            call_action("package_show", id=package_id).get("syndicated_id")
            == "remote-portal-uuid-abc123"
        )

        second_object = _fetch_and_patch(job, package_id=package_id)

        result2 = harvester.import_stage(second_object)
        assert result2 is True, second_object.errors

        pkg_after_update = call_action("package_show", id=package_id)
        assert pkg_after_update.get("syndicated_id") == "remote-portal-uuid-abc123"

        update_extras = {e["key"]: e["value"] for e in pkg_after_update["extras"]}
        assert update_extras["harvest_source_id"] == source.id
        assert update_extras["harvest_source_title"] == source.title
        assert update_extras["harvest_source_type"] == "ods"


def test_ods_preserve_fields_is_the_shared_constant():
    """Guards against re-divergence: ODS must use the same list as base.py,
    not a locally re-declared or narrowed copy."""
    from ckanext.datavic_harvester.harvesters import base as base_module
    from ckanext.datavic_harvester.harvesters import ods as ods_module

    assert ods_module.PRESERVE_PKG_FIELDS is base_module.PRESERVE_PKG_FIELDS


def test_ods_add_harvest_source_extras_is_the_shared_function():
    """Guards against re-divergence: ODS must call the shared helper in
    base.py, not a locally re-implemented copy."""
    from ckanext.datavic_harvester.harvesters import base as base_module
    from ckanext.datavic_harvester.harvesters import ods as ods_module

    assert (
        ods_module.add_harvest_source_extras
        is base_module.add_harvest_source_extras
    )


def test_ods_get_object_extra_is_the_shared_function():
    """ODS extends ODSHarvester, so it does not inherit
    DataVicBaseHarvester._get_object_extra - it must use the module-level
    helper. Calling self._get_object_extra would raise AttributeError."""
    from ckanext.datavic_harvester.harvesters import base as base_module
    from ckanext.datavic_harvester.harvesters import ods as ods_module

    assert ods_module.get_object_extra is base_module.get_object_extra
    assert not hasattr(DataVicODSHarvester, "_get_object_extra")


def test_method_form_retained_for_other_harvesters():
    """base.py's _get_object_extra now delegates to the module-level helper;
    DELWP and dcat_json call it as a method, so it must stay on the class.
    Its behaviour is covered by their suites."""
    from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester

    assert hasattr(DataVicBaseHarvester, "_get_object_extra")


class TestGetObjectExtra:
    def test_returns_value_for_matching_key(self):
        obj = SimpleNamespace(extras=[SimpleNamespace(key="status", value="delete")])

        assert get_object_extra(obj, "status") == "delete"

    def test_returns_none_for_absent_key(self):
        obj = SimpleNamespace(extras=[SimpleNamespace(key="status", value="delete")])

        assert get_object_extra(obj, "absent") is None

    def test_returns_none_for_empty_extras(self):
        assert get_object_extra(SimpleNamespace(extras=[]), "status") is None
