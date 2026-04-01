from __future__ import annotations

import json
from typing import Any
from typing_extensions import TypedDict
from types import GeneratorType
from datetime import datetime as dt
from unittest import mock

import pytest

from ckan import model
from ckan.tests.helpers import call_action

import ckanext.harvest.model as harvest_model

import ckanext.datavic_harvester.helpers as h
from ckanext.datavic_harvester.harvesters import DelwpHarvester


class DelwpConfig(TypedDict):
    default_groups: list[str]
    default_group_dicts: dict[str, Any]
    full_metadata_url_prefix: str
    resource_url_prefix: str
    resource_attribution: str
    license_id: str
    dataset_type: str
    api_auth: str
    geoserver_dns: str
    organisation_mapping: list[dict[str, str]]
    deletion_safeguard_enabled: bool
    deletion_safeguard_drop_threshold_percent: float
    deletion_safeguard_min_previous_count: int
    deletion_safeguard_allow_mass_delete: bool
    deletion_safeguard_notify_ok_url: str
    deletion_safeguard_notify_anomaly_url: str


@pytest.fixture
def harvester():
    harvester = DelwpHarvester()

    # _set_config now expects a HarvestJob/HarvestObject with source.config and
    # source.id. Use a mock so the fixture works without a live DB / harvest source.
    mock_item = mock.MagicMock()
    mock_item.source.config = json.dumps({"dataset_type": "delwp", "test": True})
    with mock.patch.object(harvester, "_get_source_owner_org_id", return_value=None):
        harvester._set_config(mock_item)

    return harvester


@pytest.fixture
def delwp_dataset(harvester: DelwpHarvester):
    records = harvester._fetch_records("test_url", 0, 0)
    datasets = harvester._get_record_metadata(records)
    dataset = next(datasets)
    # All mock records have accesscontrol_restricted=None/orderableondatashare=None
    # which _is_pkg_private treats as private, causing import_stage to skip them.
    # Override to make the fixture represent a public (importable) dataset.
    dataset["accesscontrol_restricted"] = False
    dataset["orderableondatashare"] = True
    return dataset


class TestDelwpHarvester:
    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage(
        self,
        harvester: DelwpHarvester,
        harvest_job_factory,
        harvest_source_factory,
        delwp_config: DelwpConfig,
    ):
        source = harvest_source_factory(
            config=json.dumps(delwp_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        obj_ids = harvester.gather_stage(harvest_job)

        assert harvest_job.gather_errors == []
        assert type(obj_ids) == list

        datasets = json.loads(harvester._get_mocked_records())["records"]
        assert len(set(obj_ids)) == len(datasets)

        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert harvest_object.guid == datasets[0]["fields"]["uuid"]
        assert json.loads(harvest_object.content) == datasets[0]["fields"]

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        delwp_config: DelwpConfig,
        delwp_dataset: dict[str, Any],
    ):
        source = harvest_source_factory(
            config=json.dumps(delwp_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        harvest_object = harvest_object_factory(
            guid=delwp_dataset["uuid"],
            content=json.dumps(delwp_dataset),
            job=harvest_job,
        )

        result = harvester.import_stage(harvest_object)

        assert harvest_object.errors == []
        assert result is True
        assert harvest_object.package_id

        package = model.Package.get(harvest_object.package_id)

        assert package
        assert package.name == h.munge_title_to_name(delwp_dataset["title"])
        assert package.extras["primary_purpose_of_collection"] == delwp_dataset["uuid"]

        # no new harvest_object, cause it's update
        assert harvest_model.HarvestObject.filter(guid=delwp_dataset["uuid"]).one()

    def test_mock_geores_data(self, harvester: DelwpHarvester):
        """The geoserver_url doesn't matter, because we're mocking response.
        The `content` with uuid below exists in test data"""
        assert harvester._get_geoserver_content_with_uuid(
            "geoserver_url", "8ad36246-9a39-53aa-bcbc-8b33aec63cde"
        )

        assert not harvester._get_geoserver_content_with_uuid("geoserver_url", "uuid")

    def test_mock_records_data(self, harvester: DelwpHarvester):
        """The actual params doesn't matter, because we're mocking response"""
        records = harvester._fetch_records("test_url", 0, 0)

        assert records
        assert len(records) > 0

        harvester._get_record_metadata(records)

    def test_get_record_metadata(self, harvester: DelwpHarvester):
        records = harvester._fetch_records("test_url", 0, 0)
        datasets = harvester._get_record_metadata(records)

        assert isinstance(datasets, GeneratorType)

        dataset: dict[str, Any] = next(datasets)

        assert dataset["uuid"]
        assert dataset["title"]
        remaining = list(datasets)
        assert len(remaining) == len(records) - 1

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_get_pkg_dict(
        self,
        harvester: DelwpHarvester,
        harvest_object_factory,
        delwp_config: DelwpConfig,
        delwp_dataset: dict[str, Any],
    ):
        harvester.config = delwp_config
        harvest_object = harvest_object_factory(
            guid=delwp_dataset["uuid"],
            content=json.dumps(delwp_dataset),
        )

        pkg_dict = harvester._get_pkg_dict(harvest_object)

        assert pkg_dict["primary_purpose_of_collection"] == delwp_dataset["uuid"]
        assert pkg_dict["title"] == delwp_dataset["title"]
        assert pkg_dict["name"] == h.munge_title_to_name(delwp_dataset["title"])
        if pkg_dict["private"]:
            assert pkg_dict["license_id"] == "other-closed"
        else:
            assert pkg_dict["license_id"] == delwp_config["license_id"]
        assert pkg_dict["notes"] == delwp_dataset["abstract"]
        assert pkg_dict["extract"].rstrip(".") in f"{delwp_dataset['abstract']}"
        assert pkg_dict["category"] in delwp_config["default_groups"]
        assert dt.fromisoformat(pkg_dict["date_created_data_asset"])
        assert dt.fromisoformat(pkg_dict["date_modified_data_asset"])

        assert pkg_dict["personal_information"] == "no"
        assert pkg_dict["protective_marking"] == "official"
        assert pkg_dict["access"] == "yes"
        assert pkg_dict["organization_visibility"] == "all"
        assert pkg_dict["workflow_status"] == "published"

        assert pkg_dict["resources"]
        assert pkg_dict["resources"][0]["format"]
        assert pkg_dict["resources"][0]["period_end"]
        assert pkg_dict["resources"][0]["period_start"]
        resource_url: str = pkg_dict["resources"][0]["url"]
        assert resource_url
        assert delwp_config["resource_url_prefix"] in resource_url
        assert delwp_dataset["uuid"] in resource_url
        assert (
            pkg_dict["resources"][0]["attribution"]
            == delwp_config["resource_attribution"]
        )

        assert pkg_dict["full_metadata_url"] == delwp_config[
            "full_metadata_url_prefix"
        ].format(UUID=delwp_dataset["uuid"])

        for tag in pkg_dict["tags"]:
            assert tag["name"] in delwp_dataset["topiccat"]

        organization = call_action("organization_show", id=pkg_dict["owner_org"])
        assert organization["title"] == delwp_dataset["resowner"]

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_get_existing_organization_exist(
        self, harvester: DelwpHarvester, delwp_config: DelwpConfig
    ):
        resowner: str = delwp_config["organisation_mapping"][0]["resowner"]
        orgname: str = delwp_config["organisation_mapping"][0]["org-name"]

        assert (
            harvester._get_existing_organization(
                delwp_config["organisation_mapping"], resowner
            )
            == orgname
        )

    def test_get_existing_organization_missing(self, harvester: DelwpHarvester):
        harvester.pkg_dict = {"title": "test"}
        assert not harvester._get_existing_organization(
            [{"resowner": "test", "org-name": "test"}], "whatever"
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_create_organization(self, harvester: DelwpHarvester, harvest_object):
        assert harvester._create_organization("organization title", harvest_object)

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_create_organization_error(
        self, harvester: DelwpHarvester, harvest_object, organization_factory
    ):
        resowner = "organization title"
        organization_factory(name=h.munge_title_to_name(resowner))

        # Set source_org_id so the fallback in _create_organization is meaningful.
        source = call_action("package_show", id=harvest_object.harvest_source_id)
        harvester.source_org_id = source["owner_org"]

        org_id: str = harvester._create_organization(resowner, harvest_object)

        assert source["owner_org"] == org_id

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_anomaly_skips_deletes_and_adds_gather_error(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dataset_factory,
        delwp_config,
    ):
        cfg = dict(delwp_config)
        cfg.update(
            {
                "deletion_safeguard_enabled": True,
                "deletion_safeguard_drop_threshold_percent": 50,
                "deletion_safeguard_min_previous_count": 1,
                "deletion_safeguard_allow_bulk_delete": False,
                "deletion_safeguard_notify_ok_url": "https://notify.example/ok",
                "deletion_safeguard_notify_anomaly_url": "https://notify.example/anomaly",
            }
        )
        source = harvest_source_factory(
            config=json.dumps(cfg), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)

        existing = {
            "guid-a": dataset_factory()["id"],
            "guid-b": dataset_factory()["id"],
            "guid-c": dataset_factory()["id"],
            "guid-d": dataset_factory()["id"],
        }
        records = [{"fields": {"uuid": "guid-a", "title": "t"}}]

        with (
            mock.patch.object(
                harvester, "_get_guids_to_package_ids", return_value=existing
            ),
            mock.patch.object(
                harvester, "_fetch_records_from_remote_portal", return_value=records
            ),
            mock.patch.object(harvester, "_save_harvest_json_to_filestore"),
            mock.patch(
                "ckanext.datavic_harvester.harvesters.delwp.requests.get"
            ) as mock_get,
        ):
            obj_ids = harvester.gather_stage(harvest_job)

        objects = [harvest_model.HarvestObject.get(obj_id) for obj_id in obj_ids]
        statuses = [harvester._get_object_extra(obj, "status") for obj in objects]

        assert statuses == ["change"]
        assert len(harvest_job.gather_errors) == 1
        assert "anomaly detected" in harvest_job.gather_errors[0].message
        mock_get.assert_called_once_with(
            "https://notify.example/anomaly", timeout=10
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_anomaly_allow_mass_delete(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        delwp_config,
        dataset_factory,
    ):
        cfg = dict(delwp_config)
        cfg.update(
            {
                "deletion_safeguard_enabled": True,
                "deletion_safeguard_drop_threshold_percent": 50,
                "deletion_safeguard_min_previous_count": 1,
                "deletion_safeguard_allow_bulk_delete": True,
                "deletion_safeguard_notify_ok_url": "https://notify.example/ok",
                "deletion_safeguard_notify_anomaly_url": "https://notify.example/anomaly",
            }
        )
        source = harvest_source_factory(
            config=json.dumps(cfg), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)

        existing = {
            "guid-a": dataset_factory()["id"],
            "guid-b": dataset_factory()["id"],
            "guid-c": dataset_factory()["id"],
            "guid-d": dataset_factory()["id"],
        }
        records = [{"fields": {"uuid": "guid-a", "title": "t"}}]

        with (
            mock.patch.object(
                harvester, "_get_guids_to_package_ids", return_value=existing
            ),
            mock.patch.object(
                harvester, "_fetch_records_from_remote_portal", return_value=records
            ),
            mock.patch.object(harvester, "_save_harvest_json_to_filestore"),
            mock.patch(
                "ckanext.datavic_harvester.harvesters.delwp.requests.get"
            ) as mock_get,
        ):
            obj_ids = harvester.gather_stage(harvest_job)

        objects = [harvest_model.HarvestObject.get(obj_id) for obj_id in obj_ids]
        statuses = [harvester._get_object_extra(obj, "status") for obj in objects]

        assert statuses.count("change") == 1
        assert statuses.count("delete") == 3
        assert len(harvest_job.gather_errors) == 1
        mock_get.assert_called_once_with(
            "https://notify.example/anomaly", timeout=10
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_safeguard_disabled_allows_deletes(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dataset_factory,
        delwp_config,
    ):
        """When deletion_safeguard_enabled=False all deletes proceed even for
        large drops, and no gather error is added."""
        cfg = dict(delwp_config)
        cfg.update(
            {
                "deletion_safeguard_enabled": False,
            }
        )
        source = harvest_source_factory(
            config=json.dumps(cfg), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)

        existing = {
            "guid-a": dataset_factory()["id"],
            "guid-b": dataset_factory()["id"],
            "guid-c": dataset_factory()["id"],
            "guid-d": dataset_factory()["id"],
        }
        # Only guid-a comes back — 75% drop, but safeguard is disabled
        records = [{"fields": {"uuid": "guid-a", "title": "t"}}]

        with (
            mock.patch.object(
                harvester, "_get_guids_to_package_ids", return_value=existing
            ),
            mock.patch.object(
                harvester, "_fetch_records_from_remote_portal", return_value=records
            ),
            mock.patch.object(harvester, "_save_harvest_json_to_filestore"),
        ):
            obj_ids = harvester.gather_stage(harvest_job)

        objects = [harvest_model.HarvestObject.get(obj_id) for obj_id in obj_ids]
        statuses = [harvester._get_object_extra(obj, "status") for obj in objects]

        assert statuses.count("change") == 1
        assert statuses.count("delete") == 3
        assert harvest_job.gather_errors == []

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_below_min_previous_count_allows_deletes(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dataset_factory,
        delwp_config,
    ):
        """When previous count is below min_previous_count the safeguard check
        is skipped and deletes proceed normally."""
        cfg = dict(delwp_config)
        cfg.update(
            {
                "deletion_safeguard_enabled": True,
                "deletion_safeguard_drop_threshold_percent": 10,
                "deletion_safeguard_min_previous_count": 10,
                "deletion_safeguard_allow_mass_delete": False,
            }
        )
        source = harvest_source_factory(
            config=json.dumps(cfg), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)

        # Only 3 existing — below min_previous_count of 10
        existing = {
            "guid-a": dataset_factory()["id"],
            "guid-b": dataset_factory()["id"],
            "guid-c": dataset_factory()["id"],
        }
        records = [{"fields": {"uuid": "guid-a", "title": "t"}}]

        with (
            mock.patch.object(
                harvester, "_get_guids_to_package_ids", return_value=existing
            ),
            mock.patch.object(
                harvester, "_fetch_records_from_remote_portal", return_value=records
            ),
            mock.patch.object(harvester, "_save_harvest_json_to_filestore"),
        ):
            obj_ids = harvester.gather_stage(harvest_job)

        objects = [harvest_model.HarvestObject.get(obj_id) for obj_id in obj_ids]
        statuses = [harvester._get_object_extra(obj, "status") for obj in objects]

        assert statuses.count("change") == 1
        assert statuses.count("delete") == 2
        assert harvest_job.gather_errors == []

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_no_anomaly_sends_healthy_ping(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dataset_factory,
        delwp_config,
    ):
        cfg = dict(delwp_config)
        cfg.update(
            {
                "deletion_safeguard_enabled": True,
                "deletion_safeguard_drop_threshold_percent": 50,
                "deletion_safeguard_min_previous_count": 1,
                "deletion_safeguard_allow_bulk_delete": False,
                "deletion_safeguard_notify_ok_url": "https://notify.example/ok",
                "deletion_safeguard_notify_anomaly_url": "https://notify.example/anomaly",
            }
        )
        source = harvest_source_factory(
            config=json.dumps(cfg), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)

        existing = {
            "guid-a": dataset_factory()["id"],
            "guid-b": dataset_factory()["id"],
            "guid-c": dataset_factory()["id"],
        }
        records = [
            {"fields": {"uuid": "guid-a", "title": "a"}},
            {"fields": {"uuid": "guid-b", "title": "b"}},
            {"fields": {"uuid": "guid-c", "title": "c"}},
        ]

        with (
            mock.patch.object(
                harvester, "_get_guids_to_package_ids", return_value=existing
            ),
            mock.patch.object(
                harvester, "_fetch_records_from_remote_portal", return_value=records
            ),
            mock.patch.object(harvester, "_save_harvest_json_to_filestore"),
            mock.patch(
                "ckanext.datavic_harvester.harvesters.delwp.requests.get"
            ) as mock_get,
        ):
            obj_ids = harvester.gather_stage(harvest_job)

        assert len(obj_ids) == 3
        assert harvest_job.gather_errors == []
        mock_get.assert_called_once_with("https://notify.example/ok", timeout=10)


class TestDetectDeletionAnomaly:
    """Unit tests for _detect_deletion_anomaly — no DB or plugins required."""

    def _make_harvester(self, **kwargs) -> DelwpHarvester:
        h = DelwpHarvester()
        h.deletion_safeguard_enabled = kwargs.get("deletion_safeguard_enabled", True)
        h.deletion_safeguard_drop_threshold_percent = kwargs.get(
            "deletion_safeguard_drop_threshold_percent", 10.0
        )
        h.deletion_safeguard_min_previous_count = kwargs.get(
            "deletion_safeguard_min_previous_count", 100
        )
        h.deletion_safeguard_allow_mass_delete = kwargs.get(
            "deletion_safeguard_allow_mass_delete", False
        )
        return h

    def test_safeguard_disabled_returns_no_anomaly(self):
        h = self._make_harvester(deletion_safeguard_enabled=False)
        detected, msg = h._detect_deletion_anomaly(previous_count=1000, source_count=0)
        assert detected is False
        assert msg is None

    def test_below_min_previous_count_skips_check(self):
        h = self._make_harvester(deletion_safeguard_min_previous_count=100)
        detected, msg = h._detect_deletion_anomaly(previous_count=50, source_count=0)
        assert detected is False
        assert msg is None

    def test_previous_count_zero_with_min_zero(self):
        """When min_previous_count=0 and previous_count=0 the zero-division
        guard fires and the check is skipped."""
        h = self._make_harvester(deletion_safeguard_min_previous_count=0)
        detected, msg = h._detect_deletion_anomaly(previous_count=0, source_count=0)
        assert detected is False
        assert msg is None

    def test_drop_within_threshold_returns_no_anomaly(self):
        h = self._make_harvester(
            deletion_safeguard_drop_threshold_percent=10.0,
            deletion_safeguard_min_previous_count=0,
        )
        # 5% drop — within 10% threshold
        detected, msg = h._detect_deletion_anomaly(previous_count=100, source_count=95)
        assert detected is False
        assert msg is None

    def test_drop_at_exact_threshold_is_not_anomaly(self):
        h = self._make_harvester(
            deletion_safeguard_drop_threshold_percent=10.0,
            deletion_safeguard_min_previous_count=0,
        )
        # exactly 10% drop — at threshold, not above
        detected, msg = h._detect_deletion_anomaly(previous_count=100, source_count=90)
        assert detected is False
        assert msg is None

    def test_drop_above_threshold_returns_anomaly(self):
        h = self._make_harvester(
            deletion_safeguard_drop_threshold_percent=10.0,
            deletion_safeguard_min_previous_count=0,
        )
        # 50% drop — exceeds 10% threshold
        detected, msg = h._detect_deletion_anomaly(
            previous_count=100, source_count=50
        )
        assert detected is True
        assert msg is not None
        assert "anomaly detected" in msg

    def test_source_count_larger_than_previous_is_not_anomaly(self):
        """Growing datasets (source > previous) should never trigger anomaly."""
        h = self._make_harvester(
            deletion_safeguard_drop_threshold_percent=10.0,
            deletion_safeguard_min_previous_count=0,
        )
        detected, msg = h._detect_deletion_anomaly(
            previous_count=100, source_count=150
        )
        assert detected is False
        assert msg is None


class TestSendDeletionSafeguardNotify:
    """Unit tests for _send_deletion_safeguard_notify — no DB or plugins required."""

    def _make_harvester(self, ok_url=None, anomaly_url=None) -> DelwpHarvester:
        h = DelwpHarvester()
        h.deletion_safeguard_notify_ok_url = ok_url
        h.deletion_safeguard_notify_anomaly_url = anomaly_url
        return h

    def test_no_urls_configured_makes_no_request(self):
        h = self._make_harvester()
        with mock.patch(
            "ckanext.datavic_harvester.harvesters.delwp.requests.get"
        ) as mock_get:
            h._send_deletion_safeguard_notify(anomaly=False)
            h._send_deletion_safeguard_notify(anomaly=True)
        mock_get.assert_not_called()

    def test_ok_url_called_when_no_anomaly(self):
        h = self._make_harvester(ok_url="https://notify.example/ok")
        with mock.patch(
            "ckanext.datavic_harvester.harvesters.delwp.requests.get"
        ) as mock_get:
            h._send_deletion_safeguard_notify(anomaly=False)
        mock_get.assert_called_once_with("https://notify.example/ok", timeout=10)

    def test_anomaly_url_called_when_anomaly(self):
        h = self._make_harvester(anomaly_url="https://notify.example/anomaly")
        with mock.patch(
            "ckanext.datavic_harvester.harvesters.delwp.requests.get"
        ) as mock_get:
            h._send_deletion_safeguard_notify(anomaly=True)
        mock_get.assert_called_once_with(
            "https://notify.example/anomaly", timeout=10
        )

    def test_request_failure_does_not_raise(self):
        """Notify is fail-open: a network error should be logged but not
        propagate as an exception."""
        import requests as req_lib

        h = self._make_harvester(ok_url="https://notify.example/ok")
        with mock.patch(
            "ckanext.datavic_harvester.harvesters.delwp.requests.get",
            side_effect=req_lib.RequestException("timeout"),
        ):
            h._send_deletion_safeguard_notify(anomaly=False)  # must not raise
