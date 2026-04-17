from __future__ import annotations

import json
import os
import tempfile
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
    mock_item.source.config = json.dumps(
        {"dataset_type": "uat-datashare-metadata", "test": True}
    )
    with mock.patch.object(harvester, "_get_source_owner_org_id", return_value=None):
        harvester._set_config(mock_item)

    return harvester


@pytest.fixture
def delwp_dataset(harvester: DelwpHarvester):
    records = harvester._fetch_records("test_url", 0, 0)
    datasets = harvester._get_record_metadata(records)
    return next(datasets)


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
        assert isinstance(obj_ids, list)

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
        # datashare records may omit tempextentend; period_end is then None (not a harvester bug).
        assert pkg_dict["resources"][0]["period_start"] == h.convert_date_to_isoformat(
            delwp_dataset.get("tempextentbegin"),
            "tempextentbegin",
            delwp_dataset.get("title"),
        )
        assert pkg_dict["resources"][0]["period_end"] == h.convert_date_to_isoformat(
            delwp_dataset.get("tempextentend"),
            "tempextentend",
            delwp_dataset.get("title"),
        )
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
                harvester,
                "_get_current_harvest_guids",
                return_value=set(existing.keys()),
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
                harvester,
                "_get_current_harvest_guids",
                return_value=set(existing.keys()),
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
                harvester,
                "_get_current_harvest_guids",
                return_value=set(existing.keys()),
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
                harvester,
                "_get_current_harvest_guids",
                return_value=set(existing.keys()),
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
                harvester,
                "_get_current_harvest_guids",
                return_value=set(existing.keys()),
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

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_does_not_redelete_soft_deleted_guids(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dataset_factory,
        delwp_config,
    ):
        """Regression: a guid that was already soft-deleted in a prior run
        (current=False, report_status='deleted') must not be queued for deletion
        again when it remains absent from the source. Running the same harvest
        twice with identical source responses should produce zero delete actions
        on the second run."""
        source = harvest_source_factory(
            config=json.dumps(delwp_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)

        pkg_a = dataset_factory()["id"]
        pkg_b = dataset_factory()["id"]
        pkg_deleted = dataset_factory()["id"]

        # Simulate post-first-run state:
        #   guid-a, guid-b -> still active (current=True)
        #   guid-deleted   -> soft-deleted row from a previous delete run
        model.Session.add_all(
            [
                harvest_model.HarvestObject(
                    guid="guid-a",
                    job=harvest_job,
                    content="{}",
                    current=True,
                    package_id=pkg_a,
                ),
                harvest_model.HarvestObject(
                    guid="guid-b",
                    job=harvest_job,
                    content="{}",
                    current=True,
                    package_id=pkg_b,
                ),
                harvest_model.HarvestObject(
                    guid="guid-deleted",
                    job=harvest_job,
                    content="{}",
                    current=False,
                    package_id=pkg_deleted,
                    report_status="deleted",
                ),
            ]
        )
        model.Session.commit()

        # Second run: source still returns only the two active guids.
        records = [
            {"fields": {"uuid": "guid-a", "title": "a"}},
            {"fields": {"uuid": "guid-b", "title": "b"}},
        ]

        with (
            mock.patch.object(
                harvester, "_fetch_records_from_remote_portal", return_value=records
            ),
            mock.patch.object(harvester, "_save_harvest_json_to_filestore"),
        ):
            obj_ids = harvester.gather_stage(harvest_job)

        objects = [harvest_model.HarvestObject.get(obj_id) for obj_id in obj_ids]
        statuses = [harvester._get_object_extra(obj, "status") for obj in objects]

        assert statuses.count("change") == 2
        assert statuses.count("delete") == 0
        assert harvest_job.gather_errors == []


class TestGetCurrentHarvestGuids:
    """Regression: current-guid set excludes soft-deleted rows so deletion
    detection in gather_stage does not re-delete already-deleted guids."""

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_returns_distinct_current_guids_only(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dataset_factory,
    ):
        source = harvest_source_factory(source_type=harvester.info()["name"])
        job = harvest_job_factory(source=source)
        pkg_id = dataset_factory()["id"]

        current_a = harvest_model.HarvestObject(
            guid="current-a",
            job=job,
            content="{}",
            current=True,
            package_id=pkg_id,
        )
        current_b = harvest_model.HarvestObject(
            guid="current-b",
            job=job,
            content="{}",
            current=True,
            package_id=pkg_id,
        )
        soft_deleted = harvest_model.HarvestObject(
            guid="soft-deleted-guid",
            job=job,
            content="{}",
            current=False,
            package_id=pkg_id,
            report_status="deleted",
        )
        model.Session.add_all([current_a, current_b, soft_deleted])
        model.Session.commit()

        assert harvester._get_current_harvest_guids(source.id) == {
            "current-a",
            "current-b",
        }

        guid_map = harvester._get_guids_to_package_ids(source.id)
        assert len(guid_map) == 3
        assert set(guid_map.keys()) == {
            "current-a",
            "current-b",
            "soft-deleted-guid",
        }

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_empty_source_returns_empty_set(
        self, harvester: DelwpHarvester, harvest_source_factory
    ):
        source = harvest_source_factory(source_type=harvester.info()["name"])
        assert harvester._get_current_harvest_guids(source.id) == set()


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


class TestIsPkgPrivate:
    """Unit tests for _is_pkg_private (DATAVIC-812).

    Public ONLY when accesscontrol_restricted=False AND orderableondatashare=True.
    All other combinations — including missing fields, blank strings, and string
    booleans — are tested to ensure the correct behaviour.
    """

    def _h(self) -> DelwpHarvester:
        return DelwpHarvester()

    # --- happy path ---

    def test_public_when_not_restricted_and_orderable(self):
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": False, "orderableondatashare": True}
        ) is False

    def test_public_with_string_booleans(self):
        """String 'false'/'true' are normalised via asbool — same as CKAN config."""
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": "false", "orderableondatashare": "true"}
        ) is False

    def test_public_with_string_no_and_yes(self):
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": "no", "orderableondatashare": "yes"}
        ) is False

    # --- access restricted ---

    def test_private_when_restricted_and_orderable(self):
        """Restricted overrides orderable — still private."""
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": True, "orderableondatashare": True}
        ) is True

    def test_private_when_restricted_and_not_orderable(self):
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": True, "orderableondatashare": False}
        ) is True

    # --- not orderable ---

    def test_private_when_not_restricted_but_not_orderable(self):
        """Not restricted but also not orderable → private."""
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": False, "orderableondatashare": False}
        ) is True

    # --- missing fields ---

    def test_private_when_both_fields_missing(self):
        """Both missing: defaults are restricted=True, orderable=False → private."""
        assert self._h()._is_pkg_private({}) is True

    def test_private_when_only_orderable_present_and_true(self):
        """accesscontrol_restricted missing → defaults True → private."""
        assert self._h()._is_pkg_private({"orderableondatashare": True}) is True

    def test_private_when_only_restricted_present_and_false(self):
        """orderableondatashare missing → defaults False → private."""
        assert self._h()._is_pkg_private({"accesscontrol_restricted": False}) is True

    # --- blank strings treated as private (DATAVIC-812 key fix) ---

    def test_private_when_accesscontrol_blank(self):
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": "", "orderableondatashare": True}
        ) is True

    def test_private_when_orderable_blank(self):
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": False, "orderableondatashare": "   "}
        ) is True

    def test_private_when_both_blank(self):
        assert self._h()._is_pkg_private(
            {"accesscontrol_restricted": "", "orderableondatashare": ""}
        ) is True



class TestHarvestFilestore:
    """Basic smoke tests for harvest JSON filestore (DATAVIC-822).

    This feature is for debugging/replay purposes only, so we test the main
    happy path and the two skip conditions.
    """

    def _h(self) -> DelwpHarvester:
        return DelwpHarvester()

    def test_save_writes_json_file_with_records_key(self):
        h = self._h()
        records = [{"fields": {"uuid": "abc", "title": "Test"}}]
        with tempfile.TemporaryDirectory() as tmpdir:
            with (
                mock.patch.dict(os.environ, {"DELWP_HARVEST_JSON_RETENTION_DAYS": "7"}),
                mock.patch.object(h, "_get_harvest_filestore_dir", return_value=tmpdir),
            ):
                h._save_harvest_json_to_filestore(records, "job-123")
            files = [f for f in os.listdir(tmpdir) if f.endswith(".json")]
            assert len(files) == 1
            with open(os.path.join(tmpdir, files[0])) as f:
                assert json.load(f)["records"] == records

    def test_save_skipped_when_retention_zero(self):
        h = self._h()
        with (
            mock.patch.dict(os.environ, {"DELWP_HARVEST_JSON_RETENTION_DAYS": "0"}),
            mock.patch.object(h, "_get_harvest_filestore_dir") as mock_dir,
        ):
            h._save_harvest_json_to_filestore([{"fields": {"uuid": "x"}}], "job-1")
        mock_dir.assert_not_called()

    def test_save_skipped_when_no_storage_path(self):
        h = self._h()
        with (
            mock.patch.dict(os.environ, {"DELWP_HARVEST_JSON_RETENTION_DAYS": "7"}),
            mock.patch.object(h, "_get_harvest_filestore_dir", return_value=None),
        ):
            h._save_harvest_json_to_filestore([{"fields": {"uuid": "x"}}], "job-1")


class TestPurgedDatasetRecreation:
    """Tests for the purged-package re-creation path (DATAVIC-822).

    If a harvest object has status='change' but its package has been hard-purged
    from the DB, import_stage must not crash or create a duplicate with a suffixed
    name. Instead it re-creates the dataset as a new package.
    """

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_recreates_purged_dataset(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        delwp_dataset: dict,
        delwp_config,
    ):
        """When a harvest object references a package_id that no longer exists in
        the DB (purged), import_stage should create a new package successfully
        rather than failing or producing a duplicate."""
        source = harvest_source_factory(
            config=json.dumps(delwp_config),
            source_type=harvester.info()["name"],
        )
        job = harvest_job_factory(source=source)

        # First import: create the dataset normally
        obj1 = harvest_object_factory(
            guid=delwp_dataset["uuid"],
            content=json.dumps(delwp_dataset),
            job=job,
        )
        harvester.import_stage(obj1)
        original_package_id = obj1.package_id
        assert original_package_id
        assert obj1.errors == []

        # Hard-purge the package so model.Package.get() returns None.
        # Nullify harvest object FK first so the purge cascade isn't blocked.
        sysadmin = call_action("get_site_user", ignore_auth=True)
        call_action("package_delete", {"user": sysadmin["name"]}, id=original_package_id)
        model.Session.query(harvest_model.HarvestObject).filter(
            harvest_model.HarvestObject.package_id == original_package_id
        ).update({"package_id": None})
        model.Session.commit()
        call_action("dataset_purge", {"user": sysadmin["name"]}, id=original_package_id)

        # Simulate a harvest object that still points to the purged package_id
        # with status="change" — this is the duplicate-triggering scenario.
        # We can't pass package_id to the factory (harvest_object_create validates
        # it exists), so we set it on the in-memory object without committing.
        # import_stage reads harvest_object.package_id and internally defers the
        # FK constraint (SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED),
        # so there is no FK violation during the test.
        obj2 = harvest_object_factory(
            guid=delwp_dataset["uuid"],
            content=json.dumps(delwp_dataset),
            job=job,
            extras={"status": "change"},
        )
        obj2.package_id = original_package_id
        # No commit here — import_stage handles FK deferral internally.

        result = harvester.import_stage(obj2)

        assert result is True
        assert obj2.errors == []

        # A new package should exist — different ID from the purged one
        new_package_id = obj2.package_id
        assert new_package_id
        assert new_package_id != original_package_id

        new_pkg = model.Package.get(new_package_id)
        assert new_pkg is not None
        assert new_pkg.state == "active"


class TestRestoreFlow:
    """Tests for the soft-delete restore path (DATAVIC-906).

    When a GUID that was previously soft-deleted reappears in the source, the
    import stage should restore the existing package to active state rather than
    creating a new one with a suffixed name.
    """

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_restores_deleted_dataset(
        self,
        harvester: DelwpHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        delwp_dataset: dict,
        delwp_config,
    ):
        """A dataset that was soft-deleted and reappears in the source is set
        back to active state by import_stage."""
        source = harvest_source_factory(
            config=json.dumps(delwp_config),
            source_type=harvester.info()["name"],
        )
        job = harvest_job_factory(source=source)

        # First import: create the dataset
        obj1 = harvest_object_factory(
            guid=delwp_dataset["uuid"],
            content=json.dumps(delwp_dataset),
            job=job,
        )
        harvester.import_stage(obj1)

        package_id = obj1.package_id
        assert package_id, "import_stage should have created a package"
        assert obj1.errors == []

        pkg = call_action("package_show", id=package_id)
        assert pkg["state"] == "active"

        # Soft-delete the package (simulating a previous harvest deletion run)
        sysadmin = call_action("get_site_user", ignore_auth=True)
        call_action("package_delete", {"user": sysadmin["name"]}, id=package_id)
        assert call_action("package_show", id=package_id)["state"] == "deleted"

        # Second import: dataset reappears in source.
        # status="change" + package_id set triggers the restore branch.
        obj2 = harvest_object_factory(
            guid=delwp_dataset["uuid"],
            content=json.dumps(delwp_dataset),
            job=job,
            package_id=package_id,
            extras={"status": "change"},
        )
        result = harvester.import_stage(obj2)

        assert result is True
        assert obj2.errors == []
        pkg = call_action("package_show", id=package_id)
        assert pkg["state"] == "active"
