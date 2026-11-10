from __future__ import annotations

import json
from typing import Any
from typing_extensions import TypedDict
from types import GeneratorType
from datetime import datetime as dt
from datetime import timedelta

import pytest

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.tests.helpers import call_action

import ckanext.harvest.model as harvest_model

import ckanext.datavic_harvester.helpers as h
from ckanext.datavic_harvester.harvesters import (
    DataVicDCATJSONHarvester as DcatHarvester,
)


class DcatConfig(TypedDict):
    default_groups: list[str]
    default_group_dicts: dict[str, Any]
    default_license: dict[str, str]
    default_full_metadata_url: str
    full_metadata_url_pattern: str


@pytest.fixture
def harvester(dcat_config: DcatConfig):
    harvester = DcatHarvester(test=True)
    harvester.config = dcat_config

    return harvester


class TestDcatHarvester:
    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage(
        self,
        harvester: DcatHarvester,
        harvest_job_factory,
        harvest_source_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        datasets = json.loads(harvester._get_mocked_content())["dataset"]
        stale_harvest_object = harvest_object_factory(
            guid=datasets[0]["identifier"],
            content=json.dumps(datasets[0]),
            job=harvest_job,
            extras={"status": "change"},
        )
        stale_harvest_object.current = True
        stale_harvest_object.package_id = None
        model.Session.add(stale_harvest_object)
        model.Session.commit()

        obj_ids = harvester.gather_stage(harvest_job)

        assert harvest_job.gather_errors == []
        assert type(obj_ids) == list

        assert len(set(obj_ids)) == len(datasets)

        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert harvest_object.guid == datasets[0]["identifier"]
        assert json.loads(harvest_object.content) == datasets[0]
        assert harvester._get_object_extra(harvest_object, "status") == "new"

        model.Session.refresh(stale_harvest_object)
        assert stale_harvest_object.current is False

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_leaves_healthy_current_object_alone(
        self,
        harvester: DcatHarvester,
        harvest_job_factory,
        harvest_source_factory,
        harvest_object_factory,
        dataset_factory,
        dcat_config: DcatConfig,
    ):
        """A current object whose package still exists must not be flipped."""
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        datasets = json.loads(harvester._get_mocked_content())["dataset"]

        package = dataset_factory()
        healthy_harvest_object = harvest_object_factory(
            guid=datasets[0]["identifier"],
            content=json.dumps(datasets[0]),
            job=harvest_job,
            package_id=package["id"],
            extras={"status": "change"},
        )
        healthy_harvest_object.current = True
        model.Session.add(healthy_harvest_object)
        model.Session.commit()

        harvester.gather_stage(harvest_job)

        model.Session.refresh(healthy_harvest_object)
        assert healthy_harvest_object.current is True

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_marks_reappeared_deleted_dataset_as_change(
        self,
        harvester: DcatHarvester,
        harvest_job_factory,
        harvest_source_factory,
        harvest_object_factory,
        dataset_factory,
        dcat_config: DcatConfig,
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        dataset = dataset_factory()
        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": dataset["id"]},
        )
        dcat_dataset = json.loads(harvester._get_mocked_content())["dataset"][0]
        deleted_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=None,
            job=harvest_job,
            package_id=dataset["id"],
            extras={"status": "delete"},
        )
        deleted_harvest_object.current = False
        deleted_harvest_object.report_status = "deleted"
        model.Session.add(deleted_harvest_object)
        model.Session.commit()

        obj_ids = harvester.gather_stage(harvest_job)

        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert harvest_object.guid == dcat_dataset["identifier"]
        assert harvest_object.package_id == dataset["id"]
        assert harvester._get_object_extra(harvest_object, "status") == "change"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_stage_returns_saved_delete_object_id(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_job_factory,
        harvest_source_factory,
        harvest_object_factory,
        dataset_factory,
        dcat_config: DcatConfig,
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        dataset = dataset_factory()
        previous_harvest_object = harvest_object_factory(
            guid="deleted-dcat-dataset",
            content=json.dumps({"identifier": "deleted-dcat-dataset"}),
            job=harvest_job,
            package_id=dataset["id"],
            extras={"status": "new"},
        )
        previous_harvest_object.current = True
        model.Session.add(previous_harvest_object)
        model.Session.commit()

        monkeypatch.setattr(harvester, "_get_mocked_content", lambda: '{"dataset":[]}')

        obj_ids = harvester.gather_stage(harvest_job)

        assert obj_ids
        assert all(obj_ids)
        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert harvest_object.package_id == dataset["id"]
        assert harvester._get_object_extra(harvest_object, "status") == "delete"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_and_import_recreates_purged_datasets(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_job_factory,
        harvest_source_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )

        # First harvest: gather the dataset from the DCAT JSON source.
        first_job = harvest_job_factory(source=source)
        first_obj_ids = harvester.gather_stage(first_job)
        first_objects = [harvest_model.HarvestObject.get(id_) for id_ in first_obj_ids]

        assert first_job.gather_errors == []
        assert len(first_objects) == 1
        assert harvester._get_object_extra(first_objects[0], "status") == "new"

        # First import: create a package from the gathered harvest object.
        first_object = first_objects[0]
        assert harvester.import_stage(first_object) is True
        assert first_object.errors == []
        self._finish_harvest_job(first_job)

        original_package_id = first_object.package_id
        assert model.Package.get(original_package_id)

        # Delete and hard-purge the harvested package from CKAN.
        self._hard_purge_harvested_packages([original_package_id])
        assert model.Package.get(original_package_id) is None
        first_object = harvest_model.HarvestObject.get(first_obj_ids[0])
        assert first_object.current is True

        # Second harvest: gather the same DCAT source after package purge.
        second_job = harvest_job_factory(source=source)
        second_obj_ids = harvester.gather_stage(second_job)
        second_objects = [
            harvest_model.HarvestObject.get(id_) for id_ in second_obj_ids
        ]
        first_objects_after_second_gather = [
            harvest_model.HarvestObject.get(id_) for id_ in first_obj_ids
        ]

        assert second_job.gather_errors == []
        assert len(second_objects) == 1
        assert first_objects_after_second_gather[0].current is False
        assert harvester._get_object_extra(second_objects[0], "status") == "new"
        assert second_objects[0].package_id is None

        # Second import: recreate the purged dataset as a new CKAN package.
        second_object = second_objects[0]
        assert harvester.import_stage(second_object) is True

        assert second_object.errors == []
        assert second_object.package_id != original_package_id
        assert model.Package.get(second_object.package_id).state == "active"
        assert model.Package.get(original_package_id) is None
        error_text = "\n".join(str(error) for error in second_object.errors)
        assert "Package was not found" not in error_text

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )

        result = harvester.import_stage(harvest_object)

        assert harvest_object.errors == []
        assert result is True
        assert harvest_object.package_id

        package = model.Package.get(harvest_object.package_id)

        assert package
        assert package.name == h.munge_title_to_name(dcat_dataset["title"])
        assert package.extras["guid"] == dcat_dataset["identifier"]

        source = call_action("package_show", id=source.id)
        assert source["owner_org"] == package.owner_org

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_returns_unchanged_when_modified_date_matches(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        first_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )

        assert harvester.import_stage(first_harvest_object) is True

        second_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
            extras={"status": "change"},
        )

        assert harvester.import_stage(second_harvest_object) == "unchanged"
        assert second_harvest_object.errors == []

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_recreates_purged_dataset_from_change_object(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )

        first_job = harvest_job_factory(source=source)
        first_obj_ids = harvester.gather_stage(first_job)
        first_harvest_object = harvest_model.HarvestObject.get(first_obj_ids[0])

        assert harvester._get_object_extra(first_harvest_object, "status") == "new"
        assert harvester.import_stage(first_harvest_object) is True
        self._finish_harvest_job(first_job)
        original_package_id = first_harvest_object.package_id
        assert original_package_id

        second_job = harvest_job_factory(source=source)
        second_obj_ids = harvester.gather_stage(second_job)
        second_harvest_object = harvest_model.HarvestObject.get(second_obj_ids[0])

        assert harvester._get_object_extra(second_harvest_object, "status") == "change"
        assert second_harvest_object.package_id == original_package_id

        self._hard_purge_harvested_packages([original_package_id])
        assert model.Package.get(original_package_id) is None
        second_harvest_object = harvest_model.HarvestObject.get(
            second_harvest_object.id
        )

        assert harvester.import_stage(second_harvest_object) is True
        assert second_harvest_object.errors == []
        assert second_harvest_object.package_id
        assert second_harvest_object.package_id != original_package_id
        assert model.Package.get(second_harvest_object.package_id).state == "active"
        assert harvester._get_object_extra(second_harvest_object, "status") == "new"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_deletes_object_without_content(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dataset_factory,
        dcat_config: DcatConfig,
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        dataset = dataset_factory()
        harvest_object = harvest_object_factory(
            guid="deleted-dcat-dataset",
            content=None,
            job=harvest_job,
            package_id=dataset["id"],
            extras={"status": "delete"},
        )

        assert harvester.import_stage(harvest_object) is True
        assert model.Package.get(dataset["id"]).state == "deleted"
        assert harvest_object.errors == []

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_restores_deleted_dataset(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        first_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )

        assert harvester.import_stage(first_harvest_object) is True
        package_id = first_harvest_object.package_id
        package = call_action("package_show", id=package_id)
        package["syndicated_id"] = "existing-odp-package-id"
        package["skip_syndication"] = "false"
        call_action(
            "package_update",
            context={"user": harvester._get_user_name(), "ignore_auth": True},
            **package,
        )
        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": package_id},
        )
        assert model.Package.get(package_id).state == "deleted"

        second_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
            package_id=package_id,
            extras={"status": "change"},
        )

        assert harvester.import_stage(second_harvest_object) is True
        assert second_harvest_object.package_id == package_id
        assert model.Package.get(package_id).state == "active"
        package_dict, dcat_dict = harvester._get_package_dict(second_harvest_object)
        harvester.modify_package_dict(package_dict, dcat_dict, second_harvest_object)
        assert package_dict["syndicated_id"] == "existing-odp-package-id"
        assert package_dict["skip_syndication"] == "false"
        extra_keys = {extra["key"] for extra in package_dict.get("extras", [])}
        assert "syndicated_id" not in extra_keys
        assert "skip_syndication" not in extra_keys
        restored_package = call_action(
            "package_show",
            context={"user": harvester._get_user_name(), "ignore_auth": True},
            id=package_id,
        )
        assert restored_package["syndicated_id"] == "existing-odp-package-id"
        assert restored_package["skip_syndication"] == "false"
        assert second_harvest_object.errors == []

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_preserve_pkg_fields_widened_beyond_syndication(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """_preserve_existing_fields now uses the full PRESERVE_PKG_FIELDS
        shared with DELWP/ODS (20 fields), not the old hardcoded
        (skip_syndication, syndicated_id) tuple. dtv_preview is the field
        with genuinely at-risk data on DCAT-harvested packages in
        production - confirmed present on all such packages, unprotected
        before this change."""
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        first_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )

        assert harvester.import_stage(first_harvest_object) is True
        package_id = first_harvest_object.package_id

        sysadmin = call_action("get_site_user", ignore_auth=True)
        call_action(
            "package_patch",
            {"user": sysadmin["name"]},
            id=package_id,
            dtv_preview=True,
            bil_confidentiality="major",
            role="Data Custodian",
        )
        assert call_action("package_show", id=package_id)["dtv_preview"] is True

        # DCAT's import_stage skips the update as "unchanged" when
        # dcat_modified matches the existing dataset, unless the package is
        # trashed (restoring_deleted_package). Delete it first, same as
        # test_import_stage_restores_deleted_dataset, to force the update
        # path that exercises _preserve_existing_fields.
        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": package_id},
        )

        second_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
            package_id=package_id,
            extras={"status": "change"},
        )

        assert harvester.import_stage(second_harvest_object) is True
        assert model.Package.get(package_id).state == "active"

        updated_package = call_action("package_show", id=package_id)
        assert updated_package["dtv_preview"] is True
        assert updated_package["bil_confidentiality"] == "major"
        assert updated_package["role"] == "Data Custodian"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_preserve_pkg_fields_maintainer_email_core_column_fallback(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """maintainer_email is a core package column, not a package_extra
        row, so existing_package.extras.get() never sees it. Regression
        guard for the fallback in _preserve_existing_fields."""
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )

        assert harvester.import_stage(harvest_object) is True
        package_id = harvest_object.package_id

        sysadmin = call_action("get_site_user", ignore_auth=True)
        call_action(
            "package_patch",
            {"user": sysadmin["name"]},
            id=package_id,
            maintainer_email="custodian@example.com",
        )
        assert (
            call_action("package_show", id=package_id)["maintainer_email"]
            == "custodian@example.com"
        )

        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": package_id},
        )

        second_harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
            package_id=package_id,
            extras={"status": "change"},
        )

        assert harvester.import_stage(second_harvest_object) is True
        assert model.Package.get(package_id).state == "active"

        updated_package = call_action("package_show", id=package_id)
        assert updated_package["maintainer_email"] == "custodian@example.com"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_get_pkg_dict(
        self,
        harvester: DcatHarvester,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        harvest_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
        )

        pkg_dict, dcat_dict = harvester._get_package_dict(harvest_object)

        assert pkg_dict["notes"]
        assert pkg_dict["extract"] in pkg_dict["notes"]
        assert pkg_dict["update_frequency"] == "asNeeded"
        assert pkg_dict["full_metadata_url"]

        assert pkg_dict["title"] == dcat_dict["title"]
        assert pkg_dict["name"] == h.munge_title_to_name(dcat_dataset["title"])

        assert pkg_dict["license_id"] == dcat_config["default_license"]["id"]
        assert (
            pkg_dict["custom_licence_text"] == dcat_config["default_license"]["title"]
        )

        assert pkg_dict["category"] in dcat_config["default_groups"]

        assert dt.fromisoformat(pkg_dict["date_created_data_asset"])
        assert dt.fromisoformat(pkg_dict["date_modified_data_asset"])

        assert pkg_dict["personal_information"] == "no"
        assert pkg_dict["protective_marking"] == "official"
        assert pkg_dict["access"] == "yes"
        assert pkg_dict["organization_visibility"] == "current"
        assert pkg_dict["workflow_status"] == "published"

        assert pkg_dict["resources"]
        assert pkg_dict["resources"][0]["format"]
        assert pkg_dict["resources"][0]["name"]
        assert pkg_dict["resources"][0]["url"]

        for tag in pkg_dict["tags"]:
            assert tag["name"] in dcat_dataset["keyword"]

    def test_get_existing_dataset_by_guid(
        self, dataset_factory, harvester: DcatHarvester
    ):
        dataset = dataset_factory(extras=[{"key": "guid", "value": "test"}])
        assert dataset == harvester._get_existing_dataset("test")

        assert not harvester._get_existing_dataset("test2")

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_import_stage_reactivates_locally_trashed_package_still_in_source(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """An admin-trashed package still in the source must be restored."""
        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )

        first_job = harvest_job_factory(source=source)
        first_ids = harvester.gather_stage(first_job)
        first_object = harvest_model.HarvestObject.get(first_ids[0])
        assert harvester.import_stage(first_object) is True
        self._finish_harvest_job(first_job)
        package_id = first_object.package_id

        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": package_id},
        )
        assert model.Package.get(package_id).state == "deleted"

        second_job = harvest_job_factory(source=source)
        second_ids = harvester.gather_stage(second_job)
        second_object = harvest_model.HarvestObject.get(second_ids[0])
        assert harvester._get_object_extra(second_object, "status") == "change"
        assert second_object.package_id == package_id

        assert harvester.import_stage(second_object) is True

        assert model.Package.get(package_id).state == "active"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_source_removal_then_reappearance_restores_same_package(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """A guid removed then re-added to the source restores the same package."""
        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )

        first_job = harvest_job_factory(source=source)
        first_ids = harvester.gather_stage(first_job)
        first_object = harvest_model.HarvestObject.get(first_ids[0])
        assert harvester.import_stage(first_object) is True
        self._finish_harvest_job(first_job)
        package_id = first_object.package_id

        monkeypatch.setattr(harvester, "_get_mocked_content", lambda: '{"dataset":[]}')
        delete_job = harvest_job_factory(source=source)
        delete_ids = harvester.gather_stage(delete_job)
        delete_object = harvest_model.HarvestObject.get(delete_ids[0])
        assert harvester._get_object_extra(delete_object, "status") == "delete"
        assert harvester.import_stage(delete_object) is True
        self._finish_harvest_job(delete_job)
        assert model.Package.get(package_id).state == "deleted"

        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        restore_job = harvest_job_factory(source=source)
        restore_ids = harvester.gather_stage(restore_job)
        restore_object = harvest_model.HarvestObject.get(restore_ids[0])

        assert harvester._get_object_extra(restore_object, "status") == "change"
        assert restore_object.package_id == package_id

        assert harvester.import_stage(restore_object) is True
        assert model.Package.get(package_id).state == "active"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_modify_package_dict_leaves_state_alone_for_plain_change(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """state must not be sent when the package is already active."""
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        first_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )
        assert harvester.import_stage(first_object) is True
        package_id = first_object.package_id

        change_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
            package_id=package_id,
            extras={"status": "change"},
        )
        package_dict, dcat_dict = harvester._get_package_dict(change_object)
        harvester.modify_package_dict(package_dict, dcat_dict, change_object)

        assert "state" not in package_dict

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_modify_package_dict_sets_state_active_for_trashed_package(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """state must be forced back to active when the package is trashed."""
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        first_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
        )
        assert harvester.import_stage(first_object) is True
        package_id = first_object.package_id

        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": package_id},
        )
        assert model.Package.get(package_id).state == "deleted"

        change_object = harvest_object_factory(
            guid=dcat_dataset["identifier"],
            content=json.dumps(dcat_dataset),
            job=harvest_job,
            package_id=package_id,
            extras={"status": "change"},
        )
        package_dict, dcat_dict = harvester._get_package_dict(change_object)
        harvester.modify_package_dict(package_dict, dcat_dict, change_object)

        assert package_dict["state"] == "active"

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_gather_reuses_trashed_package_without_report_status(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """The restore lookup must not depend on report_status being set."""
        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )

        first_job = harvest_job_factory(source=source)
        first_ids = harvester.gather_stage(first_job)
        first_object = harvest_model.HarvestObject.get(first_ids[0])
        assert harvester.import_stage(first_object) is True
        self._finish_harvest_job(first_job)
        original_package_id = first_object.package_id

        tk.get_action("package_delete")(
            {"user": harvester._get_user_name(), "ignore_auth": True},
            {"id": original_package_id},
        )
        model.Session.query(harvest_model.HarvestObject).filter_by(
            guid=dcat_dataset["identifier"]
        ).update({"current": False, "report_status": None}, False)
        model.Session.commit()

        second_job = harvest_job_factory(source=source)
        second_ids = harvester.gather_stage(second_job)
        second_object = harvest_model.HarvestObject.get(second_ids[0])

        assert second_object.package_id == original_package_id
        assert harvester._get_object_extra(second_object, "status") == "change"

        assert harvester.import_stage(second_object) is True
        assert model.Package.get(original_package_id).state == "active"
        assert (
            model.Session.query(model.Package)
            .filter(model.Package.type == "dataset")
            .filter(model.Package.state == "active")
            .count()
            == 1
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_get_latest_deleted_package_ids_picks_latest_per_guid(
        self,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        harvest_object_factory,
        dataset_factory,
        dcat_config: DcatConfig,
    ):
        """The batched lookup keeps the "latest import wins" rule per guid."""
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        context = {"user": harvester._get_user_name(), "ignore_auth": True}

        older = dataset_factory()
        newer = dataset_factory()
        other = dataset_factory()
        for package in (older, newer, other):
            tk.get_action("package_delete")(context, {"id": package["id"]})

        now = dt.now()
        for package, guid, import_finished in (
            (older, "guid-a", now - timedelta(days=2)),
            (newer, "guid-a", now),
            (other, "guid-b", now - timedelta(days=1)),
        ):
            harvest_object = harvest_object_factory(
                guid=guid,
                content=None,
                job=harvest_job,
                package_id=package["id"],
                extras={"status": "delete"},
            )
            harvest_object.current = False
            harvest_object.import_finished = import_finished
            model.Session.add(harvest_object)
        model.Session.commit()

        found = harvester._get_latest_deleted_package_ids(
            source.id, ["guid-a", "guid-b"]
        )

        assert found["guid-a"] == newer["id"]
        assert found["guid-b"] == other["id"]
        assert harvester._get_latest_deleted_package_ids(source.id, []) == {}

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_force_import_recreates_purged_package(
        self,
        monkeypatch,
        harvester: DcatHarvester,
        harvest_source_factory,
        harvest_job_factory,
        dcat_config: DcatConfig,
        dcat_dataset: dict[str, Any],
    ):
        """force_import against a purged package must not send id=None."""
        monkeypatch.setattr(
            harvester,
            "_get_mocked_content",
            lambda: json.dumps({"dataset": [dcat_dataset]}),
        )
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )

        first_job = harvest_job_factory(source=source)
        first_ids = harvester.gather_stage(first_job)
        first_object = harvest_model.HarvestObject.get(first_ids[0])
        assert harvester.import_stage(first_object) is True
        self._finish_harvest_job(first_job)
        original_package_id = first_object.package_id

        second_job = harvest_job_factory(source=source)
        second_ids = harvester.gather_stage(second_job)
        second_object = harvest_model.HarvestObject.get(second_ids[0])
        assert harvester._get_object_extra(second_object, "status") == "change"

        self._hard_purge_harvested_packages([original_package_id])
        second_object = harvest_model.HarvestObject.get(second_object.id)
        harvester.force_import = True

        assert harvester.import_stage(second_object) is True
        assert second_object.errors == []
        assert second_object.package_id != original_package_id
        assert model.Package.get(second_object.package_id).state == "active"

    def _hard_purge_harvested_packages(self, package_ids: list[str]) -> None:
        sysadmin = call_action("get_site_user", ignore_auth=True)
        context = {"user": sysadmin["name"], "ignore_auth": True}

        for package_id in package_ids:
            call_action("package_delete", context, id=package_id)

        for package_id in package_ids:
            call_action("dataset_purge", context, id=package_id)

    def _finish_harvest_job(self, job) -> None:
        job.status = "Finished"
        job.gather_finished = dt.now()
        job.finished = dt.now()
        job.save()
