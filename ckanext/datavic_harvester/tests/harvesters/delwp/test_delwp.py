from __future__ import annotations

import json
from typing import Any
from typing_extensions import TypedDict
from types import GeneratorType
from datetime import datetime as dt

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


@pytest.fixture
def harvester():
    harvester = DelwpHarvester(test=True)

    harvester._set_config(json.dumps({"dataset_type": "delwp"}))

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
        assert len(records) == 100

        harvester._get_record_metadata(records)

    def test_get_record_metadata(self, harvester: DelwpHarvester):
        records = harvester._fetch_records("test_url", 0, 0)
        datasets = harvester._get_record_metadata(records)

        assert isinstance(datasets, GeneratorType)

        dataset: dict[str, Any] = next(datasets)

        assert dataset["uuid"]
        assert dataset["title"]
        assert len([i for i in datasets]) == 99

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
        org_id: str = harvester._create_organization(resowner, harvest_object)
        source = call_action("package_show", id=harvest_object.harvest_source_id)

        assert source["owner_org"] == org_id
