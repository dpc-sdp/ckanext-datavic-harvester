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
        dcat_config: DcatConfig,
    ):
        source = harvest_source_factory(
            config=json.dumps(dcat_config), source_type=harvester.info()["name"]
        )
        harvest_job = harvest_job_factory(source=source)
        obj_ids = harvester.gather_stage(harvest_job)

        assert harvest_job.gather_errors == []
        assert type(obj_ids) == list

        datasets = json.loads(harvester._get_mocked_content())["dataset"]
        assert len(set(obj_ids)) == len(datasets)

        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert harvest_object.guid == datasets[0]["identifier"]
        assert json.loads(harvest_object.content) == datasets[0]

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
        assert pkg_dict["workflow_status"] == "draft"

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
