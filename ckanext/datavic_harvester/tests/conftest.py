from __future__ import annotations

from typing import Any

import pytest
import factory
from pytest_factoryboy import register
from ckantoolkit.tests.factories import _get_action_user_name
from faker import Faker

import ckan.plugins.toolkit as tk
from ckan.tests import factories

import ckanext.harvest.model as harvest_model
from ckanext.harvest.tests.factories import HarvestJob, HarvestSource

fake = Faker()


@register
class GroupFactory(factories.Group):
    pass


register(GroupFactory, "group")


class OrganizationFactory(factories.Organization):
    pass


register(OrganizationFactory, "organization")


@register
class DatasetFactory(factories.Dataset):
    access = "yes"
    category = factory.LazyFunction(lambda: GroupFactory()["id"])
    date_created_data_asset = factory.Faker("date")
    extract = factory.Faker("sentence")
    license_id = "notspecified"
    personal_information = "yes"
    organization_visibility = "all"
    update_frequency = "unknown"
    workflow_status = "test"
    protective_marking = "official"
    enable_dtv = False
    owner_org = factory.LazyFunction(lambda: OrganizationFactory()["id"])


register(DatasetFactory, "dataset")


@register
class HarvestSourceFactory(HarvestSource):
    owner_org = factory.LazyFunction(lambda: OrganizationFactory()["id"])
    source_type = "delwp"
    _return_type = "obj"


@register
class HarvestJobFactory(HarvestJob):
    source = factory.SubFactory(HarvestSourceFactory)

    _return_type = "obj"


class HarvestObject(factory.Factory):

    FACTORY_FOR = harvest_model.HarvestObject

    class Meta:
        model = harvest_model.HarvestObject

    _return_type = "dict"

    job = factory.SubFactory(HarvestJobFactory)
    extras = {"status": "new"}

    @classmethod
    def _create(cls, target_class, *args, **kwargs):
        if args:
            assert False, "Positional args aren't supported, use keyword args."

        if "job_id" not in kwargs:
            kwargs["job_id"] = kwargs["job"].id
            kwargs["source_id"] = kwargs["job"].source.id

        if "job" in kwargs:
            kwargs.pop("job")

        job_dict = tk.get_action("harvest_object_create")(
            {"user": _get_action_user_name(kwargs)}, kwargs
        )

        if cls._return_type == "dict":
            return job_dict
        else:
            return harvest_model.HarvestObject.get(job_dict["id"])


@register
class HarvestObjectFactory(HarvestObject):
    _return_type = "obj"


@pytest.fixture
def harvest_object():
    return HarvestObjectFactory()


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    reset_db()
    migrate_db_for("harvest")
    migrate_db_for("activity")



@pytest.fixture
def delwp_config(group, organization_factory):
    org1 = organization_factory(title="Department of Environment, Land, Water & Planning")
    org2 = organization_factory(title="Department of Health & Human Services")

    return {
        "default_groups": [group["id"]],
        "default_group_dicts": [group],
        "full_metadata_url_prefix": "https://metashare.maps.vic.gov.au/geonetwork/srv/api/records/{UUID}/formatters/sdm-html?root=html&output=html",
        "resource_url_prefix": "https://datashare.maps.vic.gov.au/search?md=",
        "resource_attribution": "Copyright (c) The State of Victoria, Department of Environment, Land, Water & Planning",
        "license_id": "cc-by",
        "dataset_type": "datashare-metadata",
        "api_auth": "Apikey XXX",
        "geoserver_dns": "https://opendata-uat.maps.vic.gov.au",
        "organisation_mapping": [
            {
                "resowner": org1["title"],
                "org-name": org1["name"],
            },
            {
                "resowner": org2["title"],
                "org-name": org2["name"],
            },
        ],
        "test": True,
    }


@pytest.fixture
def dcat_config(group):
    return {
        "default_groups": [group["id"]],
        "default_group_dicts": [group],
        "default_license": {"id": "notspecified", "title": "License not specified"},
        "default_full_metadata_url": "https://localhost/metadata/",
        "full_metadata_url_pattern": "localhost/metadata",
    }


@pytest.fixture
def dcat_dataset():
    return {
        "@type": "dcat:Dataset",
        "identifier": "https://www.arcgis.com/home/item.html?id=4a3f184ced3d48c883917804393ad420&sublayer=0",
        "landingPage": "https://localhost/maps/melbournewater::melbourne-water-land-availability-for-our-space-your-place",
        "title": "Melbourne Water Land Availability for Our Space Your Place",
        "description": "<span style='color:rgb(83, 86, 90); font-family:&quot;Avenir Next&quot;, &quot;Avenir Next&quot;; font-size:18px;'>Fatal and injury crashes on Victorian roads during the latest five year reporting period. This data allows users to analyse Victorian fatal and injury crash data based on time, location, conditions, crash type, road user type, object hit etc. Road Safety data is provided by VicRoads for educational and research purposes. This data is in Web Mercator (Auxiliary Sphere) projection.</span><div><span style='color:rgb(83, 86, 90); font-family:&quot;Avenir Next&quot;, &quot;Avenir Next&quot;; font-size:18px;'> </span><a href='https://localhost/metadata/Crashes_Last_Five_Years%20-%20Open%20Data.html' rel='nofollow ugc' target='_blank'>About this Data</a></div>",
        "issued": "2017-08-02T07:18:27.000Z",
        "modified": "2022-05-06T05:29:14.178Z",
        "publisher": {
            "source": "Melbourne Water Corporation",
            "name": "Melbourne Water Corporation",
        },
        "contactPoint": {
            "fn": "Melbourne Water Corporation",
            "hasEmail": "mailto:enquiry@melbournewater.com.au",
            "@type": "vcard:Contact",
        },
        "accessLevel": "public",
        "spatial": "144.3896,-38.5081,146.4106,-37.1463",
        "license": "Creative Commons Attribution Share-Alike 4.0 International",
        "distribution": [
            {
                "@type": "dcat:Distribution",
                "title": "ArcGIS Hub Dataset",
                "format": "Web Page",
                "mediaType": "text/html",
                "accessURL": "https://localhost/maps/melbournewater::melbourne-water-land-availability-for-our-space-your-place",
            },
            {
                "@type": "dcat:Distribution",
                "title": "ArcGIS GeoService",
                "format": "ArcGIS GeoServices REST API",
                "mediaType": "application/json",
                "accessURL": "https://services5.arcgis.com/ZSYwjtv8RKVhkXIL/arcgis/rest/services/Land_Availability_Conditions/FeatureServer/0",
            },
            {
                "@type": "dcat:Distribution",
                "title": "GeoJSON",
                "format": "GeoJSON",
                "mediaType": "application/vnd.geo+json",
                "accessURL": "https://localhost/datasets/melbournewater::melbourne-water-land-availability-for-our-space-your-place.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D",
            },
            {
                "@type": "dcat:Distribution",
                "title": "CSV",
                "format": "CSV",
                "mediaType": "text/csv",
                "accessURL": "https://localhost/datasets/melbournewater::melbourne-water-land-availability-for-our-space-your-place.csv?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D",
            },
            {
                "@type": "dcat:Distribution",
                "title": "KML",
                "format": "KML",
                "mediaType": "application/vnd.google-earth.kml+xml",
                "accessURL": "https://localhost/datasets/melbournewater::melbourne-water-land-availability-for-our-space-your-place.kml?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D",
            },
            {
                "@type": "dcat:Distribution",
                "title": "Shapefile",
                "format": "ZIP",
                "mediaType": "application/zip",
                "accessURL": "https://localhost/datasets/melbournewater::melbourne-water-land-availability-for-our-space-your-place.zip?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D",
            },
        ],
        "theme": ["geospatial"],
    }


@pytest.fixture
def dcat_description(dcat_dataset: dict[str, Any]):
    return dcat_dataset["description"]
