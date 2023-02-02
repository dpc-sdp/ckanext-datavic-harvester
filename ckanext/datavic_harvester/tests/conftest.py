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


register(DatasetFactory, "dataset")


@register
class HarvestSourceFactory(HarvestSource):
    owner_org = factory.LazyFunction(lambda: OrganizationFactory()["id"])
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
def clean_db(reset_db):
    reset_db()
    harvest_model.setup()


@pytest.fixture
def delwp_config(group, organization_factory):
    org1 = organization_factory()
    org2 = organization_factory()
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
                "org-name": org2["name"],
            },
            {
                "resowner": org1["title"],
                "org-name": org2["name"],
            },
        ],
    }
