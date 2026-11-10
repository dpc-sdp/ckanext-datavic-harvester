from types import SimpleNamespace

import pytest

from ckan.model import State
from ckan.tests.helpers import call_action

from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester as Base
from ckanext.datavic_harvester.harvesters.base import add_harvest_source_extras


@pytest.fixture
def harvester():
    return Base()


class TestBaseHarvester:
    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_delete_package(self, dataset, harvester: Base):
        dataset_id: str = dataset["id"]
        assert dataset_id
        harvester._delete_package(dataset_id, "guid")

        package_dict = call_action("package_show", id=dataset_id)
        assert package_dict["state"] == State.DELETED

    def test_make_context(self, harvester: Base):
        context = harvester._make_context()

        assert context["user"] == harvester._get_user_name()
        assert context["return_id_only"]
        assert context["ignore_auth"]
        assert context["model"]
        assert context["session"]

    def test_get_extra(self, harvester: Base):
        assert harvester._get_extra(
            {"extras": [{"key": "test", "value": True}]}, "test"
        )
        assert not harvester._get_extra({}, "test")
        assert not harvester._get_extra(
            {"extras": [{"key": "test2", "value": True}]}, "test"
        )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_get_extra_object(self, harvester: Base, harvest_object_factory):
        harvest_object = harvest_object_factory(extras={"test": "1"})
        assert harvester._get_object_extra(harvest_object, "test")

        harvest_object = harvest_object_factory()
        assert not harvester._get_object_extra(harvest_object, "test")


class TestAddHarvestSourceExtras:
    """Shared by DELWP and ODS (base.py), so the harvest source listing page
    (/harvest/<source>) does not depend on Solr reindex timing to show a
    dataset - see base.add_harvest_source_extras docstring."""

    def _source(self):
        return SimpleNamespace(id="source-1", title="Source One", type="ods")

    def test_adds_all_three_keys(self):
        pkg_dict = {}

        add_harvest_source_extras(pkg_dict, self._source())

        extras = {e["key"]: e["value"] for e in pkg_dict["extras"]}
        assert extras["harvest_source_id"] == "source-1"
        assert extras["harvest_source_title"] == "Source One"
        assert extras["harvest_source_type"] == "ods"

    def test_creates_extras_list_when_absent(self):
        pkg_dict = {}

        add_harvest_source_extras(pkg_dict, self._source())

        assert "extras" in pkg_dict

    def test_appends_to_existing_extras_without_dropping_them(self):
        pkg_dict = {"extras": [{"key": "custom_free_form", "value": "keep-me"}]}

        add_harvest_source_extras(pkg_dict, self._source())

        extras = {e["key"]: e["value"] for e in pkg_dict["extras"]}
        assert extras["custom_free_form"] == "keep-me"
        assert extras["harvest_source_id"] == "source-1"

    def test_does_not_duplicate_existing_harvest_source_keys(self):
        pkg_dict = {
            "extras": [{"key": "harvest_source_id", "value": "stale-id"}]
        }

        add_harvest_source_extras(pkg_dict, self._source())

        matching = [
            e for e in pkg_dict["extras"] if e["key"] == "harvest_source_id"
        ]
        assert len(matching) == 1
        assert matching[0]["value"] == "stale-id"
