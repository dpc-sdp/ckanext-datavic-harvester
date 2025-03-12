import pytest

from ckan.model import State
from ckan.tests.helpers import call_action

from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester as Base


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
