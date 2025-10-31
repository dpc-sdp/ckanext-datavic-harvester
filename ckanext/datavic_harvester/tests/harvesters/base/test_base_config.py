from __future__ import annotations

from typing import Any

import json
import pytest

from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester as Base


@pytest.fixture
def harvester():
    return Base()


@pytest.fixture
def valid_config(group):
    return {
        "default_groups": [group["id"]],
        "default_license": {"id": "test", "title": "test"},
    }


class TestBaseConfig:
    def test_set_config(self, harvester: Base):
        assert harvester.config is None
        harvester._set_config("")
        assert harvester.config == {}

    def test_validate_empty(self, harvester: Base):
        with pytest.raises(ValueError, match="No config options set"):  # type: ignore
            harvester.validate_config(None)

    def test_validate_only_json_string(self, harvester: Base):
        with pytest.raises(ValueError, match="Expecting value:"):  # type: ignore
            harvester.validate_config("hello")

    def test_validate_no_default_group(self, harvester: Base):
        with pytest.raises(ValueError, match="default_groups must be set"):  # type: ignore
            harvester.validate_config("{}")

    def test_validate_default_group_not_list(self, harvester: Base):
        with pytest.raises(
            ValueError, match="default_groups must be a \*list\* of group names\/ids"  # type: ignore
        ):
            harvester.validate_config(json.dumps({"default_groups": {}}))

    def test_validate_default_group_value_not_string(self, harvester: Base):
        with pytest.raises(
            ValueError,
            match="default_groups must be a list of group names/ids",  # type: ignore
        ):
            harvester.validate_config(json.dumps({"default_groups": [1]}))

    def test_validate_default_group_not_exist(self, harvester: Base):
        group_name: str = "test"
        with pytest.raises(ValueError, match=f"Default group {group_name} not found"):  # type: ignore
            harvester.validate_config(json.dumps({"default_groups": [group_name]}))

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_validate_default_group_exist(self, harvester: Base, group):
        """If group exists, the `default_group_dicts` will be populated with a list
        of group data"""
        config_str: str = harvester.validate_config(
            json.dumps({"default_groups": [group["id"]]})
        )
        config: dict[str, Any] = json.loads(config_str)

        # default_license is not mandatory
        assert "default_license" not in config
        assert isinstance(config["default_group_dicts"], list)
        assert config["default_group_dicts"][0]["id"] == group["id"]

    def test_validate_default_license_not_dict(self, harvester: Base, group):

        with pytest.raises(
            ValueError, match="default_license field must be a dictionary"  # type: ignore
        ):
            harvester._validate_default_license(
                {"default_groups": [group["id"]], "default_license": "test"}
            )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_validate_default_license_missing_id_or_title(self, harvester: Base, group):

        with pytest.raises(
            ValueError, match="default_license must contain `id` and `title` fields"  # type: ignore
        ):
            harvester._validate_default_license(
                {"default_groups": [group["id"]], "default_license": {"id": "test"}}
            )

    @pytest.mark.usefixtures("with_plugins", "clean_db")
    def test_validate_return_dumped_json(self, harvester: Base, valid_config):
        config: str = harvester.validate_config(json.dumps(valid_config))

        assert config
        assert isinstance(config, str)
