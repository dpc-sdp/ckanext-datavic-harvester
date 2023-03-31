from __future__ import annotations

import logging
from typing import Optional, Any

import requests

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters import HarvesterBase


log = logging.getLogger(__name__)


class DataVicBaseHarvester(HarvesterBase):
    def __init__(self, **kwargs):
        self.test = kwargs.get("test", False)
        super().__init__(**kwargs)

    def _set_config(self, config_str: str) -> None:
        if config_str:
            self.config = json.loads(config_str)

            if "api_version" in self.config:
                self.api_version = int(self.config["api_version"])

            log.debug("Using config: %r", self.config)
        else:
            self.config = {}

    def validate_config(self, config: Optional[str]) -> str:
        """Validates source config"""
        if not config:
            raise ValueError("No config options set")

        config_obj = json.loads(config)

        self._validate_default_groups(config_obj)
        self._set_default_groups_data(config_obj)
        self._validate_default_license(config_obj)

        return json.dumps(config_obj, indent=4)

    def _validate_default_groups(self, config: dict[str, Any]) -> None:
        if "default_groups" not in config:
            raise ValueError("default_groups must be set")

        default_groups: list[str] = config["default_groups"]

        if not isinstance(default_groups, list):
            raise ValueError("default_groups must be a *list* of group names/ids")

        if default_groups and not isinstance(default_groups[0], str):
            raise ValueError(
                "default_groups must be a list of group " "names/ids (i.e. strings)"
            )

    def _validate_default_license(self, config: dict[str, Any]) -> None:
        default_license: dict[str, Any] = config.get("default_license", {})

        if not default_license:
            return

        if not isinstance(default_license, dict):
            raise ValueError("default_license field must be a dictionary")

        if "id" not in default_license or "title" not in default_license:
            raise ValueError("default_license must contain `id` and `title` fields")

    def _set_default_groups_data(self, config: dict[str, Any]) -> None:
        default_groups: list[str] = config["default_groups"]

        config["default_group_dicts"] = self._get_default_groups_data(default_groups)

    def _get_default_groups_data(self, group_ids: list[str]) -> list[dict[str, Any]]:
        group_dicts: list[dict[str, Any]] = []

        for group_name_or_id in group_ids:
            try:
                group_data = tk.get_action("group_show")(
                    self._make_context(), {"id": group_name_or_id}
                )

                group_dicts.append(group_data)
            except tk.ObjectNotFound:
                raise ValueError(f"Default group {group_name_or_id} not found")

        return group_dicts

    def _get_object_extra(
        self, harvest_object: HarvestObject, key: str
    ) -> Optional[Any]:
        """Retrieving the value from a harvest object extra by a given key"""
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_extra(self, data_dict: dict[str, Any], key: str) -> Optional[Any]:
        """Retrieving the value from a data_dict extra by a given key"""
        for extra in data_dict.get("extras", []):
            if extra.get("key") == key:
                return extra.get("value")

        return None

    def _make_request(
        self, url: str, headers: Optional[dict[str, Any]] = None
    ) -> Optional[str]:
        """Make a GET request to a URL"""

        try:
            resp: requests.Response = requests.get(url, headers=headers)
        except requests.HTTPError as e:
            log.error("HTTP error: %s %s", e.response.status_code, e.request.url)
        except requests.RequestException as e:
            log.error("Request error: %s", str(e))
        except Exception as e:
            log.error("HTTP general exception: %s", str(e))
        else:
            return resp.text

    def fetch_stage(self, harvest_object: HarvestObject) -> bool:
        return True

    def _delete_package(self, package_id: str, guid: str):
        tk.get_action("package_delete")(self._make_context(), {"id": package_id})
        log.info(f"Deleted package {package_id} with guid {guid}")

    def _make_context(self) -> dict[str, Any]:
        return {
            "user": self._get_user_name(),
            "return_id_only": True,
            "ignore_auth": True,
            "model": model,
            "session": model.Session,
        }
