from __future__ import annotations

import logging
from typing import Optional, Any

import requests

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.lib.helpers import json
from ckan.model import Package

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError


log = logging.getLogger(__name__)


class DataVicBaseHarvester(HarvesterBase):
    def _set_config(self, config_str: str) -> None:
        if config_str:
            self.config = json.loads(config_str)

            if "api_version" in self.config:
                self.api_version = int(self.config["api_version"])

            log.debug("Using config: %r", self.config)
        else:
            self.config = {}

    def validate_config(self, config: Optional[str]) -> dict[str, Any]:
        """
        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        Validates the default_group entered exists and creates default_group_dicts

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        """
        if not config:
            raise ValueError("No config options set")

        config_obj = json.loads(config)

        if "default_groups" not in config_obj:
            raise ValueError("default_groups must be set")

        default_groups = config_obj["default_groups"]

        if not isinstance(default_groups, list):
            raise ValueError("default_groups must be a *list* of group names/ids")

        if default_groups and not isinstance(default_groups[0], str):
            raise ValueError(
                "default_groups must be a list of group " "names/ids (i.e. strings)"
            )

        config_obj["default_group_dicts"] = self._get_default_groups_data(
            default_groups
        )

        if "full_metadata_url_prefix" not in config_obj:
            raise ValueError("full_metadata_url_prefix must be set")

        if "{UUID}" not in config_obj.get("full_metadata_url_prefix", ""):
            raise ValueError(
                "full_metadata_url_prefix must have the {UUID} identifier in the URL"
            )

        if "resource_url_prefix" not in config_obj:
            raise ValueError("resource_url_prefix must be set")

        if "license_id" not in config_obj:
            raise ValueError("license_id must be set")

        if "resource_attribution" not in config_obj:
            raise ValueError("resource_attribution must be set")

        return config_obj

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

    def _get_package_name(self, harvest_object: HarvestObject, title: str) -> str:
        """Generate package name from title"""
        package: Package = harvest_object.package

        if package is None or package.title != title:
            name = self._gen_new_name(title)

            if not name:
                raise Exception(
                    "Could not generate a unique name from the title or the "
                    "GUID. Please choose a more unique title."
                )
        else:
            name = package.name

        return name

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

    def _get_guids_to_package_ids(self, source_id: str) -> dict[str, str]:
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(HarvestObject.current == True)
            .filter(HarvestObject.harvest_source_id == source_id)
        )

        return {
            harvest_object.guid: harvest_object.package_id for harvest_object in query
        }
