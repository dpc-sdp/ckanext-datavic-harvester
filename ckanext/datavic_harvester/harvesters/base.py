from __future__ import annotations

import logging
from typing import Optional, Any
from urllib.parse import urlparse

import requests
import time

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters import HarvesterBase


log = logging.getLogger(__name__)

MAX_CONTENT_LENGTH = int(
    tk.config.get("ckanext.datavic_harvester.max_content_length") or 104857600
)
CHUNK_SIZE = 16 * 1024
DOWNLOAD_TIMEOUT = 30
CONFIG_FSC_EXCLUDED_DOMAINS = tk.aslist(
    tk.config.get("ckanext.datavic_harvester.filesize_excluded_domains", "")
)


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
        try:
            tk.get_action("package_delete")(self._make_context(), {"id": package_id})
            log.info(f"Deleted package {package_id} with guid {guid}")
        except tk.ObjectNotFound:
            log.error(f"Package {package_id} not found")

    def _make_context(self) -> dict[str, Any]:
        return {
            "user": self._get_user_name(),
            "return_id_only": True,
            "ignore_auth": True,
            "model": model,
            "session": model.Session,
        }


class DataTooBigWarning(Exception):
    pass


def get_resource_size(resource_url: str) -> int:
    """Return external resource size in bytes

    Args:
        resource_url (str): a URL for the resource’s source

    Returns:
        int: resource size in bytes
    """

    length = 0
    cl = None

    if not resource_url or MAX_CONTENT_LENGTH < 0:
        return length

    hostname = urlparse(resource_url).hostname
    if hostname in CONFIG_FSC_EXCLUDED_DOMAINS:
        return length

    try:
        headers = {}

        response = _get_response(resource_url, headers)
        ct = response.headers.get("content-type")
        cl = response.headers.get("content-length")
        cl_enabled = tk.asbool(tk.config.get(
            "ckanext.datavic_harvester.content_length_enabled", False)
        )

        if ct and "text/html" in ct:
            message = (
                f"Resource from url <{resource_url}> is of HTML type. "
                "Skip its size calculation."
            )
            log.warning(message)
            return length

        if cl:
            if int(cl) > MAX_CONTENT_LENGTH and MAX_CONTENT_LENGTH > 0:
                response.close()
                raise DataTooBigWarning()

            if cl_enabled:
                response.close()
                log.info(
                    f"Resource from url <{resource_url}> content-length is {int(cl)} bytes."
                )
                return int(cl)

        for chunk in response.iter_content(CHUNK_SIZE):
            length += len(chunk)
            if length > MAX_CONTENT_LENGTH:
                response.close()
                raise DataTooBigWarning()

        response.close()

    except DataTooBigWarning:
        message = (
            f"Resource from url <{resource_url}> is more than the set limit "
            f"{MAX_CONTENT_LENGTH} bytes. Skip its size calculation."
        )
        log.warning(message)
        length = -1  # for the purpose of search possibility in the db
        return length

    except requests.exceptions.HTTPError as error:
        log.debug(f"HTTP error: {error}")

    except requests.exceptions.Timeout:
        log.warning(f"URL time out after {DOWNLOAD_TIMEOUT}s")

    except requests.exceptions.RequestException as error:
        log.warning(f"URL error: {error}")

    log.info(f"Resource from url <{resource_url}> length is {length} bytes.")

    return length


def _get_response(url, headers):
    def get_url():
        kwargs = {"headers": headers, "timeout": 30, "stream": True}

        if "ckan.download_proxy" in tk.config:
            proxy = tk.config.get("ckan.download_proxy")
            kwargs["proxies"] = {"http": proxy, "https": proxy}

        return requests.get(url, **kwargs)

    response = get_url()
    if response.status_code == 202:
        wait = 1
        while wait < 120 and response.status_code == 202:
            time.sleep(wait)
            response = get_url()
            wait *= 3
    response.raise_for_status()

    return response
