"""OpenDataSoft harvester adapted for DataVic needs.

Extends ODSHavester from ckanext-harvest-basket by:

* converting description into markdown using markdownify library
* an offline "test" mode that replays a bundled JSON fixture instead of
  calling the remote portal (see MOCK_FILE)

"""
from __future__ import annotations

import json
import logging
from os import path

from typing import Any, Optional

from markdownify import markdownify

import ckan.plugins.toolkit as tk
from ckanext.harvest_basket.harvesters import ODSHarvester
from .base import get_resource_size

log = logging.getLogger(__name__)

# When test is true, the harvester replays this fixture instead of calling
# the remote portal (see _get_mocked_fixture).
# To regenerate, use /app/ckan/generate_ods_fixture.py script.
MOCK_FILE = "ods_records.json"


class DataVicODSHarvester(ODSHarvester):

    def __init__(self, **kwargs):
        self.test = kwargs.get("test", False)
        super().__init__(**kwargs)

    def _set_config(self, config_str: Optional[str]) -> dict[str, Any]:
        # Set self.config and self.test from the source config string.
        config = super()._set_config(config_str)

        _test = config.get("test", False)
        self.test = tk.asbool(False if _test is None else _test)

        if self.test:
            log.warning(
                "%s: TEST MODE ACTIVE - replaying bundled fixture %s, "
                "no remote requests will be made",
                self.SRC_ID,
                MOCK_FILE,
            )

        return config

    def _description_refine(self, string: Optional[str]) -> str:
        """Prepare raw description for CKAN.

        @override
        Use markdownify instead of html2markdown
        """
        if not string:
            return ""

        return markdownify(string)

    def _search_datasets(self, source_url: str) -> list[dict[str, Any]]:
        # Override ODSHarvester._search_datasets to replay fixture data in test mode.
        if not self.test:
            return super()._search_datasets(source_url)

        pkg_dicts = self._get_mocked_fixture().get("datasets", [])
        if not pkg_dicts:
            log.warning(
                "%s: fixture %s has no 'datasets' entries", self.SRC_ID, MOCK_FILE
            )

        log.info(
            "%s: test mode, replaying %d dataset(s) from %s",
            self.SRC_ID,
            len(pkg_dicts),
            MOCK_FILE,
        )

        return pkg_dicts

    def _get_all_resource_urls(self, res_link: str) -> list[dict[str, Any]]:
        #  In test mode, get the resource URLs from the bundled fixture
        #  instead of calling the remote portal.
        if not self.test:
            return super()._get_all_resource_urls(res_link)

        if not res_link:
            return []

        dataset_id = res_link.rstrip("/").split("/")[-2]
        exports = self._get_mocked_fixture().get("exports", {})

        if dataset_id not in exports:
            log.warning(
                "%s: no 'exports' entry for dataset %s in fixture %s, "
                "dataset will have no resources",
                self.SRC_ID,
                dataset_id,
                MOCK_FILE,
            )
            return []

        return exports[dataset_id]

    def _get_mocked_fixture(self) -> dict[str, Any]:
        here: str = path.abspath(path.dirname(__file__))

        with open(path.join(here, f"../data/{MOCK_FILE}")) as f:
            return json.load(f)

    def _fetch_resources(self, source_url, resource_urls, pkg_data):
        """ To resources with CSV forrmat add the delimiter parameter into URL to
        fix generatint Table Preview

        """
        resources = super()._fetch_resources(source_url, resource_urls, pkg_data)
        for res in resources:
            if res["format"] == "CSV":
                res["url"] = f'{res["url"]}?delimiter=%2C'

            # All resources are set to 1KB in test mode to avoid network calls.
            res["size"] = 1024 if self.test else get_resource_size(res["url"])
            res["filesize"] = res["size"]

        return resources
