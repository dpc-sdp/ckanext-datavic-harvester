from __future__ import annotations

import json
import logging
from os import path
from typing import Optional, Any

from bs4 import BeautifulSoup

from ckan.plugins import toolkit as tk

from ckanext.dcat import converters
from ckanext.dcat.harvesters._json import DCATJSONHarvester
from ckanext.harvest.model import HarvestObject

from ckanext.datavic_harvester import helpers
from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester


log = logging.getLogger(__name__)


class DataVicDCATJSONHarvester(DCATJSONHarvester, DataVicBaseHarvester):
    def info(self):
        return {
            "name": "datavic_dcat_json",
            "title": "DataVic DCAT JSON Harvester",
            "description": "DataVic Harvester for DCAT dataset descriptions serialized as JSON",
        }

    def gather_stage(self, harvest_job):
        self._set_config(harvest_job.source.config)
        return super().gather_stage(harvest_job)

    def import_stage(self, harvest_object):
        self._set_config(harvest_object.source.config)
        return super().import_stage(harvest_object)

    def _get_package_dict(
        self, harvest_object: HarvestObject
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Converts a DCAT dataset into a CKAN dataset. Performs specific DataVic
        conversions of the data"""

        dcat_dict: dict[str, Any] = json.loads(harvest_object.content)
        pkg_dict = converters.dcat_to_ckan(dcat_dict)

        soup: BeautifulSoup = BeautifulSoup(pkg_dict["notes"], "html.parser")

        pkg_dict["name"] = self._get_package_name(harvest_object, pkg_dict["title"])

        self._set_description_and_extract(pkg_dict, soup)
        self._set_full_metadata_url_and_update_frequency(pkg_dict, soup)
        self._mutate_tags(pkg_dict)
        self._set_default_group(pkg_dict)
        self._set_required_fields_defaults(dcat_dict, pkg_dict)

        return pkg_dict, dcat_dict

    def _set_description_and_extract(self, pkg_dict: dict[str, Any], soup) -> None:
        if "default.description" in pkg_dict["notes"]:
            pkg_dict["notes"] = "No description has been entered for this dataset."
            pkg_dict["extract"] = "No abstract has been entered for this dataset."
        else:
            pkg_dict["notes"] = helpers.unwrap_all_except(
                helpers.remove_all_attrs_except_for(soup),
            )
            pkg_dict["extract"] = self._generate_extract(soup)

    def _generate_extract(self, soup: BeautifulSoup) -> str:
        """Extract is the first sentence of the description/notes"""

        try:
            notes = soup.get_text()
            index = notes.index(".")
            notes = notes[: index + 1]
        except Exception as ex:
            log.error(f"Generate extract error for: {soup}")
            log.error(str(ex))
            return ""
        return notes

    def _set_full_metadata_url_and_update_frequency(
        self, pkg_dict: dict[str, Any], soup: BeautifulSoup
    ) -> None:
        metadata_url: Optional[str] = self._get_extra(pkg_dict, "full_metadata_url")

        if not metadata_url and "default_full_metadata_url" in self.config:
            metadata_url = self.config["default_full_metadata_url"]

        if not metadata_url and "full_metadata_url_pattern" in self.config:
            desc_metadata_url: Optional[str] = helpers.extract_metadata_url(
                soup, self.config["full_metadata_url_pattern"]
            )

            if desc_metadata_url:
                metadata_url = desc_metadata_url

        if metadata_url:
            pkg_dict["update_frequency"] = self._fetch_update_frequency(metadata_url)
            pkg_dict["full_metadata_url"] = metadata_url

    def _fetch_update_frequency(self, full_metadata_url: str) -> str:
        """Fetch an update_frequency by full_metadata_url"""

        default_udp_frequency: str = "unknown"

        resp_text: Optional[str] = (
            self._get_mocked_full_metadata()
            if self.test
            else self._make_request(full_metadata_url)
        )

        if not resp_text:
            log.error(f"Request error occured during fetching update_frequency")
            return default_udp_frequency

        soup: BeautifulSoup = BeautifulSoup(resp_text, "html.parser")

        frequency_mapping: dict[str, str] = {
            "deemed": "asNeeded",
            "week": "weekly",
            "twice": "biannually",
            "year": "annually",
            "month": "monthly",
            "quarter": "quarterly",
        }

        for tag in soup(
            "script", attrs={"id": "tpx_ExternalView_Frequency_of_Updates"}
        ):
            for k, v in frequency_mapping.items():
                if k in tag.string:
                    return v

        return default_udp_frequency

    def _mutate_tags(self, pkg_dict: dict[str, Any]) -> None:
        """Replace ampersands with "and" in tags"""

        if not pkg_dict["tags"]:
            return

        for tag in pkg_dict["tags"]:
            if "name" in tag and "&" in tag["name"]:
                tag["name"] = tag["name"].replace("&", "and")

    def _set_default_group(self, pkg_dict: dict[str, Any]) -> None:
        default_groups: list[dict[str, Any]] = self.config.get(
            "default_group_dicts", []
        )

        if not "groups" in pkg_dict:
            pkg_dict["groups"] = []

        if default_groups and isinstance(default_groups, list):
            category = default_groups[0] if default_groups else None

            if category:
                pkg_dict["category"] = category.get("id")

            existing_group_ids: list[str] = [
                group["id"] for group in pkg_dict["groups"]
            ]

            pkg_dict["groups"].extend(
                [
                    {"id": group["id"], "name": group["name"]}
                    for group in default_groups
                    if group["id"] not in existing_group_ids
                ]
            )

    def _set_required_fields_defaults(
        self, dcat_dict: dict[str, Any], pkg_dict: dict[str, Any]
    ) -> None:
        """Set required fields"""
        if not self._get_extra(pkg_dict, "personal_information"):
            pkg_dict["personal_information"] = "no"

        if not self._get_extra(pkg_dict, "access"):
            pkg_dict["access"] = "yes"

        if not self._get_extra(pkg_dict, "protective_marking"):
            pkg_dict["protective_marking"] = "official"

        if not self._get_extra(pkg_dict, "organization_visibility") \
            and "default_visibility" in self.config:
            pkg_dict["organization_visibility"] = self.config["default_visibility"][
                "organization_visibility"
            ]
        else:
            pkg_dict["organization_visibility"] = "current"

        pkg_dict["workflow_status"] = "published"

        issued: Optional[str] = dcat_dict.get("issued")
        if issued and not self._get_extra(pkg_dict, "date_created_data_asset"):
            pkg_dict["date_created_data_asset"] = helpers.convert_date_to_isoformat(
                issued, "issued", pkg_dict["title"], strip_tz=False
            )

        modified: Optional[str] = dcat_dict.get("modified")
        if modified and not self._get_extra(pkg_dict, "date_modified_data_asset"):
            pkg_dict["date_modified_data_asset"] = helpers.convert_date_to_isoformat(
                modified, "modified", pkg_dict["title"], strip_tz=False
            )

        landing_page: Optional[str] = dcat_dict.get("landingPage")
        if landing_page and not self._get_extra(pkg_dict, "full_metadata_url"):
            pkg_dict["full_metadata_url"] = landing_page

        if not pkg_dict.get("license_id") and "default_license" in self.config:
            pkg_dict["license_id"] = self.config["default_license"]["id"]
            pkg_dict["custom_licence_text"] = self.config["default_license"]["title"]

        pkg_dict["tag_string"] = dcat_dict.get("keyword", [])

        pkg_dict.setdefault("update_frequency", "unknown")

    def _get_existing_dataset(self, guid: str) -> Optional[dict[str, Any]]:
        """Return a package with specific guid extra if exists"""

        datasets: list[tuple[str]] = self._read_datasets_from_db(guid)

        if not datasets:
            return

        if len(datasets) > 1:
            log.error(f"Found more than one dataset with the same guid: {guid}")

        return tk.get_action("package_show")(
            {"user": self._get_user_name(), "ignore_auth": True},
            {"id": datasets[0][0]},
        )

    def _get_content_and_type(self, url, harvest_job, page=1, content_type=None):
        """Mock data, use it instead of actual request for develop process"""

        if self.test:
            return self._get_mocked_content(), ""

        return super()._get_content_and_type(url, harvest_job, page, content_type)

    def _get_mocked_content(self) -> str:
        here: str = path.abspath(path.dirname(__file__))
        with open(path.join(here, "../data/dcat_json_datasets.txt")) as f:
            return f.read()

    def _get_mocked_full_metadata(self):
        here: str = path.abspath(path.dirname(__file__))
        with open(path.join(here, "../data/dcat_json_full_metadata.txt")) as f:
            return f.read()
