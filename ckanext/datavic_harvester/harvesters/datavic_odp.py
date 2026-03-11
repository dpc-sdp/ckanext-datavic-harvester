"""CKAN harvester that sets default schema fields for DD -> ODP harvesting.

Use when the target instance (e.g. ODP) has required schema fields that
may be missing or in extras on the source (e.g. Data Directory).

The base CKANHarvester calls modify_package_dict() from import_stage (after
applying default_tags, default_groups, default_extras, etc.); override that
to set defaults package metadata.
"""
from __future__ import annotations

from typing import Any

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester


class DataVicODPHarvester(CKANHarvester):
    """Sets default package metadata fields."""

    def info(self):
        return {
            "name": "datavic_odp",
            "title": "DataVic ODP",
            "description": "Harvests from a DataVic instance.",
        }

    def modify_package_dict(self, package_dict: dict[str, Any], harvest_object: Any) -> dict[str, Any]:
        package_dict.setdefault("protective_marking", "official")
        package_dict.setdefault("access", "yes")
        package_dict.setdefault("organization_visibility", "all")
        package_dict.setdefault("private", False)

        return package_dict
