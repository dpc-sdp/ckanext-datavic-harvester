"""OpenDataSoft harvester adapted for DataVic needs.

Extends ODSHavester from ckanext-harvest-basket by:

* converting description into markdown using markdownify library

"""
from __future__ import annotations

from urllib.parse import urljoin

from typing import Optional

from markdownify import markdownify

import ckan.plugins.toolkit as tk
from ckanext.harvest_basket.harvesters import ODSHarvester

class DataVicODSHarvester(ODSHarvester):

    def _description_refine(self, string: Optional[str]) -> str:
        """Prepare raw description for CKAN.

        @override
        Use markdownify instead of html2markdown
        """
        if not string:
            return ""

        return markdownify(string)

    def _fetch_resources(self, source_url, resource_urls, pkg_data):
        """ To resources with CSV forrmat add the delimiter parameter into URL to
        fix generatint Table Preview

        """
        resources = super()._fetch_resources(source_url, resource_urls, pkg_data)
        for res in resources:
            if res["format"] == "CSV":
                res["url"] = f'{res["url"]}?delimiter=%2C'
        return resources
