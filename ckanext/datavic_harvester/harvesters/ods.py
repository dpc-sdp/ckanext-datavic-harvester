"""OpenDataSoft harvester adapted for DataVic needs.

Extends ODSHavester from ckanext-harvest-basket by:

* converting description into markdown using markdownify library

"""
from __future__ import annotations
from typing import Optional

from markdownify import markdownify

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
