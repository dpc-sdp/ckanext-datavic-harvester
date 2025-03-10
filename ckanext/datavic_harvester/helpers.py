from __future__ import annotations

import re
import logging
from typing import Optional, Any

from bs4 import BeautifulSoup

import ckan.plugins.toolkit as tk
from ckan.lib.munge import munge_title_to_name as munge_title

from ckanext.datavicmain.helpers import field_choices


log = logging.getLogger(__name__)


def remove_all_attrs_except_for(soup: BeautifulSoup) -> BeautifulSoup:
    """Remove all attributes from tags inside soup except for the listed ones
    Leave only "target" and "href" attributes for allowed ones."""

    for tag in soup.find_all(True):
        if tag.name not in ["a", "br"]:
            tag.attrs = {}
        else:
            attrs = dict(tag.attrs)
            for attr in attrs:
                if attr not in ["target", "href"]:
                    del tag.attrs[attr]
    return soup


def unwrap_all_except(soup: BeautifulSoup) -> str:
    """Removes all tags from soup obj except for allowed ones"""
    for tag in soup.find_all(True):
        if tag.name not in ["a", "br"]:
            tag.unwrap()

    return str(soup)


def extract_metadata_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Extract a metadata URL from a soup obj"""

    for tag in soup.find_all("a"):
        if "href" in tag.attrs and base_url in tag["href"]:
            return tag["href"]


def convert_date_to_isoformat(
    value: Optional[str], key: str, dataset_name: Optional[str], strip_tz=True
) -> Optional[str]:
    """Convert a date string to isoformat"""
    date = None

    if not value:
        return log.debug(f"{dataset_name}: Missing date for key {key}")

    value = value.split(".")[0]  # strip out microseconds
    value = value.lower().split("t")[0] if strip_tz else value

    try:
        date = tk.get_converter("isodate")(value, {})
    except tk.Invalid:
        return log.debug(f"{dataset_name}: date format incorrect {value} for key {key}")

    return date.isoformat() if date else None


def munge_title_to_name(value: str) -> str:
    return munge_title(value)


def get_tags(tags: str) -> list[dict[str, str]]:
    """Fetch tags from a delwp tags string, e.g `society;environment`"""
    tag_list: list[str] = re.split(";|,", tags)

    return [{"name": tag} for tag in tag_list]


def map_update_frequency(value: str):
    """Map local update_frequency to remote portal ones"""

    frequency_mapping: list[dict[str, Any]] = get_datavic_update_frequencies()

    for frequency in frequency_mapping:
        if frequency["label"].lower() == value.lower():
            return frequency["value"]

    return "unknown"


def get_datavic_update_frequencies():
    return field_choices("update_frequency")
