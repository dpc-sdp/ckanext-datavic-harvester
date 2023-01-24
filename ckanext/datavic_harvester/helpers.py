from __future__ import annotations

import re
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

import ckan.plugins.toolkit as tk

from ckanext.datavicmain.helpers import field_choices


log = logging.getLogger(__name__)


def remove_all_attrs_except_for(
    soup: BeautifulSoup, allowed_tags: list[str] = ["a", "br"]
) -> BeautifulSoup:
    """Remove all attributes from tags inside soup except for the listed ones
    Leave only "target" and "href" attributes for allowed ones.

    Args:
        soup (BeautifulSoup): Instance of BeautifulSoup
        allowed_tags (list[str], optional): list of allowed tags. Defaults to ["a", "br"].

    Returns:
        BeautifulSoup: Instance of BeautifulSoup
    """

    for tag in soup.find_all(True):
        if tag.name not in allowed_tags:
            tag.attrs = {}
        else:
            attrs = dict(tag.attrs)
            for attr in attrs:
                if attr not in ["target", "href"]:
                    del tag.attrs[attr]
    return soup


def unwrap_all_except(
    soup: BeautifulSoup, allowed_tags: list[str] = ["a", "br"]
) -> str:
    """Removes all tags from soup obj except for allowed ones

    Args:
        soup (BeautifulSoup): Instance of BeautifulSoup
        allowed_tags (list[str], optional): list of allowed tags. Defaults to ["a", "br"].

    Returns:
        str: stringified version of BeautifulSoup instance
    """
    for tag in soup.find_all(True):
        if tag.name not in allowed_tags:
            tag.unwrap()

    return str(soup)


def extract_metadata_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Extract a metadata URL from a soup obj

    Args:
        soup (BeautifulSoup): Instance of BeautifulSoup
        base_url (str): full metadata url pattern

    Returns:
        str: metadata URL
    """

    for tag in soup.find_all("a"):
        if "href" in tag.attrs and base_url in tag["href"]:
            return tag["href"]


def fetch_update_frequency(full_metadata_url: str) -> str:
    """Fetch an update_frequency from full_metadata_url

    Args:
        full_metadata_url (str): full metadata URL

    Returns:
        str: update_frequency value
    """

    update_frequency: str = "unknown"

    try:
        response: requests.Response = requests.get(full_metadata_url)
    except requests.RequestException as e:
        log.error(f"Request error occured during fetching update_frequency: {e}")
        return update_frequency

    soup: BeautifulSoup = BeautifulSoup(response.content, "html.parser")

    frequency_mapping: dict[str, str] = {
        "deemed": "asNeeded",
        "week": "weekly",
        "twice": "biannually",
        "year": "annually",
        "month": "monthly",
        "quarter": "quarterly",
    }

    for tag in soup("script", attrs={"id": "tpx_ExternalView_Frequency_of_Updates"}):
        tag_text: str = tag.get_text()

        for k, v in frequency_mapping.items():
            if k in tag_text:
                return v

    return update_frequency


def convert_date_to_isoformat(
    value: Optional[str], key: str, dataset_name: Optional[str], strip_tz=True
) -> Optional[str]:
    """Convert a date string to isoformat

    Args:
        value (str): date
        key (str): metadata field key
        dataset_name (str): dataset name

    Returns:
        Optional[str]: isoformat date
    """
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


def get_from_to(page: int, datasets_per_page: int) -> tuple[int, int]:
    """Calculate offset to make request with pagination by N records

    Args:
        page (int): page number
        datasets_per_page (int): limit of datasets per pae

    Returns:
        tuple[int, int]: start and end
    """

    if page == 1:
        _from: int = 1
        _to: int = page * datasets_per_page
    else:
        _from: int = ((page - 1) * datasets_per_page) + 1
        _to: int = page * datasets_per_page

    return _from, _to


def munge_title_to_name(value: str) -> str:
    """Munge a title into a name

    Args:
        value (str): title

    Returns:
        str: name
    """
    name = re.sub("[ .:/,]", "-", value)
    name = re.sub("[^a-zA-Z0-9-_]", "", name).lower()
    name = re.sub("[-]+", "-", name)
    return name.strip("-")[:99]


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
