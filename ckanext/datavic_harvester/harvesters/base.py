from __future__ import annotations

import logging
from typing import Optional, Any
from urllib.parse import urlparse

import requests
import time

from sqlalchemy import and_, or_

from ckan import model, types
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

# Fields preserved on the update path (package_update) so harvesting does not
# overwrite user-entered data. Shared by all DataVic harvesters; keep in sync
# with iar_ckan_dataset.yml.
#
# Applied with setdefault semantics, so a harvester's own value always wins
# where it set one. That is what makes a single shared superset safe across
# harvesters with different field coverage - do NOT trim this per-harvester.
PRESERVE_PKG_FIELDS = frozenset(
    {
        "alias",
        "agency_program_domain",
        "custom_licence_text",
        "custom_licence_link",
        "dtv_preview",
        "bil_confidentiality",
        "bil_confidentiality_description",
        "bil_availability",
        "bil_availability_description",
        "bil_integrity",
        "bil_integrity_description",
        "source_ict_system",
        "record_disposal_category",
        "disposal_category",
        "disposal_class",
        "workflow_status_notes",
        "role",
        "maintainer_email",
        "skip_syndication",
        "syndicated_id",
    }
)


def add_harvest_source_extras(pkg_dict: dict[str, Any], source: Any) -> None:
    """Persist harvest source identifiers as package extras.

    The harvest source page (/harvest/<source>) lists datasets via
    package_list_for_source, which filters Solr on
    `+harvest_source_id:"<id>"` (ckanext-harvest/helpers.py:40).
    ckanext-harvest does inject that field at index time via
    before_dataset_index, but nothing persists it on the package, so its
    presence in the index depends on when the last successful reindex ran.
    That makes the source listing flap: a dataset appears or disappears
    depending on whether the most recent harvest happened to write to Solr,
    not on its actual state.

    Persisting these as package extras makes them survive every reindex.
    Currently used by DELWP and ODS; not yet wired into DCAT JSON or ODP
    (they have partial coverage from the Solr-only hook - see the extraction
    discussion, DATAVIC-972).

    Args:
        pkg_dict: the package dict being built, mutated in place. Must
            already have (or will get) an "extras" list.
        source: the harvest source (e.g. harvest_object.source) - anything
            exposing .id, .title and .type.
    """
    pkg_dict.setdefault("extras", [])
    existing_keys = {e.get("key") for e in pkg_dict["extras"]}

    for key, value in (
        ("harvest_source_id", source.id),
        ("harvest_source_title", source.title),
        ("harvest_source_type", source.type),
    ):
        if key not in existing_keys:
            pkg_dict["extras"].append({"key": key, "value": value})


def get_object_extra(harvest_object: HarvestObject, key: str) -> Optional[Any]:
    """Retrieve the value of a harvest object extra by key.

    Module-level rather than a method because ODS extends ODSHarvester (from
    ckanext-harvest-basket) and so does not inherit DataVicBaseHarvester,
    but still needs to read the "status" extra that flags a delete.

    Args:
        harvest_object: anything exposing .extras, each with .key/.value.
        key: the extra to look up.

    Returns:
        The extra's value, or None when the key is absent.
    """
    for extra in harvest_object.extras:
        if extra.key == key:
            return extra.value
    return None


def get_existing_guids_to_package_ids(
    source_id: str, active_only: bool
) -> dict[str, str]:
    """Map every guid harvested from a source to its package_id.

    Shared by DELWP and ODS's purge_missing / new-vs-change detection.
    Both need "what package does this guid belong to", found the same way:
    current=True's package_id is always correct when that row exists - but
    each harvester earns that guarantee through its own import_stage, not a
    shared one. ODS calls super().gather_stage()/fetch_stage()/import_stage()
    into ckanext-harvest-basket's BasketBasicHarvester, which for updates
    goes through ckanext-harvest's own _create_or_update_package, promoting
    current and setting package_id atomically (see
    ckanext-harvest/harvesters/base.py:316-324). DELWP does not use that
    method at all (it has its own full import_stage, delwp.py:647) but
    reimplements the same atomicity itself: it looks up the guid's current
    previous_harvest_object, flips it to current=False, then sets
    current=True and package_id on the new object together before calling
    package_create/package_update (delwp.py:722-761). Different code, same
    per-guid invariant: at most one current=True row, and its package_id is
    correct.

    current=True may not exist at all for a guid that was previously
    soft-deleted: the delete path clears current=False for every row of a
    deleted guid and never sets a replacement current=True row. Its
    package_id is still recoverable via the delete-marker row's
    report_status="deleted". Neither harvester sets that field itself -
    it's stamped automatically, independent of which import_stage ran, by
    ckanext-harvest's shared queue harness once the row completes (queue.py's
    fetch_and_import_stages: obj.current is False -> "deleted") - so both
    harvesters fall back to that when no current row exists for the guid.

    Where the two harvesters diverge is whether an already-trashed guid
    should be excluded from the result, which is why this is a parameter
    rather than a fixed behaviour:

    * DELWP (active_only=False) needs the trashed guid's package_id kept
      in the result, because gather_stage uses this map to decide
      new-vs-change for every incoming record (delwp.py:359): if a
      previously-deleted dataset reappears in the source, DELWP's package
      ids are random (uuid.uuid4() on create, not derived from the guid -
      delwp.py:776,804), so losing the old package_id here would create a
      duplicate package instead of restoring the original.
    * ODS (active_only=True) must drop already-trashed guids from the
      result, because ODS's purge_missing loop treats every guid in this
      map that is no longer in the current gather as a fresh delete
      candidate (ods.py's gather_stage). ODS's package ids are
      deterministic (uuid5 of dataset_id + source_url -
      ods_harvester.py:206), so it never needs this map to recover an old
      id for a reappearing dataset - reappearance just recomputes the same
      id independently. Keeping an already-trashed guid here would make
      it look "missing" on every subsequent gather forever, queuing a
      fresh no-op delete HarvestObject (package_delete on an
      already-deleted package is a harmless but noisy no-op) on every
      single run.

    Args:
        source_id: the harvest source id to scope the lookup to.
        active_only: when True, only guids whose package is currently
            state=="active" are included (ODS). When False, guids whose
            package is trashed are included too (DELWP).

    Returns:
        guid -> package_id, one entry per guid.
    """
    query = model.Session.query(
        HarvestObject.guid, HarvestObject.package_id
    ).filter(HarvestObject.harvest_source_id == source_id)

    if active_only:
        query = query.join(
            model.Package, model.Package.id == HarvestObject.package_id
        ).filter(model.Package.state == "active")

    query = query.filter(
        or_(
            HarvestObject.current.is_(True),
            and_(
                HarvestObject.current.is_(False),
                HarvestObject.package_id.isnot(None),
                HarvestObject.report_status == "deleted",
            ),
        )
    ).order_by(
        HarvestObject.guid.asc(),
        HarvestObject.current.asc(),
        HarvestObject.gathered.asc(),
    )

    # current=True sorts last per guid (current.asc() puts False before
    # True), so plain assignment lets it win over a "deleted" row for the
    # same guid.
    existing: dict[str, str] = {}
    for guid, package_id in query:
        existing[guid] = package_id

    return existing


class DataVicBaseHarvester(HarvesterBase):
    def __init__(self, **kwargs):
        self.test = kwargs.get("test", False)
        super().__init__(**kwargs)

    def _set_config(self, config_str: str) -> None:
        if config_str:
            self.config = json.loads(config_str)

            if "api_version" in self.config:
                self.api_version = int(self.config["api_version"])
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
        return get_object_extra(harvest_object, key)

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
            log.error(f"Package {package_id} not found. Skipping purge")

    def _make_context(self) -> types.Context:
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
