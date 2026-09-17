"""OpenDataSoft harvester adapted for DataVic needs.

Extends ODSHavester from ckanext-harvest-basket by:

* converting description into markdown using markdownify library
* an offline "test" mode that replays a bundled JSON fixture instead of
  calling the remote portal (see MOCK_FILE)
* purge_missing: when true, datasets no longer present in the remote source
  are moved to trash (soft delete). Restored to active automatically if they
  reappear and the modified date in the source is later than the dataset modified_date.

How a harvest job flows through this class, start to end:

    gather_stage
        |  ODSHarvester.gather_stage() lists datasets from the remote
        |  portal (or the MOCK_FILE fixture in test mode) and creates one
        |  HarvestObject per dataset.
        |
        |  If purge_missing is enabled, this also creates one extra
        |  HarvestObject per previously-harvested package that is no
        |  longer present in the gather, tagged with a "status": "delete"
        |  HarvestObjectExtra.
        v
    fetch_stage (per HarvestObject)
        |  "status": "delete" object --> no-op, passed straight through.
        |  otherwise                 --> ODSHarvester.fetch_stage() fetches
        |                               the full dataset + resource
        |                               metadata; harvest source extras are
        |                               then stamped onto the package.
        v
    import_stage (per HarvestObject)
        |  "status": "delete" object --> package_delete (soft delete /
        |                               moves package to trash).
        |  otherwise                 --> ODSHarvester.import_stage()
        |                               creates/updates the package:
        |                               description converted to markdown,
        |                               fields the harvester does not own
        |                               preserved (_find_existing_package),
        |                               and state reset to "active" (undoes
        |                               any earlier soft delete).
"""
from __future__ import annotations

import json
import logging
from os import path

from typing import Any, Optional

from markdownify import markdownify

import ckan.plugins.toolkit as tk
from ckan import model
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.harvest_basket.harvesters import ODSHarvester
from .base import (
    PRESERVE_PKG_FIELDS,
    add_harvest_source_extras,
    get_existing_guids_to_package_ids,
    get_object_extra,
    get_resource_size,
)

log = logging.getLogger(__name__)

# When test is true, the harvester replays this fixture instead of calling
# the remote portal (see _get_mocked_fixture).
# To regenerate, use /app/ckan/generate_ods_fixture.py script.
MOCK_FILE = "ods_records.json"


class DataVicODSHarvester(ODSHarvester):

    def __init__(self, **kwargs):
        self.test = kwargs.get("test", False)
        super().__init__(**kwargs)

    def info(self):
        return {
            "name": "ods",
            "title": "OpenDataSoft",
            "description": (
                "Harvests datasets from remote Opendatasoft portals. Set "
                "purge_missing to true to move local datasets no longer on "
                "the remote to trash. Use a full harvest when using "
                "purge_missing."
            ),
        }

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

    def gather_stage(self, harvest_job) -> list[str]:
        object_ids = super().gather_stage(harvest_job)

        if not object_ids or not self.config.get("purge_missing"):
            return object_ids

        # guids_in_source: guids the gather above just returned for this job
        # (super().gather_stage() already created one HarvestObject per guid).
        # existing: guid -> package_id for every active package previously
        # harvested from this source, deduplicated to one row per guid (see
        # get_existing_guids_to_package_ids in base.py).
        #
        # Any guid in `existing` that is not in `guids_in_source` is no
        # longer returned by the gather, so it is queued for delete: a new
        # HarvestObject tagged "status": "delete" is created for it, and
        # every existing HarvestObject row for that guid is marked
        # current=False so the stale row no longer masks the delete object
        # as the current one for the guid.
        guids_in_source = {
            row[0]
            for row in model.Session.query(HarvestObject.guid)
            .filter(HarvestObject.harvest_job_id == harvest_job.id)
            .all()
        }

        # active_only=True: a guid already trashed by a previous
        # purge_missing run must not come back here (see
        # get_existing_guids_to_package_ids docstring for why ODS need this),
        # otherwise it would look "missing" on every subsequent gather forever
        # and get a fresh no-op delete HarvestObject queued on every single run.
        existing = get_existing_guids_to_package_ids(
            harvest_job.source_id, active_only=True
        )

        for guid, package_id in existing.items():
            if guid in guids_in_source:
                continue

            obj = HarvestObject(
                guid=guid,
                job=harvest_job,
                package_id=package_id,
                extras=[
                    HarvestObjectExtra(key="status", value="delete")
                ],
            )

            model.Session.query(HarvestObject).filter_by(
                guid=guid, harvest_source_id=harvest_job.source_id
            ).update({"current": False}, False)

            obj.save()
            object_ids.append(obj.id)
            log.info(
                "%s: queued delete for package %s (guid %s) no longer in source",
                self.SRC_ID,
                package_id,
                guid,
            )

        return object_ids

    def _is_delete_object(self, harvest_object) -> bool:
        if not harvest_object:
            return False

        return get_object_extra(harvest_object, "status") == "delete"

    def fetch_stage(self, harvest_object) -> bool:
        if self._is_delete_object(harvest_object):
            return True

        result = super().fetch_stage(harvest_object)

        if result is True:
            self._add_harvest_source_extras(harvest_object)

        return result

    def _add_harvest_source_extras(self, harvest_object) -> None:
        """Store the harvest source identifiers on the package itself.

        @see base.add_harvest_source_extras for why this is needed. Applied
        here in fetch_stage because _pre_map_stage resets extras to []
        (ods_harvester.py:230) and only has (package_dict, source_url) - no
        access to the harvest object's source - so it cannot inject these
        itself.
        """
        try:
            package_dict = json.loads(harvest_object.content)
        except (ValueError, TypeError):
            log.warning(
                "%s: could not parse content for object %s while adding "
                "harvest source extras",
                self.SRC_ID,
                harvest_object.id,
            )
            return

        add_harvest_source_extras(package_dict, harvest_object.source)
        harvest_object.content = json.dumps(package_dict)

    def import_stage(self, harvest_object):
        if self._is_delete_object(harvest_object):
            package_id = harvest_object.package_id

            if not package_id:
                log.warning(
                    "%s: delete object %s has no package_id",
                    self.SRC_ID,
                    harvest_object.id,
                )
                return True

            try:
                tk.get_action("package_delete")(
                    {
                        "model": model,
                        "session": model.Session,
                        "user": self._get_user_name(),
                        "ignore_auth": True,
                    },
                    {"id": package_id},
                )
            except tk.ObjectNotFound:
                log.warning(
                    "%s: package %s already gone, nothing to delete",
                    self.SRC_ID,
                    package_id,
                )
                return True

            log.info(
                "%s: moved package %s to trash (no longer in source)",
                self.SRC_ID,
                package_id,
            )
            return True

        return super().import_stage(harvest_object)

    def _pre_map_stage(self, package_dict: dict, source_url: str) -> None:
        super()._pre_map_stage(package_dict, source_url)

        # A dataset previously soft-deleted by purge_missing must come back
        # out of the trash when it reappears in the source. package_update
        # leaves a missing state untouched, so set it explicitly. Safe as a
        # no-op for datasets that were never deleted.
        package_dict["state"] = "active"

    def _find_existing_package(self, package_dict: dict[str, Any]) -> dict[str, Any]:
        """Carry forward fields the harvester does not own.

        @override
        package_update is a full replace, so any field absent from
        package_dict is dropped - most importantly syndicated_id, which
        breaks the link to the syndicated copy on the DV portal if lost.
        Only runs on the update path - on create, super() raises
        ObjectNotFound and this never executes, which is correct since
        there's nothing to preserve for a brand new package.
        """
        existing = super()._find_existing_package(package_dict)

        # setdefault: the harvester's own value always wins where it set one.
        for key in PRESERVE_PKG_FIELDS:
            if key in existing:
                package_dict.setdefault(key, existing[key])

        # _pre_map_stage resets extras to [] (ods_harvester.py:230), so merge
        # element-wise rather than assigning - otherwise existing free-form
        # extras are lost. Keys the harvester set are left untouched.
        package_dict.setdefault("extras", [])
        harvester_keys = {e.get("key") for e in package_dict["extras"]}
        for extra in existing.get("extras", []):
            if extra.get("key") not in harvester_keys:
                package_dict["extras"].append(
                    {"key": extra["key"], "value": extra["value"]}
                )

        return existing

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
        """For CSV resources, append a delimiter parameter to the URL.

        Without it, some CSV exports from this source are not parsed into
        columns correctly when previewed/loaded downstream.
        """
        resources = super()._fetch_resources(source_url, resource_urls, pkg_data)
        for res in resources:
            if res["format"] == "CSV":
                res["url"] = f'{res["url"]}?delimiter=%2C'

            # All resources are set to 1KB in test mode to avoid network calls.
            res["size"] = 1024 if self.test else get_resource_size(res["url"])
            res["filesize"] = res["size"]

        return resources
