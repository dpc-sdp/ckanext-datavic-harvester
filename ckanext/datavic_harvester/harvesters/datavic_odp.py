"""CKAN harvester for DD -> ODP harvesting.

Extends ckanext-harvest-basket's CustomCKANHarvester so you can reuse the same
config (tsm_schema, fq, max_datasets, organizations_filter_include, etc.).
Schema and defaults are handled via tsm_schema in the source config.

When "purge_missing": true in the harvest source config, datasets that are
no longer on the remote source are moved to trash (deleted, recoverable). Use a full harvest when using this.
"""
from __future__ import annotations

import json
import logging

import ckan.plugins.toolkit as tk
from ckan import model
from ckanext.harvest.model import HarvestObject
from ckanext.harvest_basket.harvesters import CustomCKANHarvester

log = logging.getLogger(__name__)

_DELETE_MARKER = "status"
_DELETE_VALUE = "delete"


class DataVicODPHarvester(CustomCKANHarvester):
    """DataVic ODP: same config as Custom CKAN (tsm_schema, etc.). Optional: move missing to trash."""

    SRC_ID = "DataVic ODP"

    def info(self):
        return {
            "name": "datavic_odp",
            "title": "DataVic ODP",
            "description": "Harvests from a DataVic/CKAN instance with the same config as Custom CKAN "
            "(tsm_schema, fq, max_datasets, etc.). Set purge_missing to true to move local datasets "
            "no longer on the remote to trash. Use a full harvest.",
            "form_config_interface": "Text",
        }

    def gather_stage(self, harvest_job):
        object_ids = super().gather_stage(harvest_job)
        if not object_ids or not self.config.get("purge_missing"):
            return object_ids

        self._set_config(harvest_job.source.config)
        current_guids = {
            row[0]
            for row in model.Session.query(HarvestObject.guid).filter(
                HarvestObject.harvest_job_id == harvest_job.id
            ).all()
        }
        existing = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(
                HarvestObject.harvest_source_id == harvest_job.source_id,
                HarvestObject.current == True,
                HarvestObject.package_id.isnot(None),
            )
            .distinct()
            .all()
        )
        for (guid, package_id) in existing:
            if guid in current_guids or not package_id:
                continue
            delete_content = json.dumps({
                _DELETE_MARKER: _DELETE_VALUE,
                "package_id": package_id,
                "guid": guid,
            })
            obj = HarvestObject(
                guid=guid,
                job=harvest_job,
                content=delete_content,
                package_id=package_id,
            )
            obj.save()
            object_ids.append(obj.id)
            log.info(
                "%s: queued delete for package %s (guid %s) no longer in source",
                self.SRC_ID,
                package_id,
                guid,
            )
        return object_ids

    def import_stage(self, harvest_object):
        if harvest_object.content:
            try:
                data = json.loads(harvest_object.content)
                if data.get(_DELETE_MARKER) == _DELETE_VALUE:
                    package_id = data.get("package_id")
                    if package_id:
                        self._set_config(harvest_object.source.config)
                        ctx = {
                            "model": model,
                            "session": model.Session,
                            "user": self._get_user_name(),
                            "ignore_auth": True,
                        }
                        tk.get_action("package_delete")(ctx, {"id": package_id})
                        log.info(
                            "%s: moved package %s to trash (no longer in source)",
                            self.SRC_ID,
                            package_id,
                        )
                    return True
            except (ValueError, TypeError):
                pass

        return super().import_stage(harvest_object)
