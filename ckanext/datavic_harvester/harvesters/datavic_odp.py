"""CKAN harvester for DD -> ODP harvesting.

Inherits CKANHarvester + BasketBasicHarvester (same as CustomCKANHarvester):
tsm_schema, fq, max_datasets, transmute, fetch_stage type fix, etc.
Adds purge_missing: when true, datasets no longer on the remote are moved to trash.
Use a full harvest when using purge_missing.
"""
from __future__ import annotations

import json
import logging

import requests
from requests.exceptions import HTTPError, RequestException

import ckan.plugins.toolkit as tk
from ckan import model
from ckanext.harvest.harvesters import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError
from ckanext.harvest.model import HarvestObject
from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester

log = logging.getLogger(__name__)

_DELETE_MARKER = "status"
_DELETE_VALUE = "delete"


class DataVicODPHarvester(CKANHarvester, BasketBasicHarvester):
    """DataVic ODP: same config as Custom CKAN plus optional purge_missing (move removed to trash)."""

    SRC_ID = "DataVic ODP"

    def info(self):
        return {
            "name": "datavic_odp",
            "title": "DataVic ODP",
            "description": "Harvests from a DataVic/CKAN instance with the same config as Custom CKAN "
            "(tsm_schema, fq, max_datasets, etc.). Set purge_missing to true to move local datasets "
            "no longer on the remote to trash. Use a full harvest when using purge_missing.",
            "form_config_interface": "Text",
        }

    def gather_stage(self, harvest_job):
        object_ids = super().gather_stage(harvest_job)

        """ Start of purge_missing logic """
        if not object_ids or not self.config.get("purge_missing"):
            return object_ids

        self._set_config(harvest_job.source.config)
        current_guids = {
            row[0]
            for row in model.Session.query(HarvestObject.guid).filter(
                HarvestObject.harvest_job_id == harvest_job.id
            ).all()
        }
        existing = self._get_existing_guids_to_package_ids(harvest_job.source_id)
        for guid, package_id in existing.items():
            if guid in current_guids:
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
            model.Session.query(HarvestObject).\
                filter_by(guid=guid).\
                update({'current': False}, False)
            obj.save()
            object_ids.append(obj.id)
            log.info(
                "%s: queued delete for package %s (guid %s) no longer in source",
                self.SRC_ID,
                package_id,
                guid,
            )
        """ End of purge_missing logic """

        return object_ids

    def _get_existing_guids_to_package_ids(self, source_id: str) -> dict[str, str]:
        # HarvestObject.current is unreliable: upstream import_stage
        # short-circuits "unchanged" runs (base.py:311-314), spawning
        # current=False, package_id=NULL rows that mask the original
        # current=True row. Walk newest-to-oldest, ignoring NULL
        # package_ids and trashed packages.
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .join(model.Package, model.Package.id == HarvestObject.package_id)
            .filter(
                HarvestObject.harvest_source_id == source_id,
                HarvestObject.package_id.isnot(None),
                model.Package.state == "active",
            )
            .order_by(
                HarvestObject.guid.asc(),
                HarvestObject.gathered.desc().nullslast(),
            )
        )
        existing: dict[str, str] = {}
        for guid, package_id in query:
            existing.setdefault(guid, package_id)
        return existing

    def import_stage(self, harvest_object):
        """ Start of purge_missing logic """
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

        if not harvest_object.content:
            return False
        """ End of purge_missing logic """

        try:
            package_dict = json.loads(harvest_object.content)
            self._set_config(harvest_object.source.config)
            self._transmute_content(package_dict)
            harvest_object.content = json.dumps(package_dict)
            return super().import_stage(harvest_object)
        except Exception as e:
            log.error(f"{self.SRC_ID}: import stage failed: {e}")
            return False

    """ The same as CustomCKANHarvester """
    def _search_for_datasets(self, remote_ckan_base_url, fq_terms=None):
        if fq_terms is None:
            fq_terms = []
        if fq := self.config.get("fq", ""):
            fq_terms.append(fq)

        pkg_dicts = super()._search_for_datasets(remote_ckan_base_url, fq_terms)
        max_datasets = int(self.config.get("max_datasets", 0))
        return pkg_dicts[:max_datasets] if max_datasets else pkg_dicts

    def _get_content(self, url):
        """Fetch remote content, sending the API key in both the ``Authorization``
        and ``X-CKAN-API-Key`` headers.

        ODP authenticates via ``X-CKAN-API-Key`` (its configured
        ``apitoken_header_name``), which the base ``_get_content`` does not send.
        """
        headers = {}

        user_agent = self.config.get("user_agent")
        if user_agent:
            headers["User-Agent"] = str(user_agent)

        api_key = self.config.get("api_key")
        if api_key:
            headers["Authorization"] = api_key
            headers["X-CKAN-API-Key"] = api_key

        try:
            http_request = requests.get(url, headers=headers)
        except HTTPError as e:
            raise ContentFetchError(
                "HTTP error: %s %s" % (e.response.status_code, e.request.url)
            )
        except RequestException as e:
            raise ContentFetchError("Request error: %s" % e)
        except Exception as e:
            raise ContentFetchError("HTTP general exception: %s" % e)
        return http_request.text

    def _search_datasets(self, remote_url: str):
        url = remote_url.rstrip("/") + "/api/action/package_search?rows=1"
        resp = self._make_request(url)

        if not resp:
            return

        try:
            package_dict = json.loads(resp.text)["result"]["results"]
        except (ValueError, KeyError) as e:
            err_msg: str = f"{self.SRC_ID}: response JSON doesn't contain result: {e}"
            log.error(err_msg)
            raise SearchError(err_msg)

        return package_dict

    def fetch_stage(self, harvest_object):
        data_dict = json.loads(harvest_object.content)
        data_dict["type"] = "dataset"
        harvest_object.content = json.dumps(data_dict)
        return super().fetch_stage(harvest_object)

    def _pre_map_stage(self, data_dict, source_url):
        data_dict["type initial"] = data_dict["type"]
        data_dict["type"] = "dataset"
        return data_dict

    def transmute_data(self, data, schema):
        if schema:
            tk.get_action("tsm_transmute")(
                {
                    "model": model,
                    "session": model.Session,
                    "user": self._get_user_name(),
                },
                {"data": data, "schema": schema},
            )
