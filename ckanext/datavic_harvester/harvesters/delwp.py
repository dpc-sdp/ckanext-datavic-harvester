from __future__ import annotations

import json
import logging
import os
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from os import path
from typing import Iterator, Optional, Any

from bs4 import BeautifulSoup, Tag
import requests
from sqlalchemy import and_, or_

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.logic.schema import default_create_package_schema
from ckan.lib.uploader import get_storage_path

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestObjectExtra

import ckanext.datavic_harvester.helpers as helpers
from ckanext.datavic_harvester.harvesters.base import (
    DataVicBaseHarvester,
    get_resource_size,
)


log = logging.getLogger(__name__)
HASH_FIELD = "harvester_data_hash"


class DelwpHarvester(DataVicBaseHarvester):
    HARVESTER = "DELWP Harvester"

    def info(self):
        return {
            "name": "delwp",
            "title": self.HARVESTER,
            "description": "Harvester for DELWP dataset descriptions serialized as JSON",
        }

    def validate_config(self, config: Optional[str]) -> str:
        config_obj = json.loads(super().validate_config(config))

        if "full_metadata_url_prefix" not in config_obj:
            raise ValueError("full_metadata_url_prefix must be set")

        if "{UUID}" not in config_obj.get("full_metadata_url_prefix", ""):
            raise ValueError(
                "full_metadata_url_prefix must have the {UUID} identifier in the URL"
            )

        if "resource_url_prefix" not in config_obj:
            raise ValueError("resource_url_prefix must be set")

        if "license_id" not in config_obj:
            raise ValueError("license_id must be set")

        if "resource_attribution" not in config_obj:
            raise ValueError("resource_attribution must be set")

        if "dataset_type" not in config_obj:
            raise ValueError("dataset_type must be set")

        if "api_auth" not in config_obj:
            raise ValueError("api_auth must be set")

        if "organisation_mapping" not in config_obj:
            self._validate_optional_deletion_safeguard_config(config_obj)
            return json.dumps(config_obj, indent=4)

        self._validate_organisation_mapping(config_obj)
        self._validate_optional_deletion_safeguard_config(config_obj)

        return json.dumps(config_obj, indent=4)

    def _validate_optional_deletion_safeguard_config(
        self, config: dict[str, Any]
    ) -> None:
        if "deletion_safeguard_enabled" in config:
            v = config["deletion_safeguard_enabled"]
            if isinstance(v, bool):
                pass
            elif isinstance(v, str):
                try:
                    config["deletion_safeguard_enabled"] = tk.asbool(v)
                except ValueError as e:
                    raise ValueError(
                        "deletion_safeguard_enabled must be a boolean"
                    ) from e
            else:
                raise ValueError("deletion_safeguard_enabled must be a boolean")

        if "deletion_safeguard_allow_bulk_delete" in config:
            v = config["deletion_safeguard_allow_bulk_delete"]
            if isinstance(v, bool):
                pass
            elif isinstance(v, str):
                try:
                    config["deletion_safeguard_allow_bulk_delete"] = tk.asbool(v)
                except ValueError as e:
                    raise ValueError(
                        "deletion_safeguard_allow_bulk_delete must be a boolean"
                    ) from e
            else:
                raise ValueError("deletion_safeguard_allow_bulk_delete must be a boolean")

        if "deletion_safeguard_drop_threshold_percent" in config:
            try:
                drop_threshold_percent = float(
                    config["deletion_safeguard_drop_threshold_percent"]
                )
            except (TypeError, ValueError):
                raise ValueError(
                    "deletion_safeguard_drop_threshold_percent must be a number"
                )

            if drop_threshold_percent < 0 or drop_threshold_percent > 100:
                raise ValueError(
                    "deletion_safeguard_drop_threshold_percent must be >= 0 and <= 100"
                )

        if "deletion_safeguard_min_previous_count" in config:
            try:
                min_previous_count = int(config["deletion_safeguard_min_previous_count"])
            except (TypeError, ValueError):
                raise ValueError("deletion_safeguard_min_previous_count must be an integer")

            if min_previous_count < 0:
                raise ValueError("deletion_safeguard_min_previous_count must be >= 0")

        for _key in (
            "deletion_safeguard_notify_ok_url",
            "deletion_safeguard_notify_anomaly_url",
        ):
            if _key in config:
                _v = config[_key]
                if _v is not None and not isinstance(_v, str):
                    raise ValueError(f"{_key} must be a string")

    def _validate_organisation_mapping(self, config: dict[str, Any]) -> None:
        if not isinstance(config["organisation_mapping"], list):
            raise ValueError("organisation_mapping must be a *list* of organisations")

        for organisation in config["organisation_mapping"]:
            if not isinstance(organisation, dict):
                raise ValueError(
                    'organisation_mapping item must be a *dict*. eg {"resowner": "Organisation A", "org-name": "organisation-a"}'
                )

            resowner: Optional[str] = organisation.get("resowner")
            org_name: Optional[str] = organisation.get("org-name")

            if not resowner:
                raise ValueError(
                    'organisation_mapping item must have property "resowner". eg "resowner": "Organisation A"'
                )

            if not org_name:
                raise ValueError(
                    'organisation_mapping item must have property "org-name". eg "org-name": "organisation-a"}'
                )

            if not self._get_organization(org_name):
                raise ValueError(f"Organisation {org_name} not found")

    def _get_organization(self, org_name: str) -> model.Group | None:
        return (
            model.Session.query(model.Group)
            .filter_by(name=org_name, is_organization=True)
            .first()
        )

    def _get_harvest_json_retention_days(self) -> int:
        """Return retention days from env; 0 disables storing. Default 7 if unset or invalid."""
        raw = os.environ.get("DELWP_HARVEST_JSON_RETENTION_DAYS", "7")
        try:
            return int(raw)
        except ValueError:
            return 7

    def _get_harvest_filestore_dir(self) -> Optional[str]:
        storage_path = get_storage_path()
        if not storage_path:
            return None
        return path.join(storage_path, "harvest", "delwp")

    def _parse_date_from_harvest_filename(self, filename: str) -> Optional[datetime]:
        """Parse YYYY-MM-DD from filename like YYYY-MM-DD_HH-MM-SS_<job_id>.json."""
        if not filename.endswith(".json"):
            return None
        parts = filename.split("_")
        if len(parts) < 1:
            return None
        try:
            return datetime.strptime(parts[0], "%Y-%m-%d")
        except ValueError:
            return None

    def _cleanup_old_harvest_json_files(
        self, dir_path: str, retention_days: int
    ) -> None:
        if not path.isdir(dir_path):
            return
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=retention_days)).date()
        for name in os.listdir(dir_path):
            if not name.endswith(".json"):
                continue
            file_date = self._parse_date_from_harvest_filename(name)
            if file_date is None:
                continue
            if file_date.date() < cutoff_date:
                filepath = path.join(dir_path, name)
                try:
                    os.remove(filepath)
                    log.info(
                        "%s: removed old harvest JSON (retention): %s",
                        self.HARVESTER,
                        name,
                    )
                except OSError as e:
                    log.warning(
                        "%s: failed to remove harvest JSON %s: %s",
                        self.HARVESTER,
                        name,
                        e,
                    )

    def _save_harvest_json_to_filestore(
        self, records: list[dict[str, Any]], job_id: Any
    ) -> None:
        retention_days = self._get_harvest_json_retention_days()
        if retention_days == 0:
            return
        dir_path = self._get_harvest_filestore_dir()
        if not dir_path:
            log.warning(
                "%s: ckan.storage_path not set, skipping harvest JSON filestore save",
                self.HARVESTER,
            )
            return

        self._cleanup_old_harvest_json_files(dir_path, retention_days)

        try:
            os.makedirs(dir_path, exist_ok=True)
        except OSError as e:
            log.warning(
                "%s: could not create harvest filestore dir %s: %s",
                self.HARVESTER,
                dir_path,
                e,
            )
            return

        # Re-playable shape: same as API response with "records" key, other keys are ignored.
        payload = {"records": records}

        # Filename: date + time + job_id so multiple runs per day are allowed.
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"{timestamp}_{job_id}.json"
        filepath = path.join(dir_path, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
            log.info(
                "%s: saved harvest JSON to %s (%d records)",
                self.HARVESTER,
                filename,
                len(records),
            )
        except OSError as e:
            log.warning(
                "%s: could not write harvest JSON %s: %s",
                self.HARVESTER,
                filepath,
                e,
            )

    def gather_stage(self, harvest_job):
        log.debug(f"In {self.HARVESTER} gather_stage")

        self._set_config(harvest_job)

        harvest_object_ids = []
        # guid_to_package_id includes soft-deleted rows so reappearing guids can
        # reuse their original package_id — see _get_guids_to_package_ids.
        # current_guids is the active set only, used for deletion detection so we
        # don't re-delete guids that were already deleted in a prior run.
        guid_to_package_id = self._get_guids_to_package_ids(harvest_job.source.id)
        current_guids = self._get_current_harvest_guids(harvest_job.source.id)
        guids_in_source: list[str] = []

        records = self._fetch_records_from_remote_portal(
            harvest_job.source.url.rstrip("?")
        )
        self._save_harvest_json_to_filestore(records, harvest_job.id)

        previous_count = len(current_guids)
        source_count = len(records)
        anomaly_detected, anomaly_message = self._detect_deletion_anomaly(
            previous_count, source_count
        )
        if anomaly_detected and anomaly_message:
            self._save_gather_error(anomaly_message, harvest_job)
            self._send_deletion_safeguard_notify(anomaly=True)
        elif self.deletion_safeguard_enabled:
            self._send_deletion_safeguard_notify(anomaly=False)

        for record in records:
            uuid = record["fields"]["uuid"]

            status = "change" if uuid in guid_to_package_id and guid_to_package_id[uuid] is not None else "new"

            # Create harvest object with appropriate status based on if dataset
            # already exists in the database
            obj = HarvestObject(
                guid=uuid,
                job=harvest_job,
                content=json.dumps(record["fields"]),
                extras=[
                    HarvestObjectExtra(key="status", value=status)
                ],
            )

            if status == "change":
                obj.package_id = guid_to_package_id[uuid]  # type: ignore

            obj.save()

            harvest_object_ids.append(obj.id)
            guids_in_source.append(uuid)
            log.debug("%s: harvest object id=%s guid=%s status=%s", self.HARVESTER, obj.id, uuid, status)

        # Only active (current=True) guids are candidates for deletion. Soft-deleted
        # rows in guid_to_package_id have already been deleted in a prior run —
        # re-deleting them creates noisy duplicate delete harvest_objects.
        guids_to_delete = current_guids - set(guids_in_source)
        if anomaly_detected and not self.deletion_safeguard_allow_mass_delete:
            log.warning(
                "%s: anomaly detected, suppressing %d delete action(s) for this run",
                self.HARVESTER,
                len(guids_to_delete),
            )
            guids_to_delete = set()

        if guids_to_delete:
            log.info(
                "%s: marking %d dataset(s) for deletion (not in source)",
                self.HARVESTER,
                len(guids_to_delete),
            )
        for guid in guids_to_delete:
            obj = HarvestObject(
                guid=guid,
                job=harvest_job,
                package_id=guid_to_package_id[guid],
                extras=[HarvestObjectExtra(key="status", value="delete")],
            )

            model.Session.query(HarvestObject).filter_by(guid=guid).update(
                {"current": False}, False
            )

            obj.save()

            harvest_object_ids.append(obj.id)

        log.info(
            "%s: gather_stage finished, total harvest objects: %d (from source: %d, to delete: %d)",
            self.HARVESTER,
            len(harvest_object_ids),
            len(guids_in_source),
            len(guids_to_delete),
        )
        return harvest_object_ids

    def _set_config(self, harvest_item: HarvestJob | HarvestObject) -> None:
        super()._set_config(harvest_item.source.config)

        _test = self.config.get("test", False)
        self.test = tk.asbool(False if _test is None else _test)
        self.source_org_id = self._get_source_owner_org_id(harvest_item.source.id)
        _dse = self.config.get("deletion_safeguard_enabled", True)
        self.deletion_safeguard_enabled = tk.asbool(True if _dse is None else _dse)
        self.deletion_safeguard_drop_threshold_percent = float(
            self.config.get("deletion_safeguard_drop_threshold_percent", 10)
        )
        self.deletion_safeguard_min_previous_count = int(
            self.config.get("deletion_safeguard_min_previous_count", 100)
        )
        _amd = self.config.get("deletion_safeguard_allow_bulk_delete", False)
        self.deletion_safeguard_allow_mass_delete = tk.asbool(
            False if _amd is None else _amd
        )

        def _strip_url(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v).strip()
            return s or None

        self.deletion_safeguard_notify_ok_url = _strip_url(
            self.config.get("deletion_safeguard_notify_ok_url")
        )
        self.deletion_safeguard_notify_anomaly_url = _strip_url(
            self.config.get("deletion_safeguard_notify_anomaly_url")
        )

        if "geoserver_dns" in self.config:
            geoserver_dns = self.config["geoserver_dns"]

            self.geoserver_urls = {
                "WMS": {
                    "geoserver_url": f"{geoserver_dns}/geoserver/ows?service=WMS&request=getCapabilities",
                    "resource_url": f"{geoserver_dns}/geoserver/wms?service=wms&request=getmap&format=image%2Fpng8&transparent=true&layers={{layername}}&width=512&height=512&crs=epsg%3A3857&bbox=16114148.554967716%2C-4456584.4971389165%2C16119040.524777967%2C-4451692.527328665",
                },
                "WFS": {
                    "geoserver_url": f"{geoserver_dns}/geoserver/ows?service=WFS&request=getCapabilities",
                    "resource_url": f"{geoserver_dns}/geoserver/wfs?request=GetCapabilities&service=WFS",
                },
            }

    def _detect_deletion_anomaly(
        self, previous_count: int, source_count: int
    ) -> tuple[bool, Optional[str]]:
        """True when the fetch returned far fewer rows than the current-GUID baseline (API glitch)."""
        if not self.deletion_safeguard_enabled:
            log.info(
                "%s: deletion safeguard disabled (source_count=%d, previous_count=%d)",
                self.HARVESTER,
                source_count,
                previous_count,
            )
            return False, None

        if previous_count < self.deletion_safeguard_min_previous_count:
            log.info(
                "%s: deletion safeguard check skipped (previous_count=%d < min_previous_count=%d, "
                "source_count=%d)",
                self.HARVESTER,
                previous_count,
                self.deletion_safeguard_min_previous_count,
                source_count,
            )
            return False, None

        if previous_count == 0:
            log.info(
                "%s: deletion safeguard skipped (previous_count=0, source_count=%d)",
                self.HARVESTER,
                source_count,
            )
            return False, None

        drop_percent = ((previous_count - source_count) / previous_count) * 100
        if drop_percent <= self.deletion_safeguard_drop_threshold_percent:
            log.info(
                "%s: deletion safeguard ok (source_count=%d, previous_count=%d, "
                "drop_percent=%.2f, drop_threshold_percent=%s)",
                self.HARVESTER,
                source_count,
                previous_count,
                drop_percent,
                self.deletion_safeguard_drop_threshold_percent,
            )
            return False, None

        message = (
            f"{self.HARVESTER}: anomaly detected for source count drop "
            f"(source_count={source_count}, previous_count={previous_count}, "
            f"drop_percent={drop_percent:.2f}, "
            f"drop_threshold_percent={self.deletion_safeguard_drop_threshold_percent}, "
            f"allow_mass_delete={self.deletion_safeguard_allow_mass_delete})."
        )
        log.warning(message)
        return True, message

    def _send_deletion_safeguard_notify(self, *, anomaly: bool) -> None:
        """GET a monitoring URL (fail-open: errors are logged only).

        Set ``deletion_safeguard_notify_ok_url`` and/or
        ``deletion_safeguard_notify_anomaly_url`` in source config (full URLs).
        """
        target_url = (
            self.deletion_safeguard_notify_anomaly_url
            if anomaly
            else self.deletion_safeguard_notify_ok_url
        )
        if not target_url:
            return

        try:
            requests.get(target_url, timeout=10)
        except requests.RequestException as e:
            log.warning(
                "%s: deletion safeguard notify GET failed (%s): %s",
                self.HARVESTER,
                target_url,
                e,
            )

    def _get_source_owner_org_id(self, source_id: str) -> str:
        source_package = model.Package.get(source_id)

        if not hasattr(source_package, "owner_org"):
            # should never happen
            raise ValueError(f"Source package {source_id} does not have an owner_org")

        return source_package.owner_org  # type: ignore

    def _fetch_records_from_remote_portal(
        self, harvest_source_url: str
    ) -> list[dict[str, Any]]:
        page: int = 1
        records_per_page: int = 500

        records = []

        while True:
            result = self._fetch_records(harvest_source_url, page, records_per_page)

            if not result:
                log.debug("%s: empty document at page %d, no more records", self.HARVESTER, page)
                break

            records.extend(result)
            log.debug(
                "%s: page %d returned %d records (total so far: %d)",
                self.HARVESTER, page, len(result), len(records)
            )

            if self.test:
                log.debug("%s: test mode, stopping after first page", self.HARVESTER)
                break

            page = page + 1

        log.info("%s: fetched %d total records from remote portal", self.HARVESTER, len(records))
        return records

    def _get_current_harvest_guids(self, source_id: str) -> set[str]:
        """Active GUIDs for this source (current=True only).

        Used both as the deletion-safeguard baseline and as the candidate set for
        deletion detection. Soft-deleted rows are deliberately excluded so
        previously deleted GUIDs are not re-marked for deletion on subsequent runs.
        """
        rows = (
            model.Session.query(HarvestObject.guid)
            .filter(HarvestObject.harvest_source_id == source_id)
            .filter(HarvestObject.current.is_(True))
            .distinct()
            .all()
        )
        return {row[0] for row in rows}

    def _get_guids_to_package_ids(self, source_id: str) -> dict[str, str]:
        # A dataset may be soft-deleted in one harvest (current=False, package kept)
        # and then reappear in a later harvest. Include those deleted rows so the
        # original package_id can be reused instead of creating a new suffixed package.
        # The ordering is intentional: the dict comprehension keeps the last row seen
        # for each guid, so current rows win over deleted ones, and otherwise the most
        # recently gathered deleted row wins.
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(HarvestObject.harvest_source_id == source_id)
            .filter(
                or_(
                    HarvestObject.current == True,
                    and_(
                        HarvestObject.current == False,
                        HarvestObject.package_id.isnot(None),
                        HarvestObject.report_status == "deleted",
                    ),
                )
            )
            .order_by(
                HarvestObject.guid.asc(),
                HarvestObject.current.asc(),
                HarvestObject.gathered.asc(),
            )
        )

        return {
            harvest_object.guid: harvest_object.package_id for harvest_object in query
        }

    def _fetch_records(
        self, url: str, page: int, records_per_page: int = 100
    ) -> Optional[list[dict[str, Any]]]:
        start = 0 if page == 1 else ((page - 1) * records_per_page)

        request_url: str = "{}?dataset={}&start={}&rows={}&format=json".format(
            url, self.config["dataset_type"], start, records_per_page
        )

        log.debug(f"{self.HARVESTER}: getting page of records {request_url}")

        resp_text: Optional[str] = (
            self._get_mocked_records()
            if self.test
            else self._make_request(
                request_url,
                {"Authorization": self.config["api_auth"]},
            )
        )

        if not resp_text:
            log.warning("%s: empty response from %s (page %d)", self.HARVESTER, request_url, page)
            return

        return json.loads(resp_text).get("records")

    def _get_record_metadata(self, datasets) -> Iterator[dict[str, Any]]:
        """Fetch remote portal record data from `fields` field. The field
        is a dict with all the dataset metadata."""
        if not isinstance(datasets, list):
            if isinstance(datasets, dict):
                datasets = [datasets]
            else:
                log.debug("Datasets data is not a list: %s", type(datasets))
                raise ValueError("Wrong JSON object")

        for dataset in datasets:
            yield dataset.get("fields", {})

    def import_stage(self, harvest_object: HarvestObject) -> bool | str:
        if not harvest_object:
            log.error(f"{self.HARVESTER}: no harvest object received")
            return False

        status = self._get_object_extra(harvest_object, "status")  # type: ignore
        log.debug(
            "%s: import_stage object id=%s guid=%s status=%s package_id=%s",
            self.HARVESTER,
            harvest_object.id,
            harvest_object.guid,
            status,
            harvest_object.package_id,
        )

        if status == "delete":
            log.info(
                "%s: deleting package id=%s (guid=%s)",
                self.HARVESTER,
                harvest_object.package_id,
                harvest_object.guid,
            )
            self._delete_package(
                str(harvest_object.package_id), str(harvest_object.guid)
            )
            return True

        if harvest_object.content is None:
            self._save_object_error(
                f"{self.HARVESTER}: Empty content for object {harvest_object.id}",
                harvest_object,
                "Import",
            )
            return False

        if harvest_object.guid is None:
            self._save_object_error(
                f"Empty guid for object {harvest_object.id}", harvest_object, "Import"
            )
            return False

        self._set_config(harvest_object)

        # Validate before setting current=True to prevent orphaned harvest_objects
        pkg_dict = self._get_pkg_dict(harvest_object)

        if not pkg_dict["notes"] or not pkg_dict["owner_org"]:
            msg = "Description or organization field for package {} is missing for object {}, skipping...".format(
                pkg_dict["title"], harvest_object.id
            )
            log.info(msg)
            self._save_object_error(msg, harvest_object, "Import")
            return False

        # Remove restricted Datasets
        if pkg_dict["private"]:
            msg = "Dataset {} is Restricted for object {}, skipping...".format(
                pkg_dict["title"], harvest_object.id
            )
            log.info(msg)
            self._save_object_error(msg, harvest_object, "Import")
            return False

        if status not in ["new", "change"]:
            return True

        # Set current=True only after validation passes
        previous_harvest_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .filter(HarvestObject.current == True)
            .first()
        )

        if previous_harvest_object:
            previous_harvest_object.current = False
            model.Session.add(previous_harvest_object)

        harvest_object.current = True
        model.Session.add(harvest_object)

        context = self._make_context()
        data_hash = self._calculate_hash_for_data_dict(pkg_dict)

        if status == "new":
            context["schema"] = self._create_custom_package_create_schema()

            pkg_dict["id"] = str(uuid.uuid4())

            harvest_object.package_id = pkg_dict["id"]
            model.Session.add(harvest_object)

            model.Session.execute(
                "SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED"
            )
            model.Session.flush()

            pkg_dict[HASH_FIELD] = data_hash
        elif status == "change":
            pkg_dict["id"] = harvest_object.package_id
            pkg = model.Package.get(pkg_dict["id"])

            if not pkg:
                # Package was likely purged after gather stage.
                # Re-create it as a new dataset to keep in sync with harvest source.
                log.warning(
                    f"Dataset not found for status='change'. "
                    f"GUID: {harvest_object.guid}; package_id: {harvest_object.package_id}. "
                    f"Re-creating as new dataset."
                )

                # Treat as new: generate new ID and set up for package_create
                status = "new"
                context["schema"] = self._create_custom_package_create_schema()

                pkg_dict["id"] = str(uuid.uuid4())
                harvest_object.package_id = pkg_dict["id"]
                model.Session.add(harvest_object)

                model.Session.execute(
                    "SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED"
                )
                model.Session.flush()

                pkg_dict[HASH_FIELD] = data_hash
            else:
                previous_hash = pkg.extras.get(HASH_FIELD)
                needs_restore = pkg.state != "active"

                if previous_hash == data_hash and not needs_restore:
                    log.info(
                        "%s: no changes to dataset id=%s (%s), skipping (hash unchanged)",
                        self.HARVESTER,
                        harvest_object.package_id,
                        pkg.title,
                    )
                    return "unchanged"
                else:
                    if needs_restore:
                        log.info(
                            "%s: restoring dataset id=%s (%s) to active state",
                            self.HARVESTER,
                            harvest_object.package_id,
                            pkg.title,
                        )
                        
                    if previous_hash != data_hash:
                        log.info(
                            "%s: dataset id=%s (%s) has changed, updating.",
                            self.HARVESTER,
                            harvest_object.package_id,
                            pkg.title,
                        )
                    # Force state=active to handle both normal updates and soft-deleted
                    pkg_dict["state"] = "active"
                    pkg_dict[HASH_FIELD] = data_hash

        action: str = "package_create" if status == "new" else "package_update"
        status: str = "Created" if status == "new" else "Updated"
        log.debug("%s: calling action=%s for guid=%s", self.HARVESTER, action, harvest_object.guid)

        try:
            context["return_id_only"] = False
            dataset = tk.get_action(action)(context, pkg_dict)
            log.info(
                "%s: %s dataset with id %s (%s)",
                self.HARVESTER,
                status,
                dataset["id"],
                dataset["title"],
            )
            model.Session.commit()
            return True
        except Exception as e:
            log.error(
                "%s: error %s dataset %s (guid=%s): %s",
                self.HARVESTER,
                action,
                pkg_dict.get("name", ""),
                harvest_object.guid,
                e,
                exc_info=True,
            )
            # Rollback before _save_object_error (which commits)
            model.Session.rollback()
            self._save_object_error(
                f"Error importing dataset {pkg_dict.get('name', '')}: {e} / {traceback.format_exc()}",
                harvest_object,
                "Import",
            )
            return False

    def _get_pkg_dict(self, harvest_object):
        """Create a pkg_dict from remote portal data"""
        content = harvest_object.content
        uuid = harvest_object.guid

        metashare_dict = json.loads(content)
        metashare_dict["_uuid"] = uuid

        remote_pkg_name: Optional[str] = metashare_dict.get("name")
        remote_topiccat: Optional[str] = metashare_dict.get("topiccat")

        full_metadata_url = (
            self.config["full_metadata_url_prefix"].format(**{"UUID": uuid})
            if self.config.get("full_metadata_url_prefix")
            else ""
        )

        access_notes = """
            Aerial imagery and elevation datasets\n
            You can access high-resolution aerial imagery and elevation (LiDAR point cloud) datasets by contacting a business that holds a commercial license.\n
            We have two types of commercial licensing:\n
            Data Service Providers (DSPs) provide access to the source imagery or elevation data.\n
            Value Added Retailers (VARs ) use the imagery and elevation data to create new products and services. This includes advisory services and new knowledge products.
        """

        self.pkg_dict = pkg_dict = {}

        pkg_dict["personal_information"] = "no"
        pkg_dict["protective_marking"] = "official"
        pkg_dict["access"] = "yes"
        pkg_dict["organization_visibility"] = "all"
        pkg_dict["workflow_status"] = "published"
        pkg_dict["title"] = metashare_dict.get("title")
        pkg_dict["notes"] = metashare_dict.get("abstract", "")
        pkg_dict["tags"] = helpers.get_tags(remote_topiccat) if remote_topiccat else []
        pkg_dict["last_updated"] = metashare_dict.get("geonet_info_changedate")
        pkg_dict["extract"] = f"{pkg_dict['notes'].split('.')[0]}..."
        pkg_dict["owner_org"] = self._get_organisation(
            self.config.get("organisation_mapping"),
            metashare_dict.get("resowner", "").split(";")[0],
            harvest_object,
        )

        if not pkg_dict.get("name"):
            pkg_dict["name"] = self._get_package_name(harvest_object, pkg_dict["title"])

        if uuid:
            pkg_dict["primary_purpose_of_collection"] = uuid

        if metashare_dict.get("resowner"):
            pkg_dict["data_owner"] = metashare_dict["resowner"].split(";")[0]

        pkg_dict["groups"] = [
            {"id": group.get("id")} for group in self.config["default_group_dicts"]
        ]

        if pkg_dict["groups"]:
            pkg_dict["category"] = pkg_dict["groups"][0]["id"]

        pkg_dict["date_created_data_asset"] = helpers.convert_date_to_isoformat(
            metashare_dict.get("publicationdate")
            or metashare_dict.get("geonet_info_createdate"),
            "geonet_info_createdate",
            remote_pkg_name,
        )

        pkg_dict["date_modified_data_asset"] = helpers.convert_date_to_isoformat(
            metashare_dict.get("revisiondate")
            or metashare_dict.get("geonet_info_changedate"),
            "geonet_info_changedate",
            remote_pkg_name,
        )

        pkg_dict["update_frequency"] = helpers.map_update_frequency(
            metashare_dict.get("maintenanceandupdatefrequency_text", "unknown"),
        )

        pkg_dict["resources"] = self._fetch_resources(metashare_dict)

        pkg_dict["private"] = self._is_pkg_private(metashare_dict)

        pkg_dict["license_id"] = self.config.get("license_id", "cc-by")

        if pkg_dict["private"]:
            pkg_dict["license_id"] = "other-closed"

        if self._is_delwp_raster_data(pkg_dict["resources"]):
            pkg_dict["full_metadata_url"] = (
                f"https://metashare.maps.vic.gov.au/geonetwork/srv/api/records/{uuid}/formatters/cip-pdf?root=export&output=pdf"
            )
            pkg_dict["access_description"] = access_notes
        elif full_metadata_url:
            pkg_dict["full_metadata_url"] = full_metadata_url

        for key, value in [
            ("harvest_source_id", harvest_object.source.id),
            ("harvest_source_title", harvest_object.source.title),
            ("harvest_source_type", harvest_object.source.type),
            ("delwp_restricted", pkg_dict["private"]),
        ]:
            pkg_dict.setdefault("extras", [])
            pkg_dict["extras"].append({"key": key, "value": value})

        return pkg_dict

    def _create_custom_package_create_schema(self) -> dict[str, Any]:
        from ckan.lib.navl.validators import unicode_safe

        package_schema: dict[str, Any] = default_create_package_schema()  # type: ignore
        package_schema["id"] = [unicode_safe]

        return package_schema

    def _is_delwp_vector_data(self, resources: list[dict[str, Any]]) -> bool:
        for res in resources:
            if res["format"].lower() in [
                "dwg",
                "dxf",
                "gdb",
                "shp",
                "mif",
                "tab",
                "extended tab",
                "mapinfo",
            ]:
                return True

        return False

    def _is_delwp_raster_data(self, resources: list[dict[str, Any]]) -> bool:
        for res in resources:
            if res["format"].lower() in [
                "ecw",
                "geotiff",
                "jpeg",
                "jp2",
                "jpeg 2000",
                "tiff",
                "lass",
                "xyz",
            ]:
                return True

        return False

    def _is_pkg_private(self, remote_dict: dict[str, Any]) -> bool:
        """Private unless access is not restricted and orderable on DataShare (``asbool``).
        Blank fields are treated as private."""
        accesscontrol_restricted = remote_dict.get("accesscontrol_restricted", True)
        orderableondatashare = remote_dict.get("orderableondatashare", False)
        if ((isinstance(accesscontrol_restricted, str) and not accesscontrol_restricted.strip()) or
            (isinstance(orderableondatashare, str) and not orderableondatashare.strip())):
            # one or both are empty string -> treat as private
            return True

        not_restricted = not tk.asbool(accesscontrol_restricted)
        orderable = tk.asbool(orderableondatashare)
        # private=False only when not access-restricted and orderable; any other combination stays private.
        return not (not_restricted and orderable)

    def _get_organisation(
        self,
        organisation_mapping: Optional[list[dict[str, str]]],
        resowner: str,
        harvest_object: HarvestObject,
    ) -> Optional[str]:
        """Get existing organization from the config `organization_mapping`
        field or create a new one"""

        if not resowner:
            log.warning(
                "%s: resowner for harvest object %s is empty, using source organization: %s",
                self.HARVESTER,
                harvest_object.id,
                self.source_org_id,
            )
            return self.source_org_id
        owner_org = None

        if organisation_mapping:
            owner_org: Optional[str] = self._get_existing_organization(
                organisation_mapping, resowner
            )

        return owner_org or self._create_organization(resowner, harvest_object)

    def _get_existing_organization(
        self, organisation_mapping: list[dict[str, str]], resowner: str
    ) -> Optional[str]:
        """Get an organization name either from config mapping or try to find
        an existing one on a portal by `resowner` field"""
        org_name = next(
            (
                organisation.get("org-name")
                for organisation in organisation_mapping
                if organisation.get("resowner") == resowner
            ),
            None,
        )

        if org_name:
            return org_name

        log.warning(
            "%s get_organisation: No mapping found for resowner %s for dataset %s",
            self.HARVESTER,
            resowner,
            self.pkg_dict["title"],
        )
        org_name = helpers.munge_title_to_name(resowner)

        if organization := self._get_organization(org_name):
            return organization.id

        log.warning(
            "%s get_organisation: organisation does not exist: %s, dataset %s",
            self.HARVESTER,
            org_name,
            self.pkg_dict["title"],
        )

    def _create_organization(self, resowner: str, harvest_object: HarvestObject) -> str:
        """Create organization from a resowner field"""
        org_name = helpers.munge_title_to_name(resowner)

        # Use a context without return_id_only so that plugin subscribers
        # (e.g. the activity extension) receive the full org dict rather than
        # a bare ID string which would cause a TypeError in their handlers.
        context = {k: v for k, v in self._make_context().items() if k != "return_id_only"}

        try:
            org = tk.get_action("organization_create")(
                context,
                {"name": org_name, "title": resowner},
            )
            org_id = org["id"] if isinstance(org, dict) else org
        except Exception as e:
            pkg_title = getattr(self, "pkg_dict", {}).get("title", "unknown")
            log.warning(
                "%s get_organisation: Failed to create organisation %s: %s, dataset %s",
                self.HARVESTER,
                org_name,
                e,
                pkg_title,
            )
            log.warning(
                "%s: using source organization: %s", self.HARVESTER, self.source_org_id
            )

            org_id = self.source_org_id

        return org_id

    def _get_package_name(self, harvest_object: HarvestObject, title: str) -> str:
        """Generate package name from title"""
        package = harvest_object.package

        if package is None or package.title != title:
            log.debug("%s: generating new package name for title=%s (object %s)", self.HARVESTER, title, harvest_object.id)
            name = self._gen_new_name(title)

            if not name:
                raise Exception(
                    "Could not generate a unique name from the title or the "
                    "GUID. Please choose a more unique title."
                )
        else:
            name = package.name

        return name

    def _fetch_resources(self, metashare_dict: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch resources data from a metashare_dict"""

        resources: list[dict[str, Any]] = []

        resources.extend(self._get_resources_by_formats(metashare_dict))
        resources.extend(self._get_geoserver_resoures(metashare_dict))

        return resources

    def _get_resources_by_formats(
        self, metashare_dict: dict[str, Any]
    ) -> list[dict[str, Any]]:
        resources: list[dict[str, Any]] = []

        res_url_prefix: Optional[str] = self.config.get("resource_url_prefix")
        res_url: str = (
            f"{res_url_prefix}{metashare_dict['_uuid']}" if res_url_prefix else ""
        )
        attribution = self.config.get("resource_attribution")

        if metashare_dict.get("available_formats") is None:
            return resources

        tempextentbegin = helpers.convert_date_to_isoformat(
            metashare_dict.get("tempextentbegin"),
            "tempextentbegin",
            metashare_dict.get("title"),
        )

        tempextentend = helpers.convert_date_to_isoformat(
            metashare_dict.get("tempextentend"),
            "tempextentend",
            metashare_dict.get("title"),
        )

        for res_format in metashare_dict.get("available_formats", "").split(","):
            res = {
                "name": metashare_dict.get("alttitle") or metashare_dict.get("title"),
                "format": res_format,
                "period_start": tempextentbegin,
                "period_end": tempextentend,
                "url": res_url,
            }

            res["name"] = f"{res['name']} {res_format}".replace("_", "")

            res["size"] = get_resource_size(res_url)
            res["filesize"] = res["size"]

            if attribution:
                res["attribution"] = attribution

            resources.append(res)

        return resources

    def _get_geoserver_resoures(
        self, metashare_dict: dict[str, Any]
    ) -> list[dict[str, Any]]:
        resources: list[dict[str, Any]] = []

        if "geoserver_dns" not in self.config:
            return resources

        for res_fmt in self.geoserver_urls:
            layer_data = self._get_geoserver_content_with_uuid(
                self.geoserver_urls[res_fmt]["geoserver_url"], metashare_dict["_uuid"]
            )

            if not layer_data:
                continue

            layer_title: str = layer_data.find_previous("Title").text.upper()
            layer_name: str = layer_data.find_previous("Name").text
            resource_url: str = self.geoserver_urls[res_fmt]["resource_url"]

            resources.append(
                {
                    "name": f"{layer_title} {res_fmt}",
                    "format": res_fmt,
                    "url": resource_url.format(layername=layer_name),
                    "period_start": helpers.convert_date_to_isoformat(
                        metashare_dict.get("tempextentbegin"),
                        "tempextentbegin",
                        metashare_dict.get("name"),
                    ),
                    "period_end": helpers.convert_date_to_isoformat(
                        metashare_dict.get("tempextentend"),
                        "tempextentend",
                        metashare_dict.get("name"),
                    ),
                }
            )

        return resources

    def _get_geoserver_content_with_uuid(
        self, geoserver_url: str, metadata_uuid: Optional[str]
    ) -> Optional[Tag]:
        resp_text: Optional[str] = (
            self._get_mocked_geores(geoserver_url)
            if self.test
            else self._make_request(geoserver_url)
        )

        if not resp_text:
            return

        return BeautifulSoup(resp_text, "lxml-xml").find(  # type: ignore
            "Keyword", string=f"MetadataID={metadata_uuid}"
        )

    def _get_mocked_records(self) -> str:
        """Mock data, use it instead _make_request for develop process"""
        here: str = path.abspath(path.dirname(__file__))

        mock_file = "delwp_records.json"
        if self.config["dataset_type"] == "uat-datashare-metadata":
            mock_file = "delwp_records_uat.json"

        with open(path.join(here, f"../data/{mock_file}")) as f:
            return f.read()

    def _get_mocked_geores(self, geoserver_url: str) -> str:
        """Mock data, use it instead _make_request for develop process"""
        here: str = path.abspath(path.dirname(__file__))

        mock_file = "delwp_geo_resource_wms.txt"
        if "wfs" in geoserver_url.lower():
            mock_file = "delwp_geo_resource_wfs.txt"

        with open(path.join(here, f"../data/{mock_file}")) as f:
            return f.read()

    def _calculate_hash_for_data_dict(self, pkg_dict: dict[str, Any]) -> str:
        """Calculate a hash for a package_dict to understand if it's changed"""
        json_str = json.dumps(pkg_dict, sort_keys=True)
        return sha256(json_str.encode()).hexdigest()
