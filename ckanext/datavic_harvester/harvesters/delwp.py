from __future__ import annotations

import json
import logging
import traceback
import uuid
from os import path
from typing import Iterator, Optional, Any, Union

from bs4 import BeautifulSoup, Tag, NavigableString

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.logic.schema import default_create_package_schema

from ckanext.harvest.model import HarvestObject, HarvestObjectExtra

import ckanext.datavic_harvester.helpers as helpers
from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester


log = logging.getLogger(__name__)


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
            return json.dumps(config_obj, indent=4)

        self._validate_organisation_mapping(config_obj)

        return json.dumps(config_obj, indent=4)

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

            try:
                tk.get_action("organization_show")(
                    self._make_context(), {"id": org_name}
                )
            except tk.ObjectNotFound:
                raise ValueError(f"Organisation {org_name} not found")

    def gather_stage(self, harvest_job):
        log.debug(f"In {self.HARVESTER} gather_stage")
        self._set_config(harvest_job.source.config)

        ids = []
        guid_to_package_id: dict[str, str] = self._get_guids_to_package_ids(
            harvest_job.source.id
        )
        guids_in_db: list[str] = list(guid_to_package_id.keys())
        guids_in_source: list[str] = []
        previous_guids: list[str] = []

        page: int = 1
        records_per_page: int = 500
        harvest_source_url: str = harvest_job.source.url.rstrip("?")

        while True:
            log.debug(
                f"{self.HARVESTER} fetching records by url: {harvest_source_url}, page: {page}"
            )
            records = self._fetch_records(harvest_source_url, page, records_per_page)

            batch_guids = []

            if not records:
                log.debug(f"{self.HARVESTER} empty document, no more records")
                break

            for record_metadata in self._get_record_metadata(records):
                guid: str = record_metadata["uuid"]
                batch_guids.append(guid)

                if guid in previous_guids:
                    continue

                if guid in guids_in_db:
                    # Dataset needs to be updated
                    obj = HarvestObject(
                        guid=guid,
                        job=harvest_job,
                        package_id=guid_to_package_id[guid],
                        content=json.dumps(record_metadata),
                        extras=[HarvestObjectExtra(key="status", value="change")],
                    )
                else:
                    # Dataset needs to be created
                    obj = HarvestObject(
                        guid=guid,
                        job=harvest_job,
                        content=json.dumps(record_metadata),
                        extras=[HarvestObjectExtra(key="status", value="new")],
                    )

                obj.save()
                ids.append(obj.id)

            if len(batch_guids) > 0:
                guids_in_source.extend(set(batch_guids) - set(previous_guids))

            if self.test:
                break

            page = page + 1
            previous_guids = batch_guids

        # Check datasets that need to be deleted
        guids_to_delete = set(guids_in_db) - set(guids_in_source)
        for guid in guids_to_delete:
            obj = HarvestObject(
                guid=guid,
                job=harvest_job,
                package_id=guid_to_package_id[guid],
                extras=[HarvestObjectExtra(key="status", value="delete")],
            )
            ids.append(obj.id)
            model.Session.query(HarvestObject).filter_by(guid=guid).update(
                {"current": False}, False
            )
            obj.save()

        return ids

    def _get_guids_to_package_ids(self, source_id: str) -> dict[str, str]:
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(HarvestObject.current == True)
            .filter(HarvestObject.harvest_source_id == source_id)
        )

        return {
            harvest_object.guid: harvest_object.package_id for harvest_object in query
        }

    def _fetch_records(
        self, url: str, page: int, records_per_page: int = 100
    ) -> Optional[list[dict[str, Any]]]:

        _from, _to = helpers.get_from_to(page, records_per_page)

        request_url: str = "{}?dataset={}&start={}&rows={}&format=json".format(
            url, self.config["dataset_type"], _from, _to
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
            return

        data = json.loads(resp_text)
        return data.get("records")

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

    def import_stage(self, harvest_object: HarvestObject) -> bool:
        log.debug(f"{self.HARVESTER}: starting import stage")

        if not harvest_object:
            log.error(f"{self.HARVESTER}: no harvest object received")
            return False

        status = self._get_object_extra(harvest_object, "status")

        if status == "delete":
            self._delete_package(harvest_object.package_id, harvest_object.guid)
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

        self._set_config(harvest_object.source.config)

        previous_harvest_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .filter(HarvestObject.current == True)
            .first()
        )

        if previous_harvest_object:
            previous_harvest_object.current = False
        harvest_object.current = True

        pkg_dict = self._get_pkg_dict(harvest_object)

        if status not in ["new", "change"]:
            return True

        if status == "new":
            pkg_dict["id"] = str(uuid.uuid4())
            harvest_object.package_id = pkg_dict["id"]

            model.Session.execute(
                "SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED"
            )
            model.Session.flush()

        elif status == "change":
            pkg_dict["id"] = harvest_object.package_id

        action: str = "package_create" if status == "new" else "package_update"
        status: str = "Created" if status == "new" else "Updated"

        context = self._make_context()
        context["schema"] = self._create_custom_package_create_schema()

        try:
            package_id = tk.get_action(action)(context, pkg_dict)
            log.info("%s: %s dataset with id %s", self.HARVESTER, status, package_id)
        except Exception as e:
            log.error(f"{self.HARVESTER}: error creating dataset: {e}")
            self._save_object_error(
                f"Error importing dataset {pkg_dict.get('name', '')}: {e} / {traceback.format_exc()}",
                harvest_object,
                "Import",
            )
            return False
        finally:
            model.Session.commit()

        return True

    def _get_pkg_dict(self, harvest_object):
        """Create a pkg_dict from remote portal data"""
        content = harvest_object.content
        uuid = harvest_object.guid

        metashare_dict = json.loads(content)
        metashare_dict["_uuid"] = uuid

        remote_pkg_name: Optional[str] = metashare_dict.get("name")

        full_metadata_url = (
            self.config["full_metadata_url_prefix"].format(**{"UUID": uuid})
            if self.config.get("full_metadata_url_prefix")
            else ""
        )

        pkg_dict = {}

        pkg_dict["personal_information"] = "no"
        pkg_dict["protective_marking"] = "official"
        pkg_dict["access"] = "yes"
        pkg_dict["organization_visibility"] = "all"
        pkg_dict["workflow_status"] = "published"
        pkg_dict["license_id"] = self.config.get("license_id", "cc-by")

        pkg_dict["title"] = metashare_dict.get("title")
        pkg_dict["notes"] = metashare_dict.get("abstract", "")
        pkg_dict["tags"] = helpers.get_tags(metashare_dict.get("topiccat"))
        pkg_dict["last_updated"] = metashare_dict.get("geonet_info_changedate")
        pkg_dict["extract"] = f"{pkg_dict['notes'].split('.')[0]}..."
        pkg_dict["owner_org"] = self._get_organisation(
            self.config.get("organisation_mapping"),
            metashare_dict.get("resowner").split(";")[0],
            harvest_object,
        )

        if not pkg_dict.get("name"):
            pkg_dict["name"] = self._get_package_name(harvest_object, pkg_dict["title"])

        if full_metadata_url:
            pkg_dict["full_metadata_url"] = full_metadata_url

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

        return pkg_dict

    def _create_custom_package_create_schema(self) -> dict[str, Any]:
        package_schema: dict[str, Any] = default_create_package_schema()  # type: ignore
        package_schema["id"] = [str]

        return package_schema

    def _get_organisation(
        self,
        organisation_mapping: Optional[list[dict[str, str]]],
        resowner: str,
        harvest_object: HarvestObject,
    ) -> Optional[str]:
        """Get existing organization from the config `organization_mapping`
        field or create a new one"""

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
            f"{self.HARVESTER} get_organisation: No mapping found for resowner {resowner}"
        )
        org_name = helpers.munge_title_to_name(resowner)

        try:
            organisation = tk.get_action("organization_show")(
                self._make_context(),
                {
                    "id": org_name,
                    "include_dataset_count": False,
                    "include_extras": False,
                    "include_users": False,
                    "include_groups": False,
                    "include_tags": False,
                    "include_followers": False,
                },
            )
        except tk.ObjectNotFound:
            log.warning(
                f"{self.HARVESTER} get_organisation: organisation does not exist: {org_name}"
            )
        else:
            return organisation.get("id")

    def _create_organization(self, resowner: str, harvest_object: HarvestObject) -> str:
        """Create organization from a resowner field"""
        org_name = helpers.munge_title_to_name(resowner)

        try:
            org_id = tk.get_action("organization_create")(
                self._make_context(),
                {"name": org_name, "title": resowner},
            )
        except Exception as e:
            log.warning(
                f"{self.HARVESTER} get_organisation: Failed to create organisation {org_name}"
            )

            source_dict: dict[str, Any] = tk.get_action("package_show")(
                self._make_context(), {"id": harvest_object.harvest_source_id}
            )
            org_id = source_dict["owner_org"]

        return org_id

    def _get_package_name(self, harvest_object: HarvestObject, title: str) -> str:
        """Generate package name from title"""
        package: model.Package = harvest_object.package

        if package is None or package.title != title:
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

        pkg_name: Optional[str] = metashare_dict.get("name")
        formats: Optional[str] = metashare_dict.get("available_formats")

        if not formats:
            return resources

        for res_format in formats.split(","):
            res = {
                "name": metashare_dict.get("alttitle") or metashare_dict.get("title"),
                "format": res_format,
                "period_start": helpers.convert_date_to_isoformat(
                    metashare_dict.get("tempextentbegin"),
                    "tempextentbegin",
                    pkg_name,
                ),
                "period_end": helpers.convert_date_to_isoformat(
                    metashare_dict.get("tempextentend"),
                    "tempextentend",
                    pkg_name,
                ),
                "url": res_url,
            }

            res["name"] = f"{res['name']} {res_format}".replace("_", "")

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

        geoserver_dns = self.config["geoserver_dns"]
        metadata_uuid: Optional[str] = metashare_dict["_uuid"]

        dict_geoserver_urls = {
            "WMS": {
                "geoserver_url": f"{geoserver_dns}/geoserver/ows?service=WMS&request=getCapabilities",
                "resource_url": f"{geoserver_dns}/geoserver/wms?service=wms&request=getmap&format=image%2Fpng8&transparent=true&layers={{layername}}&width=512&height=512&crs=epsg%3A3857&bbox=16114148.554967716%2C-4456584.4971389165%2C16119040.524777967%2C-4451692.527328665",
            },
            "WFS": {
                "geoserver_url": f"{geoserver_dns}/geoserver/ows?service=WFS&request=getCapabilities",
                "resource_url": f"{geoserver_dns}/geoserver/wfs?request=GetCapabilities&service=WFS",
            },
        }

        for res_fmt in dict_geoserver_urls:
            layer_data = self._get_geoserver_content_with_uuid(
                dict_geoserver_urls[res_fmt]["geoserver_url"], metadata_uuid
            )

            if not layer_data:
                continue

            layer_title: str = layer_data.find_previous("Title").text.upper()
            layer_name: str = layer_data.find_previous("Name").text
            resource_url: str = dict_geoserver_urls[res_fmt]["resource_url"]

            resources.append(
                {
                    "name": f"{layer_title} {res_fmt}",
                    "format": res_fmt,
                    "url": resource_url.format(layername=layer_name),
                }
            )

        return resources

    def _get_geoserver_content_with_uuid(
        self, geoserver_url: str, metadata_uuid: Optional[str]
    ) -> Optional[Union[Tag, NavigableString]]:

        resp_text: Optional[str] = (
            self._get_mocked_geores()
            if self.test
            else self._make_request(geoserver_url)
        )

        if not resp_text:
            return

        geoserver_data: BeautifulSoup = BeautifulSoup(resp_text, "lxml-xml")
        return geoserver_data.find("Keyword", string=f"MetadataID={metadata_uuid}")

    def _get_mocked_records(self) -> str:
        """Mock data, use it instead _make_request for develop process"""
        here: str = path.abspath(path.dirname(__file__))
        with open(path.join(here, "../data/delwp_records.txt")) as f:
            return f.read()

    def _get_mocked_geores(self) -> str:
        """Mock data, use it instead _make_request for develop process"""
        here: str = path.abspath(path.dirname(__file__))
        with open(path.join(here, "../data/delwp_geo_resource.txt")) as f:
            return f.read()
