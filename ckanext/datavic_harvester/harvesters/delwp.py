from __future__ import annotations

import json
import logging
import traceback
import uuid
from hashlib import sha256
from os import path
from typing import Iterator, Optional, Any

from bs4 import BeautifulSoup, Tag

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.logic.schema import default_create_package_schema

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

            if not self._get_organization(org_name):
                raise ValueError(f"Organisation {org_name} not found")

    def _get_organization(self, org_name: str) -> model.Group | None:
        return (
            model.Session.query(model.Group)
            .filter_by(name=org_name, is_organization=True)
            .first()
        )

    def gather_stage(self, harvest_job):
        log.debug(f"In {self.HARVESTER} gather_stage")

        self._set_config(harvest_job)

        harvest_object_ids = []
        guid_to_package_id = self._get_guids_to_package_ids(harvest_job.source.id)
        guids_in_db = list(guid_to_package_id.keys())
        guids_in_source: list[str] = []

        for record in self._fetch_records_from_remote_portal(
            harvest_job.source.url.rstrip("?")
        ):
            uuid = record["fields"]["uuid"]

            # Create harvest object with appropriate status based on if dataset
            # already exists in the database
            obj = HarvestObject(
                guid=uuid,
                job=harvest_job,
                content=json.dumps(record["fields"]),
                extras=[
                    HarvestObjectExtra(
                        key="status", value="change" if uuid in guids_in_db else "new"
                    )
                ],
            )

            if uuid in guids_in_db:
                obj.package_id = guid_to_package_id[uuid]  # type: ignore

            obj.save()

            harvest_object_ids.append(obj.id)
            guids_in_source.append(uuid)

        # Check datasets that are in the database but not in the source
        # therefore they need to be deleted
        for guid in set(guids_in_db) - set(guids_in_source):
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

        return harvest_object_ids

    def _set_config(self, harvest_job: HarvestJob) -> None:
        super()._set_config(harvest_job.source.config)

        self.test = bool(self.config.get("test"))
        self.source_org_id = self._get_source_owner_org_id(harvest_job.source.id)

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
                log.debug(f"{self.HARVESTER} empty document, no more records")
                break

            records.extend(result)

            if self.test:
                break

            page = page + 1

        return records

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

        status = self._get_object_extra(harvest_object, "status") # type: ignore

        if status == "delete":
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

        pkg_dict = self._get_pkg_dict(harvest_object)

        if not pkg_dict["notes"] or not pkg_dict["owner_org"]:
            msg = f"Description or organization field is missing for object {harvest_object.id}, skipping..."
            log.info(msg)
            self._save_object_error(msg, harvest_object, "Import")
            return False

        # Remove restricted Datasets
        if pkg_dict["private"]:
            msg = f"Dataset is Restricted for object {harvest_object.id}, skipping..."
            log.info(msg)
            self._save_object_error(msg, harvest_object, "Import")
            return False

        if status not in ["new", "change"]:
            return True

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
                status = "new"
            else:
                previous_hash = pkg.extras.get(HASH_FIELD)

                if previous_hash == data_hash:
                    log.info(
                        f"No changes to dataset with ID {harvest_object.package_id}, skipping..."
                    )
                    return "unchanged"
                else:
                    log.info(
                        f"Dataset {harvest_object.package_id} is being changed, updating."
                    )
                    pkg_dict[HASH_FIELD] = data_hash

        action: str = "package_create" if status == "new" else "package_update"
        status: str = "Created" if status == "new" else "Updated"

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

        pkg_dict = {}

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

        pkg_dict["private"] = self._is_pkg_private(
            metashare_dict, pkg_dict["resources"]
        )

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

    def _is_pkg_private(
        self, remote_dict: dict[str, Any], resources: list[dict[str, Any]]
    ) -> bool:
        """Check if the dataset should be private"""
        if (
            self._is_delwp_vector_data(resources)
            and remote_dict.get("mdclassification") == "unclassified"
            and remote_dict.get("resclassification") == "unclassified"
        ):
            return False

        return True

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
            f"{self.HARVESTER} get_organisation: No mapping found for resowner {resowner}"
        )
        org_name = helpers.munge_title_to_name(resowner)

        if organization := self._get_organization(org_name):
            return organization.id

        log.warning(
            f"{self.HARVESTER} get_organisation: organisation does not exist: {org_name}"
        )

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
                f"{self.HARVESTER} get_organisation: Failed to create organisation {org_name}: {e}"
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
            self._get_mocked_geores()
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
        with open(path.join(here, "../data/delwp_records.txt")) as f:
            return f.read()

    def _get_mocked_geores(self) -> str:
        """Mock data, use it instead _make_request for develop process"""
        here: str = path.abspath(path.dirname(__file__))
        with open(path.join(here, "../data/delwp_geo_resource.txt")) as f:
            return f.read()

    def _calculate_hash_for_data_dict(self, pkg_dict: dict[str, Any]) -> str:
        """Calculate a hash for a package_dict to understand if it's changed"""
        json_str = json.dumps(pkg_dict, sort_keys=True)
        return sha256(json_str.encode()).hexdigest()
