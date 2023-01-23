from __future__ import annotations

import json
import logging
import traceback
import uuid
from typing import Optional, Any

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.logic.schema import default_create_package_schema

from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.datavicmain import helpers

from ckanext.datavic_harvester.harvesters.base import DataVicBaseHarvester
from ckanext.datavic_harvester.helpers import convert_date_to_isoformat, get_from_to


log = logging.getLogger(__name__)


class MetaShareHarvester(DataVicBaseHarvester):
    def info(self):
        return {
            "name": "metashare",
            "title": "MetaShare Harvester",
            "description": "Harvester for MetaShare dataset descriptions serialized as JSON",
        }

    def _get_page_of_records(
        self, url: str, page: int, datasets_per_page: int = 100
    ) -> Optional[list[dict[str, Any]]]:
        _from, _to = get_from_to(page, datasets_per_page)
        records = None

        request_url = f"{url}?from={_from}&to={_to}&_content_type=json&fast=index"
        log.debug(f"Getting page of records {request_url}")

        resp_text: Optional[str] = self._make_request(
            request_url, {"Authorization": api_auth}
        )

        if not resp_text:
            return records

        if r.status_code == 200:
            data = json.loads(r.text)

            # Records are contained in the "metadata" element of the response JSON
            # see example: https://dev-metashare.maps.vic.gov.au/geonetwork/srv/en/q?from=1&to=1&_content_type=json&fast=index
            records = data.get("metadata")

        return records

    def _get_guids_and_datasets(self, datasets):
        """
        Copied & adapted from ckanext/dcat/harvesters/_json.py
        - don't json.loads the `datasets` input - it already be a list of dicts
        - get `uuid` from `geonet:info` property
        :param content:
        :return:
        """

        if not isinstance(datasets, list):
            if isinstance(datasets, dict):
                datasets = [datasets]
            else:
                log.debug(f"Datasets data is not a list: {type(datasets)}")
                raise ValueError("Wrong JSON object")

        for dataset in datasets:

            as_string = json.dumps(dataset)

            # Get identifier
            geonet_info = dataset.get("geonet:info")
            guid = geonet_info.get("uuid")

            yield guid, as_string

    def _get_package_dict(self, harvest_object):
        """
        Convert the string based content from the harvest_object
        into a package_dict for a CKAN dataset
        :param harvest_object:
        :return:
        """
        content = harvest_object.content

        metashare_dict = json.loads(content)

        uuid = harvest_object.guid

        full_metadata_url_prefix = self.config.get("full_metadata_url_prefix")
        full_metadata_url = (
            full_metadata_url_prefix.format(**{"UUID": uuid})
            if full_metadata_url_prefix
            else ""
        )
        resource_url_prefix = self.config.get("resource_url_prefix")
        resource_url = f"{resource_url_prefix}{uuid}" if resource_url_prefix else ""

        package_dict = {}

        # Mandatory fields where no value exists in MetaShare
        # So we set them to Data.Vic defaults
        package_dict["personal_information"] = "no"
        package_dict["protective_marking"] = "official"
        package_dict["access"] = "yes"

        package_dict["title"] = metashare_dict.get("title")

        # 'xmlcharrefreplace'	- replaces the character with an xml character
        package_dict["notes"] = metashare_dict.get("abstract", "").encode(
            "ascii", "xmlcharrefreplace"
        )

        # Get organisation from the harvest source organisation dropdown
        source_dict = tk.get_action("package_show")(
            {}, {"id": harvest_object.harvest_source_id}
        )
        package_dict["owner_org"] = source_dict.get("owner_org")

        # Default as discussed with SDM
        package_dict["license_id"] = self.config.get("license_id", "cc-by")

        # Tags / Keywords
        # `topicCat` can either be a single tag as a string or a list of tags
        topic_cat = metashare_dict.get("topicCat")
        if topic_cat:
            package_dict["tags"] = get_tags(topic_cat)

        package_dict["extract"] = f"{package_dict['notes'].split(b'.')[0]}..."

        # There is no field in Data.Vic schema to store the source UUID of the harvested record
        # Therefore, we are using the `primary_purpose_of_collection` field
        if uuid:
            package_dict["primary_purpose_of_collection"] = uuid

        # @TODO: Consider this - in the field mapping spreadsheet:
        # https://docs.google.com/spreadsheets/d/112hzp6ZrTnp3fl_ZdmT6oHldUGf36LvEpLswAJDLdr0/edit#gid=1669999637
        # The response from SDM was:
        #       "We could either add Custodian to the Q Search results or just use resource owner. Given that it is
        #       not publically displayed in DV, not sure it's worth the effort of adding the custodian"
        res_owner = metashare_dict.get("resOwner")
        if res_owner:
            package_dict["data_owner"] = res_owner.split(";")[0]

        # Decision from discussion with Simon/DPC on 2020-10-13 is to assign all datasets to "Spatial Data" group
        # Data.Vic "category" field is equivalent to groups, but stored as an extra and only has 1 group
        default_group_dicts = self.config.get("default_group_dicts")
        if default_group_dicts and isinstance(default_group_dicts, list):
            package_dict["groups"] = [
                {"id": group.get("id")} for group in default_group_dicts
            ]
            category = default_group_dicts[0] if default_group_dicts else None
            if category:
                package_dict["category"] = category.get("id")

        package_dict["date_created_data_asset"] = convert_date_to_isoformat(
            metashare_dict.get("tempExtentBegin"),
            "tempExtentBegin",
            package_dict["title"],
        )

        package_dict["date_modified_data_asset"] = convert_date_to_isoformat(
            metashare_dict.get("revisionDate"),
            "revisionDate",
            package_dict["title"],
        )

        package_dict["update_frequency"] = map_update_frequency(
            get_datavic_update_frequencies(),
            metashare_dict.get("maintenanceAndUpdateFrequency_text", "unknown"),
        )

        if full_metadata_url:
            package_dict["full_metadata_url"] = full_metadata_url

        # Create a single resource for the dataset
        resource = {
            "name": metashare_dict.get("altTitle") or metashare_dict.get("title"),
            "format": metashare_dict.get("spatialRepresentationType_text"),
            "period_start": convert_date_to_isoformat(
                metashare_dict.get("tempExtentBegin"),
                "tempExtentBegin",
                metashare_dict.get("altTitle") or metashare_dict.get("title"),
            ),
            "period_end": convert_date_to_isoformat(
                metashare_dict.get("tempExtentEnd"),
                "tempExtentEnd",
                metashare_dict.get("altTitle") or metashare_dict.get("title"),
            ),
            "url": resource_url,
        }

        attribution = self.config.get("resource_attribution")
        if attribution:
            resource["attribution"] = attribution

        package_dict["resources"] = [resource]

        # @TODO: What about these ones?
        # responsibleParty

        # Add all the `extras` to our compiled dict

        return package_dict

    def gather_stage(self, harvest_job):
        log.debug("In MetaShareHarvester gather_stage")

        ids = []

        # Get the previous guids for this source
        query = (
            model.Session.query(HarvestObject.guid, HarvestObject.package_id)
            .filter(HarvestObject.current == True)
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)
        )

        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = list(guid_to_package_id.keys())

        guids_in_source = []
        #
        # END: This section is copied from ckanext/dcat/harvesters/_json.py
        #

        previous_guids = []
        page = 1
        # CKAN harvest default is 100, in testing 500 works pretty fast and is more efficient as it only needs 5 API calls instead of 19 for 1701 test datasets
        records_per_page = 500

        harvest_source_url = (
            harvest_job.source.url[:-1]
            if harvest_job.source.url.endswith("?")
            else harvest_job.source.url
        )

        # _get_page_of_records will return None if there are no more records
        records = True
        while records:
            records = self._get_page_of_records(
                harvest_source_url, page, records_per_page
            )

            batch_guids = []
            if records:
                #
                # BEGIN: This section is copied from ckanext/dcat/harvesters/_json.py
                #
                for guid, as_string in self._get_guids_and_datasets(records):
                    batch_guids.append(guid)

                    if guid not in previous_guids:

                        if guid in guids_in_db:
                            # Dataset needs to be udpated
                            obj = HarvestObject(
                                guid=guid,
                                job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                content=as_string,
                                extras=[
                                    HarvestObjectExtra(key="status", value="change")
                                ],
                            )
                        else:
                            # Dataset needs to be created
                            obj = HarvestObject(
                                guid=guid,
                                job=harvest_job,
                                content=as_string,
                                extras=[HarvestObjectExtra(key="status", value="new")],
                            )
                        obj.save()
                        ids.append(obj.id)

                if len(batch_guids) > 0:
                    guids_in_source.extend(set(batch_guids) - set(previous_guids))
                else:
                    log.debug("Empty document, no more records")
                    # Empty document, no more ids
                    break
                #
                # END: This section is copied from ckanext/dcat/harvesters/_json.py
                #

            #
            # BEGIN: This section is copied from ckanext/dcat/harvesters/_json.py
            #
            page = page + 1
            previous_guids = batch_guids
            #
            # END: This section is copied from ckanext/dcat/harvesters/_json.py
            #

        #
        # BEGIN: This section is copied from ckanext/dcat/harvesters/_json.py
        # @TODO: Can probably be moved into its own function
        #
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
        #
        # END: This section is copied from ckanext/dcat/harvesters/_json.py
        #

    def import_stage(self, harvest_object):
        """
        Mostly copied from `ckanext/dcat/harvesters/_json.py`
        :param harvest_object:
        :return:
        """
        log.debug("In MetaShareHarvester import_stage")

        if not harvest_object:
            log.error("No harvest object received")
            return False

        status = self._get_object_extra(harvest_object, "status")

        context = {
            "user": self._get_user_name(),
            "return_id_only": True,
            "ignore_auth": True,
            "model": model,
            "session": model.Session,
        }

        if status == "delete":
            # Delete package

            tk.get_action("package_delete")(context, {"id": harvest_object.package_id})
            log.info(
                f"Deleted package {harvest_object.package_id} with guid {harvest_object.guid}"
            )

            return True

        if harvest_object.content is None:
            self._save_object_error(
                f"Empty content for object {harvest_object.id}",
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

        # Get the last harvested object (if any)
        previous_object = (
            model.Session.query(HarvestObject)
            .filter(HarvestObject.guid == harvest_object.guid)
            .filter(HarvestObject.current == True)
            .first()
        )

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        package_dict = self._get_package_dict(harvest_object)

        if not package_dict:
            return False

        if not package_dict.get("name"):
            package_dict["name"] = self._get_package_name(
                harvest_object, package_dict["title"]
            )

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        try:
            if status == "new":
                package_schema = default_create_package_schema()
                context["schema"] = package_schema

                # We need to explicitly provide a package ID
                package_dict["id"] = str(uuid.uuid4())
                package_schema["id"] = [str]

                # Save reference to the package on the object
                harvest_object.package_id = package_dict["id"]
                harvest_object.add()

                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                model.Session.execute(
                    "SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED"
                )
                model.Session.flush()

            elif status == "change":
                package_dict["id"] = harvest_object.package_id

            if status in ["new", "change"]:
                action = "package_create" if status == "new" else "package_update"
                message_status = "Created" if status == "new" else "Updated"

                package_id = tk.get_action(action)(context, package_dict)
                log.info("%s dataset with id %s", message_status, package_id)

        except Exception as e:
            # dataset = json.loads(harvest_object.content)
            dataset_name = package_dict.get("name", "")

            self._save_object_error(
                f"Error importing dataset {dataset_name}: {e} / {traceback.format_exc()}",
                harvest_object,
                "Import",
            )
            return False

        finally:
            model.Session.commit()

        return True


def get_tags(value):
    tags = []
    if isinstance(value, list):
        for tag in value:
            tags.append({"name": tag})
    else:
        tags.append({"name": value})

    return tags


def get_datavic_update_frequencies():
    return helpers.field_choices("update_frequency")


def map_update_frequency(datavic_update_frequencies, value):
    # Check if the value from SDM matches one of those, if so just return original value
    for frequency in datavic_update_frequencies:
        if frequency["label"].lower() == value.lower():
            return frequency["value"]

    # Otherwise return the default of 'unknown'
    return "unknown"
