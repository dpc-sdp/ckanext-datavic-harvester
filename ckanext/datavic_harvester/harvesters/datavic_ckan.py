import os
import logging

import requests

from ckan import model
from ckan.plugins import toolkit as tk
from ckan.lib.helpers import json
from ckan.lib.search.index import PackageSearchIndex

from ckanext.harvest.harvesters.ckanharvester import CKANHarvester, RemoteResourceError


log = logging.getLogger(__name__)


class DataVicCKANHarvester(CKANHarvester):
    """
    A Harvester for CKAN Data.Vic instances
    """

    def info(self):
        return {
            "name": "datavic_ckan_harvester",
            "title": "CKAN Harvester for Data.Vic",
            "description": "Harvests remote CKAN instances using the Data.Vic custom schema and performs some post-processing",
            "form_config_interface": "Text",
        }

    def import_stage(self, harvest_object):
        log.debug("In DataVicCKANHarvester import_stage")

        base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }
        if not harvest_object:
            log.error("No harvest object received")
            return False

        if harvest_object.content is None:
            self._save_object_error(
                f"Empty content for object {harvest_object.id}",
                harvest_object,
                "Import",
            )
            return False

        self._set_config(harvest_object.job.source.config)

        try:
            package_dict = json.loads(harvest_object.content)

            try:
                local_dataset = tk.get_action("package_show")(
                    base_context.copy(), {"id": package_dict["id"]}
                )
            except (tk.ObjectNotFound) as e:
                local_dataset = {}
                log.info(
                    "-- Package ID %s (%s) does not exist locally",
                    package_dict["id"],
                    package_dict["name"],
                )

            ignore_private = tk.asbool(
                self.config.get("ignore_private_datasets", False)
            )
            # DATAVIC-94 - Even if a dataset is marked Private we need to check if it exists locally in CKAN
            # If it exists then it needs to be removed
            if ignore_private and tk.asbool(package_dict["private"]) is True:
                if local_dataset:
                    if not local_dataset["state"] == "deleted":
                        tk.get_action("package_delete")(
                            base_context.copy(), {"id": local_dataset["id"]}
                        )
                        package_index = PackageSearchIndex()
                        package_index.remove_dict(local_dataset)
                        log.info(
                            "REMOVING now Private record: "
                            + package_dict["name"]
                            + " - ID: "
                            + package_dict["id"]
                        )
                    # Return true regardless of if the local dataset is already deleted, because we need to avoid this
                    # dataset harvest object from being processed any further.
                    return True
                else:
                    log.info(
                        "IGNORING Private record: "
                        + package_dict["name"]
                        + " - ID: "
                        + package_dict["id"]
                    )
                    return True

            if package_dict.get("type") == "harvest":
                log.warn("Remote dataset is a harvest source, ignoring...")
                return True

            # Set default tags if needed
            default_tags = self.config.get("default_tags", [])
            if default_tags:
                if not "tags" in package_dict:
                    package_dict["tags"] = []
                package_dict["tags"].extend(
                    [t for t in default_tags if t not in package_dict["tags"]]
                )

            remote_groups = self.config.get("remote_groups", None)
            if not remote_groups in ("only_local", "create"):
                # Ignore remote groups
                package_dict.pop("groups", None)
            else:
                if not "groups" in package_dict:
                    package_dict["groups"] = []

                # check if remote groups exist locally, otherwise remove
                validated_groups = []

                # Only process the first group that matches an existing group in CKAN
                if len(package_dict["groups"]) > 1:
                    package_group_names = [x["name"] for x in package_dict["groups"]]
                    # Get all the groups in CKAN
                    ckan_groups = tk.get_action("group_list")(base_context.copy(), {})
                    for group_name in package_group_names:
                        if group_name in ckan_groups:
                            package_dict["groups"] = [
                                x
                                for x in package_dict["groups"]
                                if x["name"] == group_name
                            ]
                            break

                for group_ in package_dict["groups"]:
                    try:
                        try:
                            if "id" in group_:
                                data_dict = {"id": group_["id"]}
                                group = tk.get_action("group_show")(
                                    base_context.copy(), data_dict
                                )
                            else:
                                raise tk.ObjectNotFound

                        except tk.ObjectNotFound as e:
                            if "name" in group_:
                                data_dict = {"id": group_["name"]}
                                group = tk.get_action("group_show")(
                                    base_context.copy(), data_dict
                                )
                            else:
                                raise tk.ObjectNotFound
                        # Found local group
                        validated_groups.append(
                            {"id": group["id"], "name": group["name"]}
                        )

                    except tk.ObjectNotFound as e:
                        log.info("Group %s is not available", group_)
                        if remote_groups == "create":
                            try:
                                group = self._get_group(
                                    harvest_object.source.url, group_
                                )
                            except RemoteResourceError:
                                log.error("Could not get remote group %s", group_)
                                continue

                            for key in [
                                "packages",
                                "created",
                                "users",
                                "groups",
                                "tags",
                                "extras",
                                "display_name",
                            ]:
                                group.pop(key, None)

                            tk.get_action("group_create")(base_context.copy(), group)
                            log.info("Group %s has been newly created", group_)
                            validated_groups.append(
                                {"id": group["id"], "name": group["name"]}
                            )

                package_dict["groups"] = validated_groups

            # Local harvest source organization
            source_dataset = tk.get_action("package_show")(
                base_context.copy(), {"id": harvest_object.source.id}
            )
            local_org = source_dataset.get("owner_org")

            remote_orgs = self.config.get("remote_orgs", None)

            if not remote_orgs in ("only_local", "create"):
                # Assign dataset to the source organization
                package_dict["owner_org"] = local_org
            else:
                if not "owner_org" in package_dict:
                    package_dict["owner_org"] = None

                # check if remote org exist locally, otherwise remove
                validated_org = None
                remote_org = package_dict["owner_org"]

                if remote_org:
                    try:
                        data_dict = {"id": remote_org}
                        org = tk.get_action("organization_show")(
                            base_context.copy(), data_dict
                        )
                        validated_org = org["id"]
                    except tk.ObjectNotFound as e:
                        log.info("Organization %s is not available", remote_org)
                        if remote_orgs == "create":
                            try:
                                try:
                                    org = self._get_organization(
                                        harvest_object.source.url, remote_org
                                    )
                                except RemoteResourceError:
                                    # fallback if remote CKAN exposes organizations as groups
                                    # this especially targets older versions of CKAN
                                    org = self._get_group(
                                        harvest_object.source.url, remote_org
                                    )

                                # DATAVIC-8: Try and find a local org with the same name first..
                                try:
                                    matching_local_org = tk.get_action(
                                        "organization_show"
                                    )(base_context.copy(), {"id": org["name"]})
                                    log.info(
                                        "Found local org matching name: " + org["name"]
                                    )
                                    validated_org = matching_local_org["id"]
                                except tk.ObjectNotFound as e:
                                    log.info(
                                        "Did NOT find local org matching name: "
                                        + org["name"]
                                        + " - attempting to create..."
                                    )
                                    for key in [
                                        "packages",
                                        "created",
                                        "users",
                                        "groups",
                                        "tags",
                                        "extras",
                                        "display_name",
                                        "type",
                                    ]:
                                        org.pop(key, None)
                                    tk.get_action("organization_create")(
                                        base_context.copy(), org
                                    )
                                    log.info(
                                        "Organization %s has been newly created",
                                        remote_org,
                                    )
                                    validated_org = org["id"]
                            except (RemoteResourceError, tk.ValidationError):
                                log.error("Could not get remote org %s", remote_org)

                package_dict["owner_org"] = validated_org or local_org

            # Set default groups if needed
            default_groups = self.config.get("default_groups", [])
            if default_groups:
                if not "groups" in package_dict:
                    package_dict["groups"] = []
                existing_group_ids = [g["id"] for g in package_dict["groups"]]
                package_dict["groups"].extend(
                    [
                        g
                        for g in self.config["default_group_dicts"]
                        if g["id"] not in existing_group_ids
                    ]
                )

            # Set default extras if needed
            default_extras = self.config.get("default_extras", {})

            def get_extra(key, package_dict):
                for extra in package_dict.get("extras", []):
                    if extra["key"] == key:
                        return extra

            if default_extras:
                override_extras = self.config.get("override_extras", False)
                if not "extras" in package_dict:
                    package_dict["extras"] = []
                for key, value in default_extras.items():
                    existing_extra = get_extra(key, package_dict)
                    if existing_extra and not override_extras:
                        continue  # no need for the default
                    if existing_extra:
                        package_dict["extras"].remove(existing_extra)
                    # Look for replacement strings
                    if isinstance(value, str):
                        value = value.format(
                            harvest_source_id=harvest_object.job.source.id,
                            harvest_source_url=harvest_object.job.source.url.strip("/"),
                            harvest_source_title=harvest_object.job.source.title,
                            harvest_job_id=harvest_object.job.id,
                            harvest_object_id=harvest_object.id,
                            dataset_id=package_dict["id"],
                        )

                    package_dict["extras"].append({"key": key, "value": value})

            for resource in package_dict.get("resources", []):
                if resource.get("url_type") == "upload":
                    local_resource = next(
                        (
                            x
                            for x in local_dataset.get("resources", [])
                            if resource.get("id") == x.get("id")
                        ),
                        None,
                    )

                    # Check last modified date to see if resource file has been updated
                    # Resource last_modified date is only updated when a file has been uploaded

                    if not local_resource or (
                        local_resource
                        and resource.get("last_modified", None)
                        > local_resource.get("last_modified", None)
                    ):

                        filename = self.copy_remote_file_to_filestore(
                            resource["id"], resource["url"], self.config.get("api_key")
                        )

                        if filename:
                            resource["url"] = filename
                else:
                    # Clear remote url_type for resources (eg datastore, upload) as
                    # we are only creating normal resources with links to the
                    # remote ones
                    resource.pop("url_type", None)

                # Clear revision_id as the revision won't exist on this CKAN
                # and saving it will cause an IntegrityError with the foreign
                # key.
                resource.pop("revision_id", None)

                # Copy `citation` from the dataset to the resource (for Legacy Data.Vic records)
                citation = package_dict.get("citation", None)
                if citation is not None:
                    resource["attribution"] = citation

            # DATAVIC-61: Add any additional schema fields not existing in Data.Vic schema as extras
            # if identified within the harvest configuration
            additional_fields_as_extras = self.config.get(
                "additional_fields_as_extras", {}
            )
            if additional_fields_as_extras:
                for key in additional_fields_as_extras:
                    if package_dict[key]:
                        package_dict["extras"].append(
                            {"key": key, "value": package_dict[key]}
                        )

            # Use the same harvester for the different scenarios, e.g.
            additional_fields = self.config.get("additional_fields", {})

            if additional_fields:
                if not "extras" in package_dict:
                    package_dict["extras"] = []
                for key in additional_fields:
                    if key in package_dict:
                        package_dict["extras"].append(
                            {"key": key, "value": package_dict[key]}
                        )

            result = self._create_or_update_package(
                package_dict, harvest_object, package_dict_form="package_show"
            )

            # DATAVIC: workflow_status and organization_visibility are now set in the ckanext-workflow extension:
            # file: ckanext-workflow/ckanext/workflow/plugin.py
            # function: create()

            return result

        except tk.ValidationError as e:
            self._save_object_error(
                f"Invalid package with GUID {harvest_object.guid}: {e.error_dict}",
                harvest_object,
                "Import",
            )
        except Exception as e:
            self._save_object_error(str(e), harvest_object, "Import")

    def copy_remote_file_to_filestore(self, resource_id, resource_url, apikey=None):
        try:
            (
                _,
                parent_dir,
                sub_dir,
                _,
                full_path,
            ) = self.get_paths_from_resource_id(resource_id)

            # Check to see if the full path, i.e. file already exists - if so delete it
            if os.path.exists(full_path):
                os.remove(full_path)

            resource_dir_exists = self.resource_directory_exists(parent_dir, sub_dir)

            if resource_dir_exists:
                headers = {}
                if apikey:
                    headers["Authorization"] = apikey
                r = requests.get(resource_url, headers=headers)
                open(full_path, "wb").write(r.content)
                log.info(f"Downloaded resource {resource_url} to {full_path}")
            else:
                log.error(f"Directory for local resource {sub_dir} does not exist")

            # Return the actual filename of the remote resource
            return resource_url.split("/")[-1]
        except Exception as e:
            log.error(f"Error copying remote file {resource_url} to local {full_path}")
            log.error(f"Exception: {e}")
            return None

    def resource_directory_exists(self, parent_dir, sub_dir):
        try:
            # Check to see if the sub dir exist - if not create it
            if not os.path.exists(sub_dir):
                # Check to see if the parent dir exists - if not, create it
                if not os.path.exists(parent_dir):
                    self.create_resource_directory(parent_dir)
                self.create_resource_directory(sub_dir)
            return True
        except Exception as e:
            log.error(f"`resource_directory_exists` Exception: {e}")
        return False

    def create_resource_directory(self, directory):
        try:
            os.mkdir(directory)
            return True
        except Exception as e:
            log.error(
                f"`create_resource_directory` Error creating directory: {directory}"
            )
            log.error(f"`create_resource_directory` Exception: {e}")
        return False

    def get_paths_from_resource_id(self, resource_id):
        # Our base path for storing resource files
        resources_path = "/".join([tk.config.get("ckan.storage_path"), "resources"])

        # Separate the resource ID into the necessary chunks for filestore resource directory structure
        parent_dir = "/".join([resources_path, resource_id[0:3]])
        sub_dir = "/".join([parent_dir, resource_id[3:6]])
        filename = resource_id[6:]

        full_path = "/".join([sub_dir, filename])

        return resources_path, parent_dir, sub_dir, filename, full_path
