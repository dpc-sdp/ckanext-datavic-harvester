# CKAN Datavic Harvester

This is a custom Harvester (https://github.com/ckan/ckanext-harvest) extension for CKAN.

It assumes that the ``ckanext-harvest`` extension is installed and enabled in the CKAN .ini file.

## Installation

To install ``ckanext-datavic-harvester``:

1. Activate your CKAN virtual environment, for example:

        . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-datavic-harvester Python package into your virtual environment:

        cd /usr/lib/ckan/default/src/ckanext-datavic-harvester
        python setup.py develop

3. Add ``datavic_ckan_harvester`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

         sudo service apache2 reload

5. When creating a new harvest source via the standard ``ckanext-harvest`` admin UI, select ``CKAN Harvester for Data.Vic`` as the harvest type.

## Additional Parameters

This extension adds a new parameter to the harvest configuration options:

        additional_fields
        additional_fields_as_extras
        ignore_workflow_status
        exclude_sdm_records

### additional_fields

This setting can be used to spell out top level fields in the harvest source that should be copied to the harvested dataset in matching top fields as defined in the custom schema, e.g.

        "additional_fields": ["update_frequency", "extract"]

...will scan the harvested source dataset for `update_frequency` and `extract` and if they exist they will be added to the destination dataset.

### additional_fields_as_extras

This setting can be used to spell out top level fields in the harvest source that should be added to the harvested dataset as additional extras, e.g.

        "additional_fields_as_extras": ["anzlic_id", "package_scope"]

...will scan the harvested source dataset for `anzlic_id` and `package_scope` and if they exist they will be added to the destination dataset as extras.

### ignore_workflow_status

This setting can be used on the ODP harvester to ignore any post-harvest processing required to set the respective `workflow_status` and `organization_visibility` fields for the datasets.

Default: False

### exclude_sdm_records

This setting can be used on the initial harvest from Data.Vic to exclude SDM records from being harvested, as they will eventually be harvested directly from the SDM CKAN instance.

Default: False

### ignore_private_datasets

This setting can be used on the Public data.vic harvest from the Data Directory to exclude Private records from being harvested.

Default: False
