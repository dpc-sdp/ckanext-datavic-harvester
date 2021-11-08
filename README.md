# CKAN Datavic Harvester

This is a custom Harvester (https://github.com/ckan/ckanext-harvest) extension for CKAN.

It assumes that the ``ckanext-harvest`` extension is installed and enabled in the CKAN .ini file.

## Installation

To install ``ckanext-datavic-harvester``:

1. Activate your CKAN virtual environment, for example:

        . /app/src/ckan/default/bin/activate

2. Install the ckanext-datavic-harvester Python package into your virtual environment:

        cd /app/src/ckan/default/src/ckanext-datavic-harvester
        python setup.py develop

3. Add ``datavic_ckan_harvester`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with docker:

         docker-compose restart ckan

5. When creating a new harvest source via the standard ``ckanext-harvest`` admin UI, select ``CKAN Harvester for Data.Vic`` as the harvest type.

## Additional Parameters
### ignore_private_datasets

This setting can be used on the Public data.vic harvest from the Data Directory to exclude Private records from being harvested.

Default: False
