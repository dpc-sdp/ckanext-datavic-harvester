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

3. Add ``datavic_dcat_json_harvester delwp_harvester`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/app/ckan/default/ckan.ini``).

4. Restart CKAN. For example if you've deployed CKAN with docker:

         docker-compose restart ckan

5. When creating a new harvest source via the standard ``ckanext-harvest`` admin UI, select ``CKAN Harvester for Data.Vic`` as the harvest type.

## Running tests

Tests run inside the CKAN Docker container against a dedicated test database.
Test configuration is in ``pytest.ini`` and ``test.ini``. Tests use a separate
``ckan_test`` postgres database and the ``solr``/``redis`` services already
running in the stack.

Start the stack (if not already running):

        ahoy up

Create the test database (one-time setup; the main ``ckan`` DB is not used for tests):

        ahoy ckan
        psql postgresql://ckan:ckan@postgres/postgres -c "CREATE DATABASE ckan_test OWNER ckan;"

Run all tests (inside the CKAN container; `ahoy ckan` activates the virtualenv for you):

        ahoy ckan
        cd /app/src/ckanext-datavic-harvester && pytest ckanext/datavic_harvester/tests/

Run a specific test file:

        ahoy ckan
        cd /app/src/ckanext-datavic-harvester && pytest ckanext/datavic_harvester/tests/harvesters/delwp/test_delwp.py

Run a single test by name:

        ahoy ckan
        cd /app/src/ckanext-datavic-harvester && pytest ckanext/datavic_harvester/tests/ -k test_import_stage

Stop on first failure with verbose output:

        ahoy ckan
        cd /app/src/ckanext-datavic-harvester && pytest ckanext/datavic_harvester/tests/ -x -v

## Additional Parameters

### ignore_private_datasets

This setting can be used on the Public data.vic harvest from the Data Directory to exclude Private records from being harvested.

Default: False
