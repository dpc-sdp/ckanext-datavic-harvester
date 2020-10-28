from datetime import datetime
import json
import logging
import requests
import traceback
import uuid

from ckan import logic
from ckan import model
from ckan import plugins as p
from ckanext.datavicmain.schema import DATASET_EXTRA_FIELDS
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from hashlib import sha1


log = logging.getLogger(__name__)


def _get_from_to(page, datasets_per_page):
    # if ... else expanded for readability
    if page == 1:
        _from = 1
        _to = page * datasets_per_page
    else:
        _from = ((page - 1) * datasets_per_page) + 1
        _to = (page * datasets_per_page)

    return _from, _to


def get_tags(value):
    tags = []
    if isinstance(value, list):
        for tag in value:
            tags.append({
                'name': tag
            })
    else:
        tags.append({
            'name': value
        })

    return tags


def process_date(value):
    """
    Example dates:
        '2020-10-13t05:00:11'
        u'2006-12-31t13:00:00.000z'
    :param value:
    :return:
    """
    # Remove any microseconds
    value = value.split('.')[0]
    if 't' in value:
        return datetime.strptime(value, "%Y-%m-%dt%H:%M:%S").isoformat()


def get_datavic_update_frequencies():
    # DATASET_EXTRA_FIELDS is a list of tuples - where the first element
    # is the name of the metadata schema field
    # The second element contains the configuration for the field
    update_frequency_options = [x[1]['options'] for x in DATASET_EXTRA_FIELDS if x[0] == 'update_frequency']

    return update_frequency_options[0]


def map_update_frequency(datavic_update_frequencies, value):
    # Check if the value from SDM matches one of those, if so just return original value
    for frequency in datavic_update_frequencies:
        if frequency['text'].lower() == value.lower():
            return frequency['value']

    # Otherwise return the default of 'unknown'
    return 'unknown'


class MetaShareHarvester(HarvesterBase):

    config = None
    force_import = False

    # Copied from `ckanext/harvest/harvesters/ckanharvester.py`
    def _set_config(self, config_str):
        if config_str:
            self.config = json.loads(config_str)
            if 'api_version' in self.config:
                self.api_version = int(self.config['api_version'])

            log.debug('Using config: %r', self.config)
        else:
            self.config = {}

    # Copied from `ckanext/dcat/harvesters/base.py`
    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    # Copied from `ckanext/dcat/harvesters/base.py`
    def _get_package_name(self, harvest_object, title):

        package = harvest_object.package
        if package is None or package.title != title:
            name = self._gen_new_name(title)
            if not name:
                raise Exception(
                    'Could not generate a unique name from the title or the '
                    'GUID. Please choose a more unique title.')
        else:
            name = package.name

        return name

    def info(self):
        return {
            'name': 'metashare',
            'title': 'MetaShare Harvester',
            'description': 'Harvester for MetaShare dataset descriptions ' +
                           'serialized as JSON'
        }

    def validate_config(self, config):
        '''
        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        Validates the default_group entered exists and creates default_group_dicts

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''
        if not config:
            raise ValueError('No config options set')
        {
            "default_groups": ["spatial-data"],
            "full_metadata_url_prefix": "https://datashare.maps.vic.gov.au/search?md=",
            "attribution": "Copyright (c) The State of Victoria, Department of Environment, Land, Water & Planning",
            "license_id": "cc-by"
        }
        try:
            config_obj = json.loads(config)

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'], list):
                    raise ValueError('default_groups must be a *list* of group'
                                     ' names/ids')
            else:
                raise ValueError('default_groups must be set')

            if 'full_metadata_url_prefix' not in config_obj:
                raise ValueError('full_metadata_url_prefix must be set')

            if 'license_id' not in config_obj:
                raise ValueError('license_id must be set')

            if 'resource_attribution' not in config_obj:
                raise ValueError('resource_attribution must be set')
        except ValueError, e:
            raise e

        return config

    def _get_page_of_records(self, url, page, datasets_per_page=100):
        _from, _to = _get_from_to(page, datasets_per_page)
        records = None
        try:
            request_url = '{0}?from={1}&to={2}&_content_type=json&fast=index'.format(url, _from, _to)
            log.debug('Getting page of records {}'.format(request_url))
            r = requests.get(request_url)

            if r.status_code == 200:
                data = json.loads(r.text)

                # Records are contained in the "metadata" element of the response JSON
                # see example: https://dev-metashare.maps.vic.gov.au/geonetwork/srv/en/q?from=1&to=1&_content_type=json&fast=index
                records = data.get('metadata', None)
        except Exception as e:
            log.error(e)

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
                log.debug('Datasets data is not a list: {}'.format(type(datasets)))
                raise ValueError('Wrong JSON object')

        for dataset in datasets:

            as_string = json.dumps(dataset)

            # Get identifier
            geonet_info = dataset.get('geonet:info', None)
            guid = geonet_info.get('uuid', None)

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

        full_metadata_url_prefix = self.config.get('full_metadata_url_prefix', None)
        full_metadata_url = '{0}{1}'.format(full_metadata_url_prefix, uuid) if full_metadata_url_prefix else ''

        package_dict = {}

        extras = [
            # Mandatory fields where no value exists in MetaShare
            # So we set them to Data.Vic defaults
            {'key': 'personal_information', 'value': 'no'},
            {'key': 'protective_marking', 'value': 'Public Domain'},
            {'key': 'access', 'value': 'yes'},
        ]

        package_dict['title'] = metashare_dict.get('title', None)
        # TODO: Some datasets have non ascii data in abstract
        # We have a few options to handle the error:
        # A String specifying the error method. Legal values are:
        # 'backslashreplace'	- uses a backslash instead of the character that could not be encoded
        # 'ignore'	- ignores the characters that cannot be encoded
        # 'namereplace'	- replaces the character with a text explaining the character
        # 'strict'	- Default, raises an error on failure
        # 'replace'	- replaces the character with a questionmark
        # 'xmlcharrefreplace'	- replaces the character with an xml character
        package_dict['notes'] = metashare_dict.get('abstract', '').encode('ascii', 'xmlcharrefreplace')

        # Get organisation from the harvest source organisation dropdown
        source_dict = logic.get_action('package_show')({}, {'id': harvest_object.harvest_source_id})
        package_dict['owner_org'] = source_dict.get('owner_org')

        # Decision from discussion with Simon/DPC on 2020-10-13 is to assign all datasets to "Spatial Data" group
        default_groups = self.config.get('default_groups', None)
        if default_groups and isinstance(default_groups, list):
            package_dict['groups'] = [{"name": group} for group in default_groups]

        # Default as discussed with SDM
        package_dict['license_id'] = self.config.get('license_id', 'cc-by')

        # Tags / Keywords
        # `topicCat` can either be a single tag as a string or a list of tags
        topic_cat = metashare_dict.get('topicCat', None)
        if topic_cat:
            package_dict['tags'] = get_tags(topic_cat)

        extras.append({
            'key': 'extract',
            # Get the first sentence
            'value': '{}...'.format(package_dict['notes'].split('.')[0])
        })

        # There is no field in Data.Vic schema to store the source UUID of the harvested record
        # Therefore, we are using the `primary_purpose_of_collection` field
        if uuid:
            extras.append({
                'key': 'primary_purpose_of_collection',
                'value': uuid
            })

        # @TODO: Consider this - in the field mapping spreadsheet:
        # https://docs.google.com/spreadsheets/d/112hzp6ZrTnp3fl_ZdmT6oHldUGf36LvEpLswAJDLdr0/edit#gid=1669999637
        # The response from SDM was:
        #       "We could either add Custodian to the Q Search results or just use resource owner. Given that it is
        #       not publically displayed in DV, not sure it's worth the effort of adding the custodian"
        res_owner = metashare_dict.get('resOwner', None)
        if res_owner:
            package_dict['maintainer'] = res_owner[0] if isinstance(res_owner, list) else res_owner

        # Decision from discussion with Simon/DPC on 2020-10-13 is to assign all datasets to "Spatial Data" group
        # Data.Vic "category" field is equivalent to groups, but stored as an extra and only has 1 group
        category = default_groups[0] if default_groups else None
        if category:
            extras.append({
                'key': 'category',
                'value': category
            })

        # @TODO: Default to UTC now if not available... OR try and get it from somewhere else in the record
        # date provided seems to be a bit of a mess , e.g. '2013-03-31t13:00:00.000z'
        # might need to run some regex on this
        temp_extent_begin = metashare_dict.get('tempExtentBegin', None)
        if temp_extent_begin:
            extras.append({
                'key': 'date_created_data_asset',
                'value': process_date(temp_extent_begin)
            })
        else:
            print('WHAT DO WE DO HERE? tempExtentBegin does not exist for {}'.format(uuid))

        # @TODO: Examples can be "2012-03-27" - do we need to convert this to UTC before inserting?
        # is a question for SDM - i.e. are their dates in UTC or Vic/Melb time?
        extras.append({
            'key': 'date_modified_data_asset',
            'value': metashare_dict.get('revisionDate', None)
        })

        extras.append({
            'key': 'update_frequency',
            'value': map_update_frequency(get_datavic_update_frequencies(),
                                          metashare_dict.get('maintenanceAndUpdateFrequency_text', 'unknown'))
        })

        if full_metadata_url:
            extras.append({
                'key': 'full_metadata_url',
                'value': full_metadata_url
            })

        # Create a single resource for the dataset
        resource = {
            'name': metashare_dict.get('title', None),
            'format': metashare_dict.get('spatialRepresentationType_text', None),
            'period_start': process_date(metashare_dict.get('tempExtentBegin', None)),
            'period_end': process_date(metashare_dict.get('tempExtentEnd', None)),
            'url': full_metadata_url
        }

        attribution = self.config.get('resource_attribution', None)
        if attribution:
            resource['attribution'] = attribution

        package_dict['resources'] = [resource]

        # @TODO: What about these ones?
        # responsibleParty

        # Add all the `extras` to our compiled dict
        package_dict['extras'] = extras

        return package_dict

    def gather_stage(self, harvest_job):

        log.debug('In MetaShareHarvester gather_stage')

        #
        # BEGIN: This section is copied from ckanext/dcat/harvesters/_json.py
        # @TODO: Move this into a separate function for readability
        # (except the `ids = []` & `guids_in_source = []` lines)
        #
        ids = []

        # Get the previous guids for this source
        query = \
            model.Session.query(HarvestObject.guid, HarvestObject.package_id) \
            .filter(HarvestObject.current == True) \
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)

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

        harvest_source_url = harvest_job.source.url[:-1] if harvest_job.source.url.endswith('?') else harvest_job.source.url

        # _get_page_of_records will return None if there are no more records
        records = True
        while records:
            records = self._get_page_of_records(harvest_source_url, page, records_per_page)

            batch_guids = []
            if records:
                #
                # BEGIN: This section is copied from ckanext/dcat/harvesters/_json.py
                #
                for guid, as_string in self._get_guids_and_datasets(records):
                    # Only add back for debugging as it pollutes the logs with 1700+ guids
                    # log.debug('Got identifier: {0}'
                    #           .format(guid.encode('utf8')))
                    batch_guids.append(guid)

                    if guid not in previous_guids:

                        if guid in guids_in_db:
                            # Dataset needs to be udpated
                            obj = HarvestObject(
                                guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                content=as_string,
                                extras=[HarvestObjectExtra(key='status',
                                                           value='change')])
                        else:
                            # Dataset needs to be created
                            obj = HarvestObject(
                                guid=guid, job=harvest_job,
                                content=as_string,
                                extras=[HarvestObjectExtra(key='status',
                                                           value='new')])
                        obj.save()
                        ids.append(obj.id)

                if len(batch_guids) > 0:
                    guids_in_source.extend(set(batch_guids)
                                           - set(previous_guids))
                else:
                    log.debug('Empty document, no more records')
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
                guid=guid, job=harvest_job,
                package_id=guid_to_package_id[guid],
                extras=[HarvestObjectExtra(key='status', value='delete')])
            ids.append(obj.id)
            model.Session.query(HarvestObject).\
                filter_by(guid=guid).\
                update({'current': False}, False)
            obj.save()

        return ids
        #
        # END: This section is copied from ckanext/dcat/harvesters/_json.py
        #

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        """
        Mostly copied from `ckanext/dcat/harvesters/_json.py`
        :param harvest_object:
        :return:
        """
        log.debug('In MetaShareHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if self.force_import:
            status = 'change'
        else:
            status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # Delete package
            context = {'model': model, 'session': model.Session,
                       'user': self._get_user_name()}

            p.toolkit.get_action('package_delete')(
                context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'
                     .format(harvest_object.package_id, harvest_object.guid))

            return True

        if harvest_object.content is None:
            self._save_object_error(
                'Empty content for object %s' % harvest_object.id,
                harvest_object, 'Import')
            return False

        if harvest_object.guid is None:
            self._save_object_error(
                'Empty guid for object %s' % harvest_object.id,
                harvest_object, 'Import')
            return False

        self._set_config(harvest_object.job.source.config)

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(HarvestObject.current == True) \
            .first()

        # Flag previous object as not current anymore
        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()

        package_dict = self._get_package_dict(harvest_object)

        if not package_dict:
            return False

        if not package_dict.get('name'):
            package_dict['name'] = \
                self._get_package_name(harvest_object, package_dict['title'])

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        context = {
            'user': self._get_user_name(),
            'return_id_only': True,
            'ignore_auth': True,
        }

        try:
            if status == 'new':
                package_schema = logic.schema.default_create_package_schema()
                context['schema'] = package_schema

                # We need to explicitly provide a package ID
                package_dict['id'] = str(uuid.uuid4())
                package_schema['id'] = [str]

                # Save reference to the package on the object
                harvest_object.package_id = package_dict['id']
                harvest_object.add()

                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                model.Session.execute(
                    'SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()

            elif status == 'change':
                package_dict['id'] = harvest_object.package_id

            if status in ['new', 'change']:
                action = 'package_create' if status == 'new' else 'package_update'
                message_status = 'Created' if status == 'new' else 'Updated'

                package_id = p.toolkit.get_action(action)(context, package_dict)
                log.info('%s dataset with id %s', message_status, package_id)

        except Exception as e:
            dataset = json.loads(harvest_object.content)
            dataset_name = dataset.get('name', '')

            self._save_object_error('Error importing dataset %s: %r / %s' % (dataset_name, e, traceback.format_exc()), harvest_object, 'Import')
            return False

        finally:
            model.Session.commit()

        return True
