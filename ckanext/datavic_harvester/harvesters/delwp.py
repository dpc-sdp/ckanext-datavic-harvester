from datetime import datetime
import json
import logging
import requests
import traceback
import uuid
import re
import six
from bs4 import BeautifulSoup

from ckan import logic
from ckan import model
from ckanext.harvest.harvesters import HarvesterBase
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from hashlib import sha1
from ckan.plugins import toolkit as toolkit

import ckanext.datavicmain.helpers as helpers

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
    value = re.split(';|,', value)
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


def convert_date_to_isoformat(value, key, dataset_name):
    """
    Example dates:
        '2020-10-13t05:00:11'
        u'2006-12-31t13:00:00.000z'
    :param value:
    :return:
    """
    date = None
    try:
        # Remove any timezone with time
        value = value.lower().split('t')[0]
        date = toolkit.get_converter('isodate')(value, {})
    except Exception as ex:
        log.debug('{0}: Date format incorrect {1} for key {2}'.format(dataset_name, value, key))
        log.debug(ex)
    # TODO: Do we return None or value if date string cannot be converted?
    return date.isoformat() if date else None


def get_datavic_update_frequencies():
    return helpers.field_choices('update_frequency')


def map_update_frequency(datavic_update_frequencies, value):
    # Check if the value from SDM matches one of those, if so just return original value
    for frequency in datavic_update_frequencies:
        if frequency['label'].lower() == value.lower():
            return frequency['value']

    # Otherwise return the default of 'unknown'
    return 'unknown'


def munge_title_to_name(name):
    '''Munge a package title into a package name.
        Copied from vicmaps-harvest.py to use the same code to create name from title
        This is required to match existing pacakge names 
    '''
    # convert spaces and separators
    name = re.sub('[ .:/,]', '-', name)
    # take out not-allowed characters
    name = re.sub('[^a-zA-Z0-9-_]', '', name).lower()
    # remove doubles
    name = re.sub('---', '-', name)
    name = re.sub('--', '-', name)
    name = re.sub('--', '-', name)
    # remove leading or trailing hyphens
    name = name.strip('-')[:99]
    return name


def _get_organisation(organisation_mapping, resowner, harvest_object, context):
    org_name = next((organisation.get('org-name') for organisation in organisation_mapping if organisation.get('resowner') == resowner), None)
    if org_name:
        return org_name
    else:
        # No mapping found, see if the organisation exist
        log.warning(f'DELWP harvester _get_organisation: No mapping found for resowner {resowner}')
        org_title = resowner
        org_name = munge_title_to_name(org_title)
        org_id = None
        try:
            data_dict = {
                'id': org_name,
                "include_dataset_count": False,
                "include_extras": False,
                "include_users": False,
                "include_groups": False,
                "include_tags": False,
                "include_followers": False
            }
            organisation = toolkit.get_action('organization_show')(context.copy(), data_dict)
            org_id = organisation.get('id')
        except toolkit.ObjectNotFound:
            log.warning(f'DELWP harvester _get_organisation: Organisation does not exist {org_id}')
            # Organisation does not exist so create it and use it
            try:
                # organization_create will return organisation id because context has 'return_id_only' to true
                org_id = toolkit.get_action('organization_create')(context.copy(), {'name': org_name, 'title': org_title})
            except Exception as e:
                log.warning(f'DELWP harvester _get_organisation: Failed to create organisation {org_name}')
                log.error(f'DELWP harvester _get_organisation: {str(e)}')
                # Fallback to using organisation from harvest source
                source_dict = logic.get_action('package_show')(context.copy(), {'id': harvest_object.harvest_source_id})
                org_id = source_dict.get('owner_org')
        return org_id

def clean_resource_name(name):
    '''
     Replace underscores (_) with spaces to avoid braking words
    '''
    # convert underscores to spaces
    name = re.sub('_', ' ', name)

    return name

def _generate_geo_resource(layer_data_with_uuid, resource_format, resource_url):
    resource_data = {
        "name": layer_data_with_uuid.find_previous("Title").text.upper() + ' ' + resource_format,
        "format": resource_format,
        "url": resource_url.format(layername=layer_data_with_uuid.find_previous("Name").text)
    }
    return resource_data

class DelwpHarvester(HarvesterBase):

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
            'name': 'delwp',
            'title': 'DELWP Harvester',
            'description': 'Harvester for DELWP dataset descriptions ' +
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

        try:
            config_obj = json.loads(config)
            context = {'model': model, 'user': toolkit.g.user}
            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'], list):
                    raise ValueError('default_groups must be a *list* of group'
                                     ' names/ids')
                if config_obj['default_groups'] and \
                        not isinstance(config_obj['default_groups'][0],
                                       six.string_types):
                    raise ValueError('default_groups must be a list of group '
                                     'names/ids (i.e. strings)')

                # Check if default groups exist
                config_obj['default_group_dicts'] = []
                for group_name_or_id in config_obj['default_groups']:
                    try:
                        group = toolkit.get_action('group_show')(
                            context.copy(), {'id': group_name_or_id})
                        # save the dict to the config object, as we'll need it
                        # in the import_stage of every dataset
                        config_obj['default_group_dicts'].append(group)
                    except toolkit.ObjectNotFound:
                        raise ValueError('Default group not found')
                config = json.dumps(config_obj)
            else:
                raise ValueError('default_groups must be set')

            if 'full_metadata_url_prefix' not in config_obj:
                raise ValueError('full_metadata_url_prefix must be set')

            if '{UUID}' not in config_obj.get('full_metadata_url_prefix', ''):
                raise ValueError('full_metadata_url_prefix must have the {UUID} identifier in the URL')

            if 'resource_url_prefix' not in config_obj:
                raise ValueError('resource_url_prefix must be set')

            if 'license_id' not in config_obj:
                raise ValueError('license_id must be set')

            if 'resource_attribution' not in config_obj:
                raise ValueError('resource_attribution must be set')

            if 'dataset_type' not in config_obj:
                raise ValueError('dataset_type must be set')

            if 'api_auth' not in config_obj:
                raise ValueError('api_auth must be set')

            if 'organisation_mapping' in config_obj:
                if not isinstance(config_obj['organisation_mapping'], list):
                    raise ValueError('organisation_mapping must be a *list* of organisations')
                # Check if organisation exist
                for organisation_mapping in config_obj['organisation_mapping']:
                    if not isinstance(organisation_mapping, dict):
                        raise ValueError('organisation_mapping item must be a *dict*. eg {"resowner": "Organisation A", "org-name": "organisation-a"}')
                    if not organisation_mapping.get('resowner'):
                        raise ValueError('organisation_mapping item must have property "resowner". eg "resowner": "Organisation A"')
                    if not organisation_mapping.get('org-name'):
                        raise ValueError('organisation_mapping item must have property *org-name*. eg "org-name": "organisation-a"}')
                    try:
                        group = toolkit.get_action('organization_show')(context.copy(), {'id': organisation_mapping.get('org-name')})
                    except toolkit.ObjectNotFound:
                        raise ValueError(f'Organisation {organisation_mapping.get("org-name")} not found')
            else:
                raise ValueError('organisation_mapping must be set')
            
            config = json.dumps(config_obj, indent=1)
        except ValueError as e:
            raise e

        return config

    def _get_page_of_records(self, url, dataset_type, api_auth, page, datasets_per_page=100):
        _from, _to = _get_from_to(page, datasets_per_page)
        records = None
        try:
            request_url = '{0}?dataset={1}&start={2}&rows={3}&format=json'.format(url, dataset_type, _from, _to)
            log.debug('Getting page of records {}'.format(request_url))
            r = requests.get(request_url, headers={'Authorization': api_auth})

            if r.status_code == 200:
                data = json.loads(r.text)

                # Records are contained in the "metadata" element of the response JSON
                # see example: https://dev-metashare.maps.vic.gov.au/geonetwork/srv/en/q?from=1&to=1&_content_type=json&fast=index
                records = data.get('records', None)
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
            fields = dataset.get('fields', {})
            as_string = json.dumps(fields)

            # Get identifier
            guid = fields.get('uuid', None)

            yield guid, as_string
    
    def _get_package_dict(self, harvest_object, context):
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
        full_metadata_url = full_metadata_url_prefix.format(**{'UUID': uuid}) if full_metadata_url_prefix else ''
        resource_url_prefix = self.config.get('resource_url_prefix', None)
        resource_url = '{0}{1}'.format(resource_url_prefix, uuid) if resource_url_prefix else ''

        # Set the package_dict
        package_dict = {}

        # Mandatory fields where no value exists in MetaShare
        # So we set them to Data.Vic defaults
        package_dict['personal_information'] = 'no'
        package_dict['protective_marking'] = 'official'
        package_dict['access'] = 'yes'
        # Set to default values if missing
        package_dict['organization_visibility'] = 'all'
        package_dict['workflow_status'] = 'published'

        package_dict['title'] = metashare_dict.get('title', None)

        package_dict['notes'] = metashare_dict.get('abstract', '')

        # Get organisation from the harvest source organisation_mapping in config
        package_dict['owner_org'] = _get_organisation(self.config.get('organisation_mapping'), metashare_dict.get('resowner').split(';')[0], harvest_object, context)

        # Default as discussed with SDM
        package_dict['license_id'] = self.config.get('license_id', 'cc-by')

        # Tags / Keywords
        # `topicCat` can either be a single tag as a string or a list of tags
        topic_cat = metashare_dict.get('topiccat', None)
        if topic_cat:
            package_dict['tags'] = get_tags(topic_cat)

        # TODO: Is this the right metadata field? Should it be at the resource?
        # Las Updated  corelates to geonet_info_changedate
        package_dict['last_updated'] = metashare_dict.get('geonet_info_changedate', None)

        # TODO: Remove extras to package_dict
        package_dict['extract'] = '{}...'.format(package_dict['notes'].split('.')[0])

        # There is no field in Data.Vic schema to store the source UUID of the harvested record
        # Therefore, we are using the `primary_purpose_of_collection` field
        if uuid:
            package_dict['primary_purpose_of_collection'] = uuid

        # @TODO: Consider this - in the field mapping spreadsheet:
        # https://docs.google.com/spreadsheets/d/112hzp6ZrTnp3fl_ZdmT6oHldUGf36LvEpLswAJDLdr0/edit#gid=1669999637
        # The response from SDM was:
        #       "We could either add Custodian to the Q Search results or just use resource owner. Given that it is
        #       not publically displayed in DV, not sure it's worth the effort of adding the custodian"
        res_owner = metashare_dict.get('resowner', None)
        if res_owner:
            package_dict['data_owner'] = res_owner.split(';')[0]

        # Decision from discussion with Simon/DPC on 2020-10-13 is to assign all datasets to "Spatial Data" group
        # Data.Vic "category" field is equivalent to groups, but stored as an extra and only has 1 group
        default_group_dicts = self.config.get('default_group_dicts', None)
        if default_group_dicts and isinstance(default_group_dicts, list):
            package_dict['groups'] = [{"id": group.get('id')} for group in default_group_dicts]
            category = default_group_dicts[0] if default_group_dicts else None
            if category:
                package_dict['category'] = category.get('id')

        # @TODO: Default to UTC now if not available... OR try and get it from somewhere else in the record
        # date provided seems to be a bit of a mess , e.g. '2013-03-31t13:00:00.000z'
        # might need to run some regex on this
        #temp_extent_begin = metashare_dict.get('tempextentbegin', None)
        date_created_data_asset = convert_date_to_isoformat(metashare_dict.get('publicationdate', ''), 'publicationdate', metashare_dict.get('name'))
        if not date_created_data_asset:
            date_created_data_asset = convert_date_to_isoformat(
                metashare_dict.get('geonet_info_createdate', ''), 'geonet_info_createdate', metashare_dict.get('name'))
        package_dict['date_created_data_asset'] = date_created_data_asset

        # @TODO: Examples can be "2012-03-27" - do we need to convert this to UTC before inserting?
        # is a question for SDM - i.e. are their dates in UTC or Vic/Melb time?
        date_modified_data_asset = convert_date_to_isoformat(metashare_dict.get('revisiondate', ''), 'revisiondate', metashare_dict.get('name'))
        if not date_modified_data_asset:
            date_modified_data_asset = convert_date_to_isoformat(
                metashare_dict.get('geonet_info_changedate', ''), 'geonet_info_changedate', metashare_dict.get('name'))
        package_dict['date_modified_data_asset'] = date_modified_data_asset

        package_dict['update_frequency'] = map_update_frequency(get_datavic_update_frequencies(),
                                                                metashare_dict.get('maintenanceandupdatefrequency_text', 'unknown'))

        if full_metadata_url:
            package_dict['full_metadata_url'] = full_metadata_url

        attribution = self.config.get('resource_attribution', None)

        # Generate resources for the dataset
        formats = metashare_dict.get('available_formats', None)
        resources = []
        if formats:
            formats = formats.split(',')
            for format in formats:
                res = {
                    'name': metashare_dict.get('alttitle') or metashare_dict.get('title'),
                    'format': format,
                    'period_start': convert_date_to_isoformat(metashare_dict.get('tempextentbegin', ''), 'tempextentbegin', metashare_dict.get('name')),
                    'period_end': convert_date_to_isoformat(metashare_dict.get('tempextentend', ''), 'tempextentend', metashare_dict.get('name')),
                    'url': resource_url
                }
                
                res['name'] = res['name'] + ' ' + format
                res['name'] = clean_resource_name(res['name'])
                if attribution:
                    res['attribution'] = attribution
                resources.append(res)
                
        # Generate additional WMS/WFS resources     
        def _get_content_with_uuid(geoserver_url):
            try:
                geoserver_response = requests.get(geoserver_url)
            except requests.exceptions.RequestException as e:
                log.error(e)
                return None
            geoserver_content= BeautifulSoup(geoserver_response.content,"lxml-xml")
            return geoserver_content.find("Keyword", string=f"MetadataID={uuid}")
        
        if 'geoserver_dns' in self.config:
            geoserver_dns = self.config['geoserver_dns']
            dict_geoserver_urls = {
                'WMS': {
                    'geoserver_url': geoserver_dns + '/geoserver/ows?service=WMS&request=getCapabilities',
                    'resource_url': geoserver_dns + '/geoserver/wms?service=wms&request=getmap&format=image%2Fpng8&transparent=true&layers={layername}&width=512&height=512&crs=epsg%3A3857&bbox=16114148.554967716%2C-4456584.4971389165%2C16119040.524777967%2C-4451692.527328665'
                },
                'WFS': {
                    'geoserver_url': geoserver_dns + '/geoserver/ows?service=WFS&request=getCapabilities',
                    'resource_url': geoserver_dns + '/geoserver/wfs?request=GetCapabilities&service=WFS'
                }
            }
            
            for resource_format in dict_geoserver_urls:
                layer_data_with_uuid = _get_content_with_uuid(dict_geoserver_urls[resource_format].get('geoserver_url'))
                if layer_data_with_uuid:
                    resources.append(_generate_geo_resource(layer_data_with_uuid, resource_format, dict_geoserver_urls[resource_format].get('resource_url')))
        
        package_dict['resources'] = resources

        # @TODO: What about these ones?
        # responsibleParty

        # Add all the `extras` to our compiled dict

        return package_dict

    def gather_stage(self, harvest_job):

        log.debug('In Delwp Harvester gather_stage')

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
        self._set_config(harvest_job.source.config)

        # _get_page_of_records will return None if there are no more records
        records = True
        while records:
            dataset_type = self.config.get('dataset_type')
            api_auth = self.config.get('api_auth')
            records = self._get_page_of_records(harvest_source_url, dataset_type, api_auth, page, records_per_page)

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
        log.debug('In Delwp Harvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if self.force_import:
            status = 'change'
        else:
            status = self._get_object_extra(harvest_object, 'status')

        context = {
            'user': self._get_user_name(),
            'return_id_only': True,
            'ignore_auth': True,
            'model': model,
            'session': model.Session
        }

        if status == 'delete':
            # Delete package

            toolkit.get_action('package_delete')(
                context.copy(), {'id': harvest_object.package_id})
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

        self._set_config(harvest_object.source.config)

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(HarvestObject.current == True) \
            .first()

        # Flag previous object as not current anymore
        if previous_object and not self.force_import:
            previous_object.current = False
            previous_object.add()

        package_dict = self._get_package_dict(harvest_object, context)

        if not package_dict:
            return False

        if not package_dict.get('name'):
            package_dict['name'] = self._get_package_name(harvest_object, package_dict['title'])

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

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
                if 'package' in context:
                    del context['package']
                package_id = toolkit.get_action(action)(context.copy(), package_dict)
                log.info('%s dataset with id %s', message_status, package_id)

        except Exception as e:
            # dataset = json.loads(harvest_object.content)
            dataset_name = package_dict.get('name', '')

            self._save_object_error('Error importing dataset %s: %r / %s' % (dataset_name, e, traceback.format_exc()), harvest_object, 'Import')
            return False

        finally:
            model.Session.commit()

        return True
