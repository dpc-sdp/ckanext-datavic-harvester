import json
from ckan import model
from ckan.logic import ValidationError, NotFound, get_action
from ckan.plugins import toolkit
from bs4 import BeautifulSoup
from ckanext.datavic_harvester import bs4_helpers
from ckanext.dcat import converters
from ckanext.dcat.harvesters._json import DCATJSONHarvester
from ckanext.harvest.model import HarvestSource


class DataVicDCATJSONHarvester(DCATJSONHarvester):

    def info(self):
        return {
            'name': 'datavic_dcat_json',
            'title': 'DataVic DCAT JSON Harvester',
            'description': 'DataVic Harvester for DCAT dataset descriptions ' +
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
            return config

        try:
            config_obj = json.loads(config)

            if 'default_groups' in config_obj:
                if not isinstance(config_obj['default_groups'], list):
                    raise ValueError('default_groups must be a *list* of group'
                                     ' names/ids')
                if config_obj['default_groups'] and \
                        not isinstance(config_obj['default_groups'][0],
                                       basestring):
                    raise ValueError('default_groups must be a list of group '
                                     'names/ids (i.e. strings)')

                # Check if default groups exist
                context = {'model': model, 'user': toolkit.c.user}
                config_obj['default_group_dicts'] = []
                for group_name_or_id in config_obj['default_groups']:
                    try:
                        group = get_action('group_show')(context, {'id': group_name_or_id})
                        # save the dict to the config object, as we'll need it
                        # in the set_default_group of every dataset
                        config_obj['default_group_dicts'].append({'id': group['id'], 'name': group['name']})
                    except NotFound, e:
                        raise ValueError('Default group not found')
                config = json.dumps(config_obj, indent=1)

        except ValueError, e:
            raise e

        return config

    def fix_erroneous_tags(self, package_dict):
        '''
        Replace ampersands with "and" in tags
        :param package_dict:
        :return:
        '''
        if package_dict['tags']:
            for tag in package_dict['tags']:
                if 'name' in tag and '&' in tag['name']:
                    tag['name'] = tag['name'].replace('&', 'and')

    def generate_extract(self, soup):
        '''
        Extract is just the first sentence of the text-only description/notes for our purposes at this stage.
        :param soup:
        :return:
        '''
        try:
            notes = soup.get_text()
            index = notes.index('.')
            notes = notes[:index + 1]
        except Exception:
            pass
        return notes

    def set_description_and_extract(self, package_dict, soup):
        extract = [extra for extra in package_dict['extras'] if extra['key'] == 'extract']

        if 'default.description' in package_dict['notes']:
            package_dict['notes'] = 'No description has been entered for this dataset.'
            if not extract:
                package_dict['extras'].append({'key': 'extract', 'value': 'No abstract has been entered for this dataset.'})
        else:
            package_dict['notes'] = bs4_helpers._unwrap_all_except(
                bs4_helpers._remove_all_attrs_except_saving(soup),
                # allowed tags
                ['a', 'br']
            )
            if not extract:
                extract = self.generate_extract(soup)
                if extract:
                    package_dict['extras'].append({'key': 'extract', 'value': extract})

    def set_full_metadata_url_and_update_frequency(self, harvest_config, package_dict, soup):
        '''
        Try and extract the full metadata URL from the dataset description and then the update frequency from the
        full metadata URL.
        If full metadata URL not found, or update frequency not determined, it will default to 'unknown' either
        through the `_fetch_update_frequency` function or the IPackageController `create` function in
        ckanext.datavicmain.plugins.py
        :param package_dict:
        :param soup:
        :return:
        '''
        #
        full_metadata_url = [extra for extra in package_dict['extras'] if extra['key'] == 'full_metadata_url']
        if not full_metadata_url:
            # Set the default if it has been added to the harvest source config
            if 'default_full_metadata_url' in harvest_config:
                full_metadata_url = harvest_config['default_full_metadata_url']
            # Try and extract a full metadata url from the description based on
            # a pattern defined in the harvest source config
            if 'full_metadata_url_pattern' in harvest_config:
                desc_full_metadata_url = bs4_helpers._extract_metadata_url(soup, harvest_config['full_metadata_url_pattern'])
                if desc_full_metadata_url:
                    full_metadata_url = desc_full_metadata_url
                    # Attempt to extract the update frequency from the full metadata page
                    package_dict['extras'].append({
                        'key': 'update_frequency',
                        'value': bs4_helpers._fetch_update_frequency(full_metadata_url)
                    })
        if full_metadata_url:
            package_dict['extras'].append({
                'key': 'full_metadata_url',
                'value': full_metadata_url
            })

    def set_default_group(self, harvest_config, package_dict):
        '''
        Set the default group from config
        :param harvest_config:
        :param package_dict:
        :return:
        '''
        # Set default groups if needed
        default_groups = harvest_config.get('default_groups', [])
        if default_groups:
            if not 'groups' in package_dict:
                package_dict['groups'] = []
            existing_group_ids = [g['id'] for g in package_dict['groups']]
            package_dict['groups'].extend(
                [g for g in harvest_config['default_group_dicts']
                    if g['id'] not in existing_group_ids])

    def set_required_fields_defaults(self, harvest_config, dcat_dict, package_dict):
        personal_information = [extra for extra in package_dict['extras'] if
                                extra['key'] == 'personal_information']
        if not personal_information:
            package_dict['extras'].append({
                'key': 'personal_information',
                'value': 'no'
            })

        access = [extra for extra in package_dict['extras'] if
                  extra['key'] == 'access']
        if not access:
            package_dict['extras'].append({
                'key': 'access',
                'value': 'yes'
            })

        protective_marking = [extra for extra in package_dict['extras'] if
                              extra['key'] == 'protective_marking']
        if not protective_marking:
            package_dict['extras'].append({
                'key': 'protective_marking',
                'value': 'official'
            })

        update_frequency = [extra for extra in package_dict['extras'] if
                            extra['key'] == 'update_frequency']
        if not update_frequency:
            package_dict['extras'].append({
                'key': 'update_frequency',
                'value': 'unknown'
            })

        issued = dcat_dict.get('issued')
        date_created_data_asset = [extra for extra in package_dict['extras'] if
                                   extra['key'] == 'date_created_data_asset']
        if issued and not date_created_data_asset:
            package_dict['extras'].append({
                'key': 'date_created_data_asset',
                'value': issued
            })

        modified = dcat_dict.get('modified')
        date_modified_data_asset = [extra for extra in package_dict['extras'] if
                                    extra['key'] == 'date_modified_data_asset']
        if modified and not date_modified_data_asset:
            package_dict['extras'].append({
                'key': 'date_modified_data_asset',
                'value': modified
            })

        landing_page = dcat_dict.get('landingPage')
        full_metadata_url = [extra for extra in package_dict['extras'] if
                             extra['key'] == 'full_metadata_url']
        if landing_page and not full_metadata_url:
            package_dict['extras'].append({
                'key': 'full_metadata_url',
                'value': landing_page
            })

        license_id = package_dict.get('license_id', None)
        if not license_id and 'default_license' in harvest_config:
            default_license = harvest_config.get('default_license')
            if default_license:
                default_license_id = default_license.get('id')
                default_license_title = default_license.get('title')
                if default_license_id:
                    package_dict['license_id'] = default_license_id
                if default_license_title:
                    package_dict['license_title'] = default_license_title

    def _get_package_dict(self, harvest_object):
        '''
        Converts a DCAT dataset into a CKAN dataset
        and performs some Data.Vic specific conversion of the data
        :param harvest_object:
        :return:
        '''

        content = harvest_object.content

        dcat_dict = json.loads(content)

        package_dict = converters.dcat_to_ckan(dcat_dict)

        try:
            # Get the harvest source configuration settings via the `harvest_source_id` property of the harvest object
            harvest_source = HarvestSource.get(harvest_object.harvest_source_id)
            harvest_config = json.loads(harvest_source.config)
        except Exception:
            harvest_config = None

        soup = BeautifulSoup(package_dict['notes'], 'html.parser')

        self.set_description_and_extract(package_dict, soup)

        self.set_full_metadata_url_and_update_frequency(harvest_config, package_dict, soup)

        self.fix_erroneous_tags(package_dict)

        # Groups (Categories)
        # Default group is set in the harvest source configuration, "default_groups" property.
        self.set_default_group(harvest_config, package_dict)

        self.set_required_fields_defaults(harvest_config, dcat_dict, package_dict)

        return package_dict, dcat_dict
