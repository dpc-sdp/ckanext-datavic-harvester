import json

from bs4 import BeautifulSoup
from ckanext.datavic_harvester import bs4_helpers
from ckan.common import config
from ckanext.dcat import converters
from ckanext.dcat.harvesters._json import DCATJSONHarvester
from datetime import datetime
from dateutil import tz


class DataVicDCATJSONHarvester(DCATJSONHarvester):

    def info(self):
        return {
            'name': 'datavic_dcat_json',
            'title': 'DataVic DCAT JSON Harvester',
            'description': 'DataVic Harvester for DCAT dataset descriptions ' +
                           'serialized as JSON'
        }

    def convert_utc_datetime_to_local(self, utc_datetime):
        try:
            from_zone = tz.gettz('UTC')
            to_zone = tz.gettz(config.get('ckan.display_timezone', 'Australia/Melbourne'))

            utc = datetime.strptime(utc_datetime, '%Y-%m-%dT%H:%M:%S.%fZ')

            # Tell the datetime object that it's in UTC time zone since
            # datetime objects are 'naive' by default
            utc = utc.replace(tzinfo=from_zone)

            # Convert time zone
            local_datetime = utc.astimezone(to_zone)
            return local_datetime.date()
        except Exception:
            return utc_datetime

    def assign_dcat_dates_to_dataset(self, dcat_dict, package_dict):
        '''
        Convert issued & modified dates to local timezone and assign to Data.Vic metadata fields
        :param dcat_dict:
        :param package_dict:
        :return:
        '''
        if 'issued' in dcat_dict:
            package_dict['extras'].append(
                {'key': 'date_created_data_asset', 'value': self.convert_utc_datetime_to_local(dcat_dict['issued'])})

        if 'modified' in dcat_dict:
            package_dict['extras'].append(
                {'key': 'date_modified_data_asset', 'value': self.convert_utc_datetime_to_local(dcat_dict['modified'])})

        # Default group assignment
        if 'groups' not in package_dict:
            package_dict['groups'] = [{'name': 'transport'}]

    def assign_datavic_required_fields(self, package_dict):
        '''
        Default Data.Vic mandatory field assignments
        These would ideally be handled in an `edit` function on the IPackageController interface, but
        there is already one in use on the ckanext-workflow extension which seems to block implementing on
        the ckanext-datavicmain extension
        :param package_dict:
        :return:
        '''
        required_fields = [
            {'key': 'personal_information', 'default': 'no'},
            {'key': 'protective_marking', 'default': 'Public Domain'},
            {'key': 'access', 'default': 'no'},
            {'key': 'update_frequency', 'default': 'unknown'},
        ]
        for required_field in required_fields:
            current_value = [extra for extra in package_dict['extras'] if extra['key'] == required_field['key']]
            if not current_value:
                package_dict['extras'].append({'key': required_field['key'], 'value': required_field['default']})

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

        soup = BeautifulSoup(package_dict['notes'], 'html.parser')

        extract = [extra for extra in package_dict['extras'] if extra['key'] == 'extract']

        if 'default.description' in package_dict['notes']:
            package_dict['notes'] = 'No description has been entered for this dataset.'
            if not extract:
                package_dict['extras'].append({'key': 'extract', 'value': 'No abstract has been entered for this dataset.'})
        else:
            package_dict['notes'] = bs4_helpers._unwrap_all_except(
                bs4_helpers._remove_all_attrs_except_saving(soup),
                # allowed tags
                ['a']
            )
            if not extract:
                extract = self.generate_extract(soup)
                if extract:
                    package_dict['extras'].append({'key': 'extract', 'value': extract})

        # Try and extract the full metadata URL from the dataset description
        full_metadata_url = [extra for extra in package_dict['extras'] if extra['key'] == 'full_metadata_url']
        if not full_metadata_url:
            full_metadata_url = bs4_helpers._extract_metadata_url(soup, 'data.vicroads.vic.gov.au/metadata')
            if full_metadata_url:
                package_dict['extras'].append({'key': 'full_metadata_url', 'value': full_metadata_url})
                # Attempt to extract the update frequency from the full metadata page
                package_dict['extras'].append({
                    'key': 'update_frequency',
                    'value': bs4_helpers._fetch_update_frequency(full_metadata_url)
                })

        self.assign_dcat_dates_to_dataset(dcat_dict, package_dict)

        self.assign_datavic_required_fields(package_dict)

        self.fix_erroneous_tags(package_dict)

        return package_dict, dcat_dict
