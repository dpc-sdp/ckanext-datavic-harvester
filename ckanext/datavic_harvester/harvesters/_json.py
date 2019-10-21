import json

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
                ['a']
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

        return package_dict, dcat_dict
