import json
from ckanext.dcat import converters
from ckanext.dcat.harvesters._json import DCATJSONHarvester


class DataVicDCATJSONHarvester(DCATJSONHarvester):

    def info(self):
        return {
            'name': 'datavic_dcat_json',
            'title': 'DataVic DCAT JSON Harvester',
            'description': 'DataVic Harvester for DCAT dataset descriptions ' +
                           'serialized as JSON'
        }

    def _get_package_dict(self, harvest_object):

        content = harvest_object.content

        dcat_dict = json.loads(content)

        package_dict = converters.dcat_to_ckan(dcat_dict)
        
        package_dict['extras'].append({'key': 'DATAVIC-23', 'value': 'check'})

        return package_dict, dcat_dict
