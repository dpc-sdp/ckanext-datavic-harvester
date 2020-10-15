import json
import helpers
import requests

from ckanapi import RemoteCKAN
from pprint import pprint


BASE_URL = 'https://dev-metashare.maps.vic.gov.au/geonetwork/srv/en/q?from={0}&to={1}&_content_type=json&fast=index'
ckan = RemoteCKAN('http://datavic-ckan.docker.amazee.io', apikey='7f3dad2a-21b5-4060-a94d-4bb6b2c182bb')


def gather(start, end):
    try:
        r = requests.get(BASE_URL.format(start, end))

        if r.status_code == 200:
            data = json.loads(r.text)

            # Records are contained in the "metadata" element of the response JSON
            # see example: https://dev-metashare.maps.vic.gov.au/geonetwork/srv/en/q?from=1&to=1&_content_type=json&fast=index
            return data.get('metadata', None)
    except Exception as e:
        print(str(e))

    return []


# The purpose of this script is to prove the viability of fetching datasets from the new SDM endpoint
# and creating them within CKAN
# Once this process is confirmed, we can incorporate this into an extension of the ckanext-harvest
# extension so that we get the visibility, error reporting around harvesting, and the UI within CKAN

# Do this only once - instead of loading them for each record we are parsing
datavic_update_frequencies = helpers.get_datavic_update_frequencies()

# pprint(datavic_update_frequencies)
#
# value = "Not planned"
#
# print(helpers.map_update_frequency(datavic_update_frequencies, value))

metadata = gather(1, 3)

if metadata:
    for record in metadata:
        dataset_dict = helpers.construct_dataset_dict(record, datavic_update_frequencies)

        try:
            ckan.action.package_create(**dataset_dict)
            pprint(dataset_dict)
        except Exception as e:
            print('>>> DATASET NOT CREATED: SEE EXCEPTION <<< ')
            print('>>> DATASET NOT CREATED: SEE EXCEPTION <<< ')
            print(str(e))
            print('>>> DATASET NOT CREATED: SEE EXCEPTION <<< ')
            print('>>> DATASET NOT CREATED: SEE EXCEPTION <<< ')

        print('----------')
        print('----------')
