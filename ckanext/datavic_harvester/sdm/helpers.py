from datetime import datetime
from ckanext.datavicmain.schema import DATASET_EXTRA_FIELDS


def get_datavic_update_frequencies():
    # DATASET_EXTRA_FIELDS is a list of tuples - where the first element
    # is the name of the metadata schema field
    # The second element contains the configuration for the field
    update_frequencie_options = [x[1]['options'] for x in DATASET_EXTRA_FIELDS if x[0] == 'update_frequency']

    return update_frequencie_options[0]


def map_update_frequency(datavic_update_frequencies, value):
    # Check if the value from SDM matches one of those, if so just return original value
    for frequency in datavic_update_frequencies:
        if frequency['text'].lower() == value.lower():
            return frequency['value']

    # Otherwise return the default of 'unknown'
    return 'unknown'


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


def construct_dataset_dict(record, datavic_update_frequencies):
    dataset_dict = {
        'extras': [],
        'resources': [],
    }

    extras = []

    # We need to store the UUID of the record from SDM, so that we can link to it from
    # the Data.Vic UI for ordering - we don't really have a field in the Data.Vic metadata
    # schema for storing that so we'll use the `purpose` field for now...
    # there is a sub-property in the SDM metadata `geonet:info` containing the UUID
    geonet_info = record.get('geonet:info', [])

    uuid = geonet_info.get('uuid', None)

    # @TODO: Munge the title into the name/slug
    dataset_dict['name'] = uuid

    dataset_dict['title'] = record.get('title', None)
    dataset_dict['notes'] = record.get('abstract', None)

    # Defaults
    dataset_dict['owner_org'] = 'department-of-environment-land-water-planning'

    # Decision from discussion with Simon/DPC on 2020-10-13 is to assign all datasets to "Spatial Data" group
    dataset_dict['groups'] = [{
        'name': 'spatial-data'
    }]

    # Default as discussed with SDM
    dataset_dict['license_id'] = 'cc-by'

    # Tags / Keywords
    # `topicCat` can either be a single tag as a string or a list of tags
    topic_cat = record.get('topicCat', None)
    if topic_cat:
        dataset_dict['tags'] = get_tags(topic_cat)


    # @TODO; truncate the notes for the abstract
    extras.append({
        'key': 'extract',
        'value': dataset_dict['notes']
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
    res_owner = record.get('resOwner', None)
    if res_owner:
        extras.append({
            'key': 'data_owner',
            'value': res_owner
        })

    # Decision from discussion with Simon/DPC on 2020-10-13 is to assign all datasets to "Spatial Data" group
    extras.append({
        'key': 'category',
        'value': 'spatial-data'
    })

    # @TODO: Default to UTC now if not available... OR try and get it from somewhere else in the record
    # date provided seems to be a bit of a mess , e.g. '2013-03-31t13:00:00.000z'
    # might need to run some regex on this
    temp_extent_begin = record.get('tempExtentBegin', None)
    if temp_extent_begin:
        extras.append({
            'key': 'date_created_data_asset',
            'value': process_date(temp_extent_begin)
        })
    else:
        print('WHAT DO WE DO HERE? tempExtentBegin does not exist for {}'.format(uuid))

    # @TODO: Examples can be "2012-03-27" - do we need to convert this to UTC before inserting
    extras.append({
        'key': 'date_modified_data_asset',
        'value': record.get('revisionDate', None)
    })

    # @TODO: Convert `maintenanceAndUpdateFrequency_text` value from SDM to Data.Vic options
    extras.append({
        'key': 'update_frequency',
        'value': map_update_frequency(datavic_update_frequencies, record.get('maintenanceAndUpdateFrequency_text', None))
    })

    # Mandatory fields where no value comes across from SDM metashare
    # So we default them to Data.Vic defaults
    extras += [
        {'key': 'personal_information', 'value': 'no'},
        {'key': 'protective_marking', 'value': 'Public Domain'},
        {'key': 'access', 'value': 'yes'},
    ]

    # Create a single resource for the dataset
    dataset_dict['resources'] = [
        {
            'url': 'http://sdm.com',
            'name': record.get('title', None),
            'format': 'csv',
            'period_start': process_date(record.get('tempExtentBegin', None)),
            'period_end': process_date(record.get('tempExtentEnd', None)),
            'attribution': 'Copyright (c) The State of Victoria, Department of Environment, Land, Water & Planning',
        }
    ]

    # @TODO: What about these ones?
    # full_metadata_url (optional)
    # responsibleParty

    # Add all the `extras` to our compiled dict
    dataset_dict['extras'] = extras

    return dataset_dict
