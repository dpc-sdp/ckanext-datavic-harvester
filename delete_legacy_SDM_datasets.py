from datetime import datetime
from ckanapi import RemoteCKAN, NotFound

# TODO: Update URLS to the correct environment 
# PROD
# dd_url = "https://ckan-datavic-ckan-master.au.amazee.io"
# odp_url = "https://ckan-datavic-ckan-odp-master.au.amazee.io"
# Develop
# dd_url = "https://ckan-datavic-ckan-develop.au.amazee.io"
# odp_url = "https://ckan-datavic-ckan-odp-develop.au.amazee.io"
# Local
dd_url = "http://datavic-ckan.docker.amazee.io"
odp_url = "http://datavic-ckan-odp.docker.amazee.io"

# Update Authorization api key per environment. The below is the same keys for prod & develop
dd_apikey = 'c8a89820-a159-4c84-947d-3cb55c5a6156'
odp_apikey = '54384e40-e97d-4337-932d-fc7eb77e9511'

with open('delete_legacy_SDM_datasets_log-{}.csv'.format(datetime.now().isoformat()), 'w') as log_file:
    try:
        header = "Status,Project,Title,Name,URL,Error\n"
        log_file.write(header)
        with RemoteCKAN(dd_url, apikey=dd_apikey) as dd_ckan:
            with RemoteCKAN(odp_url, apikey=odp_apikey) as odp_ckan:
                start = 0
                rows = 1000
                while start < 3000:  # There is only 1120 records so this is used as a backup to exit loop
                    result = dd_ckan.action.package_search(fq='groups:spatial-data',q='urls:*order?email=:emailAddress&productId=*', start=start, rows=rows)
                    results = result.get('results', [])
                    # If no results exist while loop
                    if len(results) == 0:
                        break
                    for dataset in results:
                        try:
                            # TODO: Change package_delete to dataset_purge?
                            dd_ckan.action.dataset_purge(id=dataset.get('name'))
                            row = "Success,DD,{0},{1},{2}/dataset/{3},\n".format(dataset.get('title'),  dataset.get('name'), dd_url, dataset.get('name'))
                            log_file.write(row)
                        except Exception as ex:
                            row = "Failed,DD,{0},{1},{2}/dataset/{3},{4}\n".format(dataset.get('title'), dataset.get('name'), dd_url, dataset.get('name'), str(ex))
                            log_file.write(row)

                        try:
                            # TODO: Change package_delete to dataset_purge?
                            odp_ckan.action.dataset_purge(id=dataset.get('name'))
                            row = "Success,ODP,{0},{1},{2}/dataset/{3},\n".format(dataset.get('title'),  dataset.get('name'), odp_url, dataset.get('name'))
                            log_file.write(row)
                        except Exception as ex:
                            row = "Failure,ODP,{0},{1},{2}/dataset/{3},{4}\n".format(dataset.get('title'),  dataset.get('name'), odp_url, dataset.get('name'), str(ex))
                            log_file.write(row)

                    start = start+rows

    except Exception as ex:
        print(ex)
