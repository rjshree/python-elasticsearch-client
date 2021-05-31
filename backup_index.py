import os, json
from elasticsearch import Elasticsearch
import time


list_of_indices = [('data-index', 'data.json'),
                   ('image-index', 'image.json'),
                   ('series-index', 'series.json'),
                   ('archive-index', 'archive.json')]

def change_index_mapping_with_data(es_url):
    '''
        @params es_url: Elastic Search Url (http://{Host}:{Port})
        

        A function to take back up of the index passed, change the mapping of
        the provided index and re-copy the data back to the new changed
        mapping of the passed index

    '''
    try:
        es = Elasticsearch([es_url],
                           use_ssl=False,
                           verify_certs=False)
        for source_index, mapping in list_of_indices:
            with open(mapping) as json_file:
                mapping = json.load(json_file)
            backup_index = source_index + "-backup"
            print(f"Deleting the backup already present in the name:{backup_index}")
            print(es.indices.delete(backup_index, ignore_unavailable=True))
            print("Creating backup index : {} with updated mapping \n{}".format(backup_index, mapping))
            print(es.indices.create(index=backup_index, body=mapping))
            print("Reindexing data from {} -> {}".format(source_index, backup_index))
            result = es.reindex({
                "source": {"index": source_index},
                "dest": {"index": backup_index}
            })
            print(result)
            time.sleep(5)
            print(f"Record count of {backup_index} is {es.count(index=backup_index, doc_type='_doc', body={'query': {'match_all': {}}})['count']}")

    except Exception as e:
        print("Exception Occurred : {}".format(e))


if __name__ == "__main__":
    '''
        Constants for Elastic URL, Source Index and New Mapping
    '''

    es_url = os.getenv("es_endpoint","http://elasticsearch.local-learning:9200")
    '''
        Calling the Function to take backup of
        the source index and change the mapping
        and copy the data back
    '''
    change_index_mapping_with_data(es_url)
