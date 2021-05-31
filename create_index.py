from elasticsearch import Elasticsearch

EDISON_AI_ES_CLUSTER_END_POINT = "10.177.197.62:31331"
es = Elasticsearch([EDISON_AI_ES_CLUSTER_END_POINT],
            use_ssl=False,
            verify_certs=False)
list_of_indices = [('eai-f1341a2c-7a54-4d68-9f40-a8b2d14d3806-datacollection-index', 'collection_index.json'),
                   ('eai-f1341a2c-7a54-4d68-9f40-a8b2d14d3806-image-index', 'image_index.json'),
                   ('eai-f1341a2c-7a54-4d68-9f40-a8b2d14d3806-imageseries-index', 'image_series_index.json'),
                   ('eai-f1341a2c-7a54-4d68-9f40-a8b2d14d3806-upload-archive-index', 'upload_index.json')]
for index in list_of_indices:
    try:
        mapping = {}
        with open(index[1]) as json_file:
            mapping = json.load(json_file)
        print(mapping)
        # ignore 400 cause by IndexAlreadyExistsException when creating an index
        resp = es.indices.create(index=index[0], body=mapping, ignore=400)
        print("Response", resp)
    except Exception as e:
        print('Exception in creating indices', e)
