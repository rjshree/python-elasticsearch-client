import os, json
from elasticsearch import Elasticsearch, helpers
ES_CLUSTER_END_POINT = "http://elasticsearch.local-learning:9200"
print(f"ES_CLUSTER_END_POINT {ES_CLUSTER_END_POINT}")
source_index = 'image-index'
es = Elasticsearch([ES_CLUSTER_END_POINT],
            use_ssl=False,
            verify_certs=False, # in order to not verify cert and SSL
            timeout=30)
print(f"Total Record count of {source_index} is {es.count(index=source_index, doc_type='_doc', body={'query': {'match_all': {}}})['count']}")
print(f"Record count of query {source_index} is {es.count(index=source_index, doc_type='_doc', body={ 'query': {'bool': {'must': [{'match':{'objectState': 'AVAILABLE'}},{'match':{'batchId': '3a5dec89-4486-48fc-a65e-68b05d1f70a0'}}]}}})['count']}")
# helpers also scan the index
res=helpers.scan(es,index=source_index,query={ 'query': {'bool': {'must': [{'match':{'objectState': 'AVAILABLE'}},{'match':{'batchId': '3a5dec89-4486-48fc-a65e-68b05d1f70a0'}}]}}})
for items in res:
    print(items['_source']['patientId'],items['_source']['objectId'],items['_source']['objectState'],items['_source']['key'])
