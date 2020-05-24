from datetime import datetime
from hyperspace.elastic_search.func.es_func import create_index
from elasticsearch import Elasticsearch

es = Elasticsearch()

e1 = {
    "first_name":"nitin",
    "last_name":"panwar",
    "age": 27,
    "about": "Love to play cricket",
    "interests": ['sports','music'],
}

res = es.search(index='sample', body={'query': {}})
print(res)
