from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])


def create_index(index_name):
    es.indices.create(index=index_name, ignore=400)


def insert_es(index_name, doc_type, id_num, body):
    es.index(index=index_name, doc_type=doc_type, id=id_num, body=body)


def get_es(index_name, doc_type, id_num):
    val = es.get(index=index_name, doc_type=doc_type, id=id_num)

    return val['_source']


def delete_es(index_name, doc_type, id_num):
    es.delete(index=index_name, doc_type=doc_type, id=id_num)
