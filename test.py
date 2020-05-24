import boto3
import json
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'ec2-52-76-170-222.ap-southeast-1.compute.amazonaws.com',
                     'port': '9200'}])


def consume(stream_name):
    client = boto3.client('kinesis', region_name='ap-southeast-1')
    response = client.describe_stream(StreamName=stream_name)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

    shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                               ShardId=my_shard_id,
                                               ShardIteratorType='LATEST')

    my_shard_iterator = shard_iterator['ShardIterator']
    record_response = client.get_records(ShardIterator=my_shard_iterator, Limit=2)

    while 'NextShardIterator' in record_response:
        record_response = client.get_records(ShardIterator=record_response['NextShardIterator'],
                                             Limit=2)

        if record_response['Records']:
            for v in record_response['Records']:
                v_data = v['Data']
                data = str(v_data).replace("b'", '')
                data = data.replace('\\', '')
                data = data.replace('\'', '')
                json_data = json.loads(data)
                print(json_data)

                try:
                    es.index(index='index-campaign', body=json_data)
                except Exception as e:
                    print('An unknown error occured connecting to ElasticSearch: %s' % e)

consume('imfree_campaign_stream')
