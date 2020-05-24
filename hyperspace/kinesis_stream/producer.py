import boto3
import json
from datetime import datetime
import calendar
import random
import time

my_stream_name = 'test-stream'

client = boto3.client('kinesis', region_name='ap-southeast-1')


def put_to_stream(thing_id, property_value, property_timestamp):
    payload = {
                'age': str(property_value),
                'timestamp': str(property_timestamp),
                'thing_id': thing_id
              }

    print(payload)

    client.put_record(
                    StreamName=my_stream_name,
                    Data=json.dumps(payload),
                    PartitionKey=thing_id
                    )


while True:
    property_value = random.randint(1, 60)
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    thing_id = 'aa-bb'

    put_to_stream(thing_id, property_value, property_timestamp)
    time.sleep(5)
