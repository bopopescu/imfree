import boto3

client = boto3.client('kinesis')

response = client.create_stream(
    StreamName='test-stream',
    ShardCount=1
)

print(response)
