import boto3
import json

s3 = boto3.client('s3')
kinesis = boto3.client('kinesis', region_name='ap-southeast-2')
STREAM = "ecommerce-clickstream-stream"

def lambda_handler(event, context):
    # Get source S3 bucket and file key from event
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(f"New file received from RAW layer: {key}")

    # Read CSV file from S3 Raw bucket
    obj = s3.get_object(Bucket=raw_bucket, Key=key)
    lines = obj['Body'].read().decode('utf-8').splitlines()
    header = lines[0].split(',')

    batch = []
    count = 0
    for line in lines[1:]:
        # Convert CSV row to JSON
        row = dict(zip(header, line.split(',')))
        row['_source'] = key
        batch.append({'Data': json.dumps(row)+"\n", 'PartitionKey': key})

        # Send in batch of 100 for high throughput
        if len(batch) == 100:
            kinesis.put_records(StreamName=STREAM, Records=batch)
            count += len(batch)
            batch = []

    # Send remaining records
    if batch:
        kinesis.put_records(StreamName=STREAM, Records=batch)
        count += len(batch)

    print(f"Successfully sent {count} records to Kinesis stream")
    return "DONE"


