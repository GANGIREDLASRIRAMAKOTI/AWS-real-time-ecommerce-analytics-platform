import json
import boto3

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    start_job=glue_client.start_job_run(JobName='silver-orders-products-GJ')
    print("start_job:",start_job)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
