import json
import boto3
import logging
import os
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

s3_control_client = boto3.client('s3control')
s3_cli = boto3.client('s3')


def handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    account_id = boto3.client('sts').get_caller_identity().get('Account')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    bucket_arn = event['Records'][0]['s3']['bucket']['arn']
    file_key = event['Records'][0]['s3']['object']['key']
    e_tag = event['Records'][0]['s3']['object']['eTag']
    logger.info('Reading {} from {}'.format(file_key, bucket_name))

    response = s3_control_client.create_job(
        AccountId=account_id,
        ConfirmationRequired=False,
        Operation={
            'S3PutObjectCopy': {
                'TargetResource': bucket_arn,
                'StorageClass': 'STANDARD',
                'TargetKeyPrefix': 'tmp_transition'
            },
        },
        Report={
            'Bucket': bucket_arn,
            'Format': 'Report_CSV_20180820',
            'Enabled': True,
            'Prefix': f'report/{os.getenv("SOURCE_BUCKET_NAME")}',
            'ReportScope': 'FailedTasksOnly'
        },
        Manifest={
            'Spec': {
                'Format': 'S3BatchOperations_CSV_20180820',
                "Fields": ["Bucket", "Key"]
            },
            'Location': {
                'ObjectArn': f'{bucket_arn}/{unquote(file_key)}',
                'ETag': e_tag
            }
        },
        Priority=10,
        RoleArn=os.getenv("ROLE_ARN"),
        Tags=[
            {
                'Key': 'engineer',
                'Value': 'yiai'
            },
        ]
    )

    logger.info("S3 barch job response: " + json.dumps(response, indent=2))
    return
