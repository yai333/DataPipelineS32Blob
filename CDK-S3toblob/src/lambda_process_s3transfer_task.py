import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ecs = boto3.client('ecs')


def handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))
    logger.info("ENV SUBNETS: " + json.dumps(os.getenv('SUBNETS'), indent=3))

    response = ecs.run_task(
        cluster=os.getenv("CLUSTER_NAME"),
        taskDefinition=os.getenv("TASK_DEFINITION"),
        launchType='FARGATE',
        count=1,
        platformVersion='LATEST',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [
                        os.getenv("PRIVATE_SUBNET1"),
                        os.getenv("PRIVATE_SUBNET2"),
                ],
                'assignPublicIp': 'DISABLED'
            }
        },
        overrides={"containerOverrides": [{
            "name": "azcopy",
            'memory': 512,
            'memoryReservation': 512,
            'cpu': 2,
            'environment': [
                    {
                        'name': 'S3_SOURCE',
                        'value': f'https://s3.{os.getenv("AWS_REGION")}.amazonaws.com/{os.getenv("S3_BUCKET_NAME")}/tmp_transition'
                    }
            ],
        }]})
    return str(response)
