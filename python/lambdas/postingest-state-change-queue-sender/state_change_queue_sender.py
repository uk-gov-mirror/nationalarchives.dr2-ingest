import json
import boto3
import os

sqs_client = boto3.client("sqs")


def lambda_handler(event, context):
    for record in event["Records"]:
        sqs_client.send_message(
            QueueUrl=os.environ["QUEUE_URL"],
            MessageBody=json.dumps(record),
        )
    return {}
