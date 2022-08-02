
import time
from datetime import datetime
import boto3
import json

region_name = "cn-northwest-1"
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/yunlidemo"
sqs_endpoint_url = "https://vpce-09bbb4c45ad28e950-odgopmjn-cn-northwest-1a.sqs.cn-northwest-1.vpce.amazonaws.com.cn"


sqs = boto3.client('sqs', region_name=region_name,
                   endpoint_url=sqs_endpoint_url)


def get_task():

    index = 0
    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            VisibilityTimeout=300
        )
        if "Messages" not in response:
            print(f"===> got {index}")
            break

        index += 1
        messages = response["Messages"]
        for msg in messages:
            body_str = msg["Body"]
            body = json.loads(body_str)
            receipt_handle = msg["ReceiptHandle"]
            yield body
            # sqs.delete_message(
            #     QueueUrl=sqs_url,
            #     ReceiptHandle=receipt_handle
            # )


while True:
    for task in get_task():
        print(task)

    time.sleep(0.1)
