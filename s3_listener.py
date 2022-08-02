import json
import urllib.parse
import boto3

# 需要修改配置的地方============================================================
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/yunlidemo"
# ============================================================================


def lambda_handler(event, context):
    print("Received event: " + str(event))
    keys = ["/" + record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]

    sqs = boto3.client("sqs")

    for key in keys:
        ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
        response = sqs.send_message(
            QueueUrl=sqs_url,
            MessageBody=ukey,
            DelaySeconds=0)
        print(response)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
