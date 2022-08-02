# -*- coding: utf-8 -*-
import logging
import json
import boto3
import oss2

#################这里要根据你的情况修改##############
oss_user_key_id = 'key_id'
oss_user_secret_key = 'secret_key'
oss_source_bucket = 'data-zip'

s3_user_key_id = 'key_id'
s3_user_secret_key = 'secret_key'
s3_target_bucket = 'example-data'
#################################################


def handler(event, context):
    # evt = json.loads(event)
    logger = logging.getLogger()
    event_data = event.decode('utf8')
    event_object = json.loads(event_data)

    events = event_object['events']

    auth = oss2.Auth(oss_user_key_id, oss_user_secret_key)

    endpoint = 'http://oss-cn-shanghai-internal.aliyuncs.com'
    bucket = oss2.Bucket(auth, endpoint, oss_source_bucket)
    s3 = boto3.client('s3',
                      region_name='cn-northwest-1',
                      aws_access_key_id=s3_user_key_id,
                      aws_secret_access_key=s3_user_secret_key)
    for evt in events:
        if 'oss' not in evt:
            continue

        oss_tick = evt['oss']

        if 'object' not in oss_tick:
            continue

        o = oss_tick['object']
        object_key = o['key']
        object_stream = bucket.get_object(object_key)
        data = object_stream.read()
        s3.put_object(
            Body=data,
            Bucket=s3_target_bucket,
            Key=object_key,
        )

    logger.info(f"success to sync file from oss to s3: {object_key}")
    return 'hello world'
