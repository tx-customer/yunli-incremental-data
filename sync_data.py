import json
import time
import urllib.parse
import boto3

# 需要修改配置的地方===================================================
# 需要运维同学创建一个 SQS。 sqs的url地址。 和 s3_listener.py中的是同一个
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/yunlidemo"
# 临时表的s3地址
target_s3 = "s3://example-output/yunli/json_data/"
# athena 中，你的数据表放在那个db 下，那个目录下。在athena web 界面能看到
athena_data_ctx = {'Database': 'default', 'Catalog': 'AwsDataCatalog'}

# athena 的 log地址
athena_output_cfg = {
    'OutputLocation': 's3://aws-glue-assets-027040934161-cn-northwest-1/'}

# 目标表表名
target_table = "demo_prod_yunli_athena_tb"
# 临时表表名
source_temp_table = "demo_yunli_athena_tb3"
# athena 工作组
athena_work_group = 'primary'
# ===================================================================


def lambda_handler(event, context):

    print(event)
    keys = [record['body'] for record in event['Records']]
    parts = target_s3.split("/")

    target_bucket = parts[2]
    target_key = "/".join(parts[3:])
    if len(target_key) == 0:
        target_key = "/"

    s3 = boto3.client("s3")
    athena = boto3.client('athena')
    try:
        for key in keys:
            if not key.endswith(".gz"):
                print(key)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'status': 404,
                        'msg': 'file format not allowed!'
                    })
                }
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            items = ukey.split("/")
            file_name = items[-1]
            target_file = target_key + file_name

            print(target_file)
            response = s3.copy_object(
                CopySource=ukey,
                Bucket=target_bucket,
                Key=target_file
            )
            time.sleep(1)

            create_dt = "-".join(items[-4:-1])
            business_type = items[-5]

            sql = f"""
            insert into {target_table}
            select json_text, '{business_type}' as business_type, date '{create_dt}' as create_dt
            from {source_temp_table}
            """
            print(sql)
            execution = athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext=athena_data_ctx,
                ResultConfiguration=athena_output_cfg,
                WorkGroup=athena_work_group
            )
            while True:
                print("begin query")
                response = athena.get_query_execution(
                    QueryExecutionId=execution['QueryExecutionId'])
                status = response['QueryExecution']['Status']['State']

                if status == 'SUCCEEDED':
                    # 打印本次查询数据扫描量
                    print(float(response['QueryExecution']['Statistics']
                                ['DataScannedInBytes'])/1024/1024/1024)

                    break
                if status == 'FAILED':
                    print("failed to insert table")
                    break
                time.sleep(1)
            print(f"success sync {key}")
            # 删除已经同步过去的文件
            o = [{
                "Key": target_file
            }]
            print(f"begin to delete {o}")
            d = {
                'Objects': o,
                'Quiet': True
            }

            response = s3.delete_objects(
                Bucket=target_bucket,
                Delete=d
            )
            print(f"delete used file {response}")
    except Exception as ex:
        print(ex)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 400,
                'msg': ex
            })
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            'status': 200,
            'msg': 'ok'
        })
    }
