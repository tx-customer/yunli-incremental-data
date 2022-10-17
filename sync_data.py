from datetime import datetime
import json
import time
import urllib.parse
import boto3
import random


# 需要修改配置的地方===================================================

concurrency = 15

# 需要运维同学创建一个 SQS。 sqs的url地址。 和 s3_listener.py中的是同一个
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/yunlidemo"

# athena 中，你的数据表放在那个db 下，那个目录下。在athena web 界面能看到
athena_data_ctx = {'Database': 'default', 'Catalog': 'AwsDataCatalog'}

# athena 的 log地址
athena_output_cfg = {
    'OutputLocation': 's3://aws-glue-assets-027040934161-cn-northwest-1/'}

# 目标表表名
TARGET_TABLE = "yunli_athena_tbl"
TARGET_LOG_TABLE = "yunli_athena_tbl_log"


temp_table_s3_bucket = 'example-output'
temp_table_s3_prefix = 'yunli_temp'
temp_table_name = "temp_yunli_athena"


# 临时表字典
temp_tables = dict()
temp_tables_s3 = dict()


# athena 工作组
athena_work_group = 'primary'

dynamodb_status_table = 'yunli-monitor-dev'
dynamodb_status_table_key = 'keyetag'

bath_task_count = 1000
bath_wait_seconds = 15
# ===================================================================


def query_batch(athena, sqls: list, timeout=300) -> dict:
    '''
    一次发起 len(sqls) 个 athena 请求，并同时轮询几个请求的执行状态，所有都执行成功了
    再统一返回。比等待时间是几个sql中执行最长的sql的时间
    '''
    executions = dict()

    for sql in sqls:
        execution = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext=athena_data_ctx,
            ResultConfiguration=athena_output_cfg,
            WorkGroup=athena_work_group
        )
        executions[sql] = execution

    time.sleep(0.1)

    begin_time = datetime.now()
    errors = dict()

    while executions:

        item = executions.popitem()
        sql = item[0]
        execution = item[1]

        response = athena.get_query_execution(
            QueryExecutionId=execution['QueryExecutionId'])
        status = response['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            continue

        if status == 'FAILED':
            reason = response['QueryExecution']['Status']['StateChangeReason']
            errors[sql] = f"failed_execute_sql:[{sql}]  reason[{reason}]"

        else:
            executions[sql] = execution
            if (datetime.now() - begin_time).total_seconds() > timeout:
                break
            time.sleep(0.5)

    while executions:
        # 这里留下来的都是由于超时，没有处理完的查询，超时的需要手动取消，并且记录下来，供人工复核
        item = executions.popitem()
        sql = item[0]
        execution = item[1]
        athena.stop_query_execution(
            QueryExecutionId=execution['QueryExecutionId'])

        errors[sql] = f"failed_execute_sql: [{sql}]  reason[timeout: {timeout} seconds]"

    return errors


def setup():
    print("begin setup")
    # 生成临时表

    # 生成一个随机字符，作为临时表的后缀，防止如果跑多个程序，临时表被互相抢占
    chars = "abcdefghlgklmnopqrstuvwxyz"
    leng = random.randint(3, 8)
    begin = random.randint(0, 26-leng-1)
    random_word = chars[begin: begin+leng]
    print(f"temp table prefix: {random_word}")
    athena = boto3.client('athena')

    sqls = list()
    for i in range(1, concurrency+1):
        table_name = f"{temp_table_name}_{random_word}{i}"
        file_s3_prefix = f"{temp_table_s3_prefix}/{random_word}{i}"
        temp_tables[table_name] = True
        temp_tables_s3[table_name] = file_s3_prefix
        s3_path = f"s3://{temp_table_s3_bucket}/{file_s3_prefix}"

        ddl = f"""
                CREATE EXTERNAL TABLE `{table_name}`(
                `json_text` string COMMENT 'from deserializer')
                ROW FORMAT SERDE
                'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT
                'org.apache.hadoop.mapred.TextInputFormat'
                OUTPUTFORMAT
                'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                LOCATION
                '{s3_path}/'
                """
        sqls.append(ddl)

    errors = query_batch(athena, sqls)

    if errors:
        raise ValueError(errors)

    print(temp_tables)


def get_tasks(bach_count=1000, wait_seconds=30):
    sqs = boto3.client('sqs')

    msg_count = 0
    wait_unit_second = 0.01
    task_info = list()

    now = datetime.now()
    while True:
        response = sqs.receive_message(
            QueueUrl=sqs_url,
            VisibilityTimeout=60
        )

        if "Messages" in response:
            messages = response["Messages"]

            for msg in messages:
                body_str = msg["Body"]
                body = json.loads(body_str)
                receipt_handle = msg["ReceiptHandle"]
                sqs.delete_message(
                    QueueUrl=sqs_url,
                    ReceiptHandle=receipt_handle
                )
                task_info.append(body)
                msg_count += 1

        if msg_count > bach_count:
            break

        cost = (datetime.now() - now).total_seconds()
        if cost > wait_seconds:
            break

        time.sleep(wait_unit_second)

    return task_info


def get_business_type(log_project, logstore_name, default_type):
    business_type = default_type
    type_dict = {
        'k8s-log-c2ea45d230306486d9106bd53509cdc8d#audit-c2ea45d230306486d9106bd53509cdc8d': 'devops',
        'k8s-log-cbd351150c82840d4acdde97878e216e9#all-pod': 'devops',
        'k8s-log-c7732bb216d0b46948397aef06cb13a0d#all-pod': 'devops',
        'oss-log-1605435682286633-cn-hangzhou#oss-log-store': 'devops',
        'k8s-log-c6c91c06e8fd440ecbbe741a7697c26b2#audit-c6c91c06e8fd440ecbbe741a7697c26b2': 'devops',
        'operationaudit#actiontrail_operationaudit': 'devops',
        'k8s-log-cbd351150c82840d4acdde97878e216e9#audit-cbd351150c82840d4acdde97878e216e9': 'devops',
        'k8s-log-c2ea45d230306486d9106bd53509cdc8d#pod-log': 'devops',
        'k8s-log-c6c91c06e8fd440ecbbe741a7697c26b2#ingress': 'devops',
        'k8s-log-c7732bb216d0b46948397aef06cb13a0d#audit-c7732bb216d0b46948397aef06cb13a0d': 'devops',
        'taxware_log_center#qdclient_prod': 'qdclient_prod',
        'message-bus#apollo': 'message-bus',
        'janus-all#apollo': 'janus-all',
        'janus-all#gateway_ops': 'janus-all',
        'janus-all#gateway': 'janus-all',
        'janus-all#uia': 'janus-all',
        'janus-all#aided_api': 'janus-all',
        'janus-all#business_ops': 'janus-all',
        'janus-all#dataflow': 'janus-all'
    }
    key = log_project + '#' + logstore_name
    if key in type_dict:
        business_type = type_dict[key]
    return business_type


def pop_avalible_temp_table() -> str:
    empty = ''
    for tb in temp_tables:
        if temp_tables[tb] == True:
            empty = tb
            break

    if empty:
        temp_tables[empty] = False

    return empty


def release_temp_table(tb_name):
    temp_tables[tb_name] = True


def get_task_meta(file_key: str):
    items = file_key.split("/")
    file_name = items[-1]
    create_dt = "".join(items[-4:-1])
    log_project = items[-6]
    logstore_name = items[-5]
    business_type = get_business_type(
        log_project, logstore_name, logstore_name)

    if 'tcenter-prod' in business_type or 'usercenter-prod' in business_type:
        business_type = 'usercenter-prod'

    if 'slb_athena-seller-app_prod' in business_type:
        business_type = 'slb_prod'

    if '20_81_81-prd' in business_type:
        business_type = 'middleplatform_prod'

    if '2_21_p0121-phoenix-base-prod' in business_type or '2_21_p0121-phoenix-prod' in business_type:
        business_type = 'invoice_prod'

    group_key = f"{create_dt}/{log_project}/{logstore_name}/{business_type}"

    return group_key, {
        "file_name": file_name,
        "file_key": file_key
    }


def copy_goup_files(task_router: dict, group_key: str, temp_tb: str, s3) -> list:
    errors = list()

    metas = task_router[group_key]
    # s3 复制参数
    # CopySource 格式 bucket/prefix/prefix/.../filename, 不需要s3://
    # Key 格式 prefix/prefix/.../filename
    # Bucket 格式 bucket  ,不需要s3://

    temp_files = list()
    for meta in metas:
        file_name = meta['file_name']
        file_key = meta['file_key']
        target_file = f"{temp_tables_s3[temp_tb]}/{file_name}"
        response = s3.copy_object(
            CopySource=file_key,
            Bucket=temp_table_s3_bucket,
            Key=target_file
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            errors.append(file_key)
        else:
            temp_files.append(target_file)
    return temp_files, errors


def get_sync_sql(source_table: str, target_table: str, target_table_log: str, group_key: str) -> str:
    parts = group_key.split("/")
    create_dt = parts[0]
    log_project = parts[1]
    logstore_name = parts[2]
    business_type = parts[3]
    # 目标表的create_dt 的数据类型为字符串
    sql = f"""
                insert into {target_table}
                select json_text
                    , '{log_project}' as log_project
                    , '{logstore_name}' as logstore_name
                    , '{business_type}' as business_type
                    , '{create_dt}' as create_dt
                from {source_table}
                """

    # 中台/4.0进销项/税件客户端 的数据应用四层分区
    if 'middleplatform_prod' in business_type or 'invoice_prod' in business_type \
            or 'qdclient_prod' in business_type or 'devops' in business_type or 'janus-all' in business_type \
            or 'usercenter-prod' in business_type or 'message-bus' in business_type:
        sql = f"""
                    insert into {target_table_log}
                    select
                         json_extract_scalar(json_text, '$.__time__') as unix_time
                       , format_datetime(from_unixtime(cast(json_extract(json_text, '$.__time__') as bigint),'Asia/Shanghai'),'yyyy-MM-dd hh:mm:ss') as time
                       , coalesce(json_extract_scalar(json_text, '$._container_ip_'), json_extract_scalar(json_text, '$.client_ip'), '')  as ip
                       , json_text
                       ,  cast('{log_project}' as varchar) as log_project
                       ,  cast('{logstore_name}' as varchar) as logstore_name
                       ,  cast('{business_type}' as varchar) as business_type
                       , coalesce(format_datetime(from_unixtime(cast(json_extract(json_text, '$.__time__') as bigint),'Asia/Shanghai'),'yyyyMMdd'), '29991231')  as create_dt
                       , case when '{business_type}'='devops'
                                then cast('{logstore_name}' as varchar)
                              when json_extract_scalar(json_text, '$.service_name') is not null
                                then trim(json_extract_scalar(json_text, '$.service_name'))
                              when substr(json_extract_scalar(json_text, '$._container_name_'), length(json_extract_scalar(json_text, '$._container_name_'))-5,1)='-'
                                then substr(json_extract_scalar(json_text, '$._container_name_'),1, length(json_extract_scalar(json_text, '$._container_name_'))-6)
                              when json_extract_scalar(json_text, '$._container_name_') is null or trim(json_extract_scalar(json_text, '$._container_name_'))=''
                                then 'default'
                              else trim(json_extract_scalar(json_text, '$._container_name_')) end as service_name
                       , case when '{business_type}'='devops'
                                then  cast('{log_project}'  as varchar(7))
                              else cast(coalesce(trim(json_extract_scalar(json_text, '$.cid')), 'default') as varchar(7)) end as custom_col
                    from {source_table}
                         """

    return sql


def split_tasks(tasks: list):
    bad_tasks = list()
    task_router = dict()

    for task in tasks:
        key = task['file_key']
        verified_key = task['verified_key']
        if not key.endswith(".gz"):
            bad_tasks.append(key)
            continue

        ukey = urllib.parse.unquote_plus(key, encoding='utf-8')

        group_key, meta = get_task_meta(ukey)

        if group_key not in task_router:
            task_router[group_key] = list()

        task_router[group_key].append(meta)

    return task_router, bad_tasks


def update_file_status(dynamodb, file_key, status, error: str = '-'):
    dynamodb.update_item(
        TableName=dynamodb_status_table,
        Key={
            dynamodb_status_table_key: {'S': file_key},

        },
        AttributeUpdates={
            'status': {
                'Value':  {
                    "S": status
                }
            },
            'error': {
                'Value':  {
                    "S": error
                }
            }
        }
    )


def delete_s3_files(s3,  target_files: list):
    # target_file 格式 /bucket/prefix/prefix/.../filename

    # s3 delete 参数
    # Key 格式 prefix/prefix/.../filename
    # Bucket 格式 bucket  ,不需要s3://

    o = [{
        "Key": item
    } for item in target_files]
    d = {
        'Objects': o,
        'Quiet': True
    }

    response = s3.delete_objects(
        Bucket=temp_table_s3_bucket,
        Delete=d
    )


def main():
    setup()
    print("begin")
    while True:
        s3 = boto3.client("s3")
        athena = boto3.client('athena')
        dynamodb = boto3.client('dynamodb')

        # 一次获取批量的任务
        batch_tasks = get_tasks(bath_task_count, bath_wait_seconds)
        print(f"got============> {len(batch_tasks)} tasks")
        # 把一批任务根据不同的分区键，分成多个小批量任务
        task_router, bad_tasks = split_tasks(batch_tasks)
        # print(task_router)
        print(f"got============> {len(task_router)} task type")

        sqls = list()
        # 生成的sql 和 group key的隐射关系
        sql_group_key_map = dict()
        # 复制s3文件时，文件和错误直接的关系
        copy_errors = dict()
        # 这一个批次任务用到了那些临时表，以及放入临时表的文件，后续通过这个删除这些临时文件
        using_temp_table = dict()

        # 对子批量任务进行获取空闲临时表，复制文件，插入，记录状态，删除文件，上次文件同步状态，释放临时表
        for group_key in task_router:

            temp_tb = pop_avalible_temp_table()

            t_files, e = copy_goup_files(task_router, group_key, temp_tb, s3)
            for item in e:
                copy_errors[item] = f"failed to copy {item} to temp table: {temp_tb}"
            using_temp_table[temp_tb] = t_files

            sql = get_sync_sql(temp_tb, TARGET_TABLE,
                               TARGET_LOG_TABLE, group_key)

            sqls.append(sql)
            sql_group_key_map[sql] = group_key

        print("begin  query")
        query_errors = query_batch(athena, sqls)

        # 先将所有的文件状态改为成功
        all_files = [meta["file_key"]
                     for key in task_router for meta in task_router[key]]

        # 删除临时文件
        for temp_tp in using_temp_table:
            files = using_temp_table[temp_tp]
            delete_s3_files(s3, files)
            time.sleep(0.1)
            # 释放临时表
            release_temp_table(temp_tp)
            print(f"release temp table {temp_tp}")

        print("done delete files and release temp tables")

        for t_file_key in all_files:
            update_file_status(dynamodb, t_file_key, "success")

        # 在把复制失败的错误的文件状态改成失败
        for copy_error_file in copy_errors:
            update_file_status(dynamodb, copy_error_file,
                               "failure", copy_errors[copy_error_file])

        # 在把查询错误的文件状态改成失败
        if query_errors:
            for item in query_errors:
                error = query_errors[item]
                sql_group_key = sql_group_key_map[item]
                relative_files = [meta["file_key"]
                                  for meta in task_router[sql_group_key]]

                for file_key in relative_files:
                    update_file_status(dynamodb, file_key, "failure", error)

        time.sleep(0.5)


main()
