
import boto3
import time


sql1 = """
insert into debug
values ('abc','shanghai','man')
"""

sql2 = """
select count(*) from user_event
"""

executions = list()

athena_data_ctx = {'Database': 'default', 'Catalog': 'AwsDataCatalog'}

# athena 的 log地址
athena_output_cfg = {
    'OutputLocation': 's3://aws-glue-assets-027040934161-cn-northwest-1/'}

athena_work_group = 'primary'


athena = boto3.client('athena')
index = 0
for i in range(0, 50):
    sql = sql2 if i > 10 else sql1
    execution = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext=athena_data_ctx,
        ResultConfiguration=athena_output_cfg,
        WorkGroup=athena_work_group
    )
    print(i)
    executions.append(execution)

while True:
    for execution in executions:
        response = athena.get_query_execution(
            QueryExecutionId=execution['QueryExecutionId'])
        status = response['QueryExecution']['Status']['State']

        if status == 'SUCCEEDED':
            # 打印本次查询数据扫描量
            print(float(response['QueryExecution']['Statistics']
                        ['DataScannedInBytes'])/1024/1024/1024)

            query_result = True
            executions.remove(execution)
            break
        if status == 'FAILED':
            print(f"==>failed to execute sql:{sql}")
            executions.remove(execution)
            break

    time.sleep(2)
