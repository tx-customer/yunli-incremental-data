from tarfile import create_tar_file
from faker import Faker
from datetime import datetime
from random import random

fake = Faker()


template = {"__time__": "1654531777", "_container_ip_": "172.31.6.91", "_container_name_": "tenant-service",
            "_image_name_": "harbor.paas.xforceplus.com/xf-bm-usercenter/tenant-service:2022.6.3-090054-release-uc-2022-06-09",
            "_namespace_": "xf-bm-usercenter-fat", "_pod_name_": "tenant-service-5896fb4f9-fdrfz",
            "_pod_uid_": "3c496d83-8ece-46e4-9b09-594b5b4526ec",
            "_source_": "stdout", "_time_":
            "2022-06-06T16:09:37.032137033Z",
            "content": "Hibernate: "}


def _gen_data() -> dict:
    item = dict()
    item['__time__'] = datetime.now().timestamp(
    ) + fake.random_int(min=-10000, max=10000)
    item['_container_ip_'] = fake.ipv4()
    item['']


def gen(columns: dict, interval_min=1000, interval_max=3000, increment_id=''):
    """
    间隔随机毫秒数生成模拟订单数据
    :param columns: 数据包含的字段机器数据类型
    :param interval_min: 最小毫秒数
    :param interval_max: 最大毫秒数
    :return:
    """

    while True:
        # item["user_mail"] = fake.safe_email()
        item = _get_data(columns)
        if increment_id:
            now = datetime.now()
            item[increment_id] = int(now.timestamp() * 1000)

        yield item
        interval = fake.random_int(min=interval_min, max=interval_max) * 0.001
        time.sleep(interval)
