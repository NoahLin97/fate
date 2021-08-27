#
#  Copyright 2019 The FATE Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from urllib import parse

from kazoo.client import KazooClient
from kazoo.security import make_digest_acl

from fate_arch.common import conf_utils
from fate_arch.common.conf_utils import get_base_config
from fate_flow.settings import FATE_FLOW_MODEL_TRANSFER_ENDPOINT, IP, HTTP_PORT, FATEFLOW_SERVICE_NAME
from fate_flow.settings import stat_logger, SERVICES_SUPPORT_REGISTRY, FATE_SERVICES_REGISTERED_PATH


# 定义ServiceUtils类
class ServiceUtils(object):
    ZOOKEEPER_CLIENT = None


    # 获取service_name对应的值url
    @classmethod
    def get(cls, service_name, default=None):
        # 获得基础的config
        # SERVICES_SUPPORT_REGISTRY = ["servings", "fateflow"]
        if get_base_config("use_registry", False) and service_name in SERVICES_SUPPORT_REGISTRY:
            # 从本地仓库中获取基础的service_name对应的值
            return ServiceUtils.get_from_registry(service_name)
        # 从文件中获取基础的service_name对应的值
        return ServiceUtils.get_from_file(service_name, default)


    # 获取对应service_name中key对应的值
    @classmethod
    def get_item(cls, service_name, key, default=None):
        return ServiceUtils.get(service_name, {}).get(key, default)


    # 从文件中获取基础的service_name对应的值
    @classmethod
    def get_from_file(cls, service_name, default=None):
        # 获得基础的config，从"service_conf.yaml"文件中找到service_name对应的值
        return conf_utils.get_base_config(service_name, default)


    # 获取zookeeper的相关配置
    @classmethod
    def get_zk(cls, ):
        # ZooKeeper是一种集中式服务，用于维护配置信息，命名，提供分布式同步和提供组服务。
        # 所有这些类型的服务都以分布式应用程序的某种形式使用。每次实施它们都需要做很多工作来修复不可避免的错误和竞争条件。
        # 由于难以实现这些类型的服务，应用程序最初通常会吝啬它们，这使得它们在变化的情况下变得脆弱并且难以管理。
        # 即使正确完成，这些服务的不同实现也会在部署应用程序时导致管理复杂性。

        # 获得基础的config，从"service_conf.yaml"文件中找到zookeeper对应的值
        zk_config = get_base_config("zookeeper", {})

        if zk_config.get("use_acl", False):
            # acl 访问控制列表
            default_acl = make_digest_acl(zk_config.get("user", ""), zk_config.get("password", ""), all=True)

            zk = KazooClient(hosts=zk_config.get("hosts", []), default_acl=[default_acl], auth_data=[("digest", "{}:{}".format(
                zk_config.get("user", ""), zk_config.get("password", "")))])
        else:
            zk = KazooClient(hosts=zk_config.get("hosts", []))
        return zk


    # 从本地仓库中获取基础的service_name对应的值
    @classmethod
    def get_from_registry(cls, service_name):
        try:
            # 获取zookeeper的相关配置
            zk = ServiceUtils.get_zk()
            zk.start()
            '''
            FATE_SERVICES_REGISTERED_PATH = {
                "fateflow": "/FATE-SERVICES/flow/online/transfer/providers",
                "servings": "/FATE-SERVICES/serving/online/publishLoad/providers",
            }
            '''
            nodes = zk.get_children(FATE_SERVICES_REGISTERED_PATH.get(service_name, ""))
            # url的解码
            services = nodes_unquote(nodes)
            zk.stop()
            return services
        except Exception as e:
            raise Exception('loading servings node  failed from zookeeper: {}'.format(e))


    # 登记模型迁移服务记录
    @classmethod
    def register(cls):
        # 获得基础的config，从"service_conf.yaml"文件中找到use_registry对应的值
        if get_base_config("use_registry", False):
            # 获取zookeeper的相关配置
            zk = ServiceUtils.get_zk()
            zk.start()

            # FATE_FLOW_MODEL_TRANSFER_ENDPOINT = "/v1/model/transfer"
            model_transfer_url = 'http://{}:{}{}'.format(IP, HTTP_PORT, FATE_FLOW_MODEL_TRANSFER_ENDPOINT)

            '''
            FATE_SERVICES_REGISTERED_PATH = {
                "fateflow": "/FATE-SERVICES/flow/online/transfer/providers",
                "servings": "/FATE-SERVICES/serving/online/publishLoad/providers",
            }
            '''
            # FATEFLOW_SERVICE_NAME = "fateflow"
            fate_flow_model_transfer_service = '{}/{}'.format(FATE_SERVICES_REGISTERED_PATH.get(FATEFLOW_SERVICE_NAME, ""), parse.quote(model_transfer_url, safe=' '))
            try:
                zk.create(fate_flow_model_transfer_service, makepath=True, ephemeral=True)
                stat_logger.info("register path {} to {}".format(fate_flow_model_transfer_service, ";".join(get_base_config("zookeeper", {}).get("hosts"))))
            except Exception as e:
                stat_logger.exception(e)


# url的解码
def nodes_unquote(nodes):
    # 编码工作使用urllib的parse.urlencode()函数，帮我们将key:value这样的键值对转换成"key=value"这样的字符串，解码工作可以使用urllib的parse.unquote()函数
    urls = [parse.unquote(node) for node in nodes]
    servers = []
    for url in urls:
        try:
            servers.append(url.split('/')[2])
        except Exception as e:
            stat_logger.exception(e)
    return servers