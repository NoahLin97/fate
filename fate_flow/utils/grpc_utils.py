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
import requests

from fate_arch.common.log import audit_logger
from fate_flow.utils.proto_compatibility import basic_meta_pb2
from fate_flow.utils.proto_compatibility import proxy_pb2, proxy_pb2_grpc
import grpc

from fate_flow.settings import FATEFLOW_SERVICE_NAME, IP, GRPC_PORT, HEADERS, DEFAULT_REMOTE_REQUEST_TIMEOUT
from fate_flow.entity.runtime_config import RuntimeConfig
from fate_flow.utils.node_check_utils import nodes_check
from fate_arch.common.base_utils import json_dumps, json_loads

# 资料：
# [gRPC 官方文档中文版](https://doc.oschina.net/grpc?t=60138)
# [gRPC Github](https://github.com/grpc/grpc/tree/master/examples/python)
# [python grpc quickstart](https://grpc.io/docs/languages/python/quickstart/)
# [python grpc basic](https://grpc.io/docs/languages/python/basics/)
# [python stub_gRPC之python应用](https://blog.csdn.net/weixin_39936086/article/details/110207269)


# 获得进行联邦任务的channel和stub
# 被调：
# 被fate_flow.utils.api_utils.py里面的federated_coordination_on_grpc、proxy_api函数所调用
def get_command_federation_channel(host, port):
    # 要调用服务方法，我们首先需要创建一个存根
    # 我们实例化proxy_pb2_grpc模块的DataTransferServiceStub类，该模块由我们的.proto生成

    # 一个服务肯定是有调用方和被调用方的,被调用方被称为服务端,调用方叫做客户端
    # grpc中给服务端取名叫做grpc server，但是客户端不叫grpc client叫做grpc stub，
    # 作用就是起到了客户端的作用，用来发起proto请求和接收proto响应。一句话来说grpc和普通的http请求一样都是cs模型，grpc stub相当于http请求中的client

    # Stub存根：
    # 为屏蔽客户调用远程主机上的对象，必须提供某种方式来模拟本地对象,这种本地对象称为存根(stub),存根负责接收本地方法调用,并将它们委派给各自的具体实现对象

    # gRPC基于HTTP/2，gRPC的Channel利用了流的机制。Channel是一个虚拟的连接，它其实对应了多个连接以及多个流。RPC调用实际上就是HTTP/2的流，
    # 通过其中一个连接传递，RPC调用的消息以帧的形式发送，根据消息的大小，即有可能多个消息公用一个帧，也有可能一个消息分割成多个帧
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = proxy_pb2_grpc.DataTransferServiceStub(channel)
    return channel, stub


# 生成路由元数据
# 被调：
# 被fate_flow.tests.grpc.client.py里面的remote_api函数所调用
# 被fate_flow.utils.api_utils.py里面的federated_coordination_on_grpc、proxy_api函数所调用
# 被fate_flow.utils.grpc_utils.py里面的unaryCall函数所调用
def gen_routing_metadata(src_party_id, dest_party_id):
    routing_head = (
        ("service", "fateflow"),
        ("src-party-id", str(src_party_id)),
        ("src-role", "guest"),
        ("dest-party-id", str(dest_party_id)),
        ("dest-role", "host"),
    )
    return routing_head


# 包装grpc数据包
# 被调：
# 被fate_flow.tests.grpc.client.py里面的remote_api函数所调用
# 被fate_flow.utils.api_utils.py里面的federated_coordination_on_grpc函数所调用
# 被fate_flow.utils.grpc_utils.py里面的unaryCall函数所调用
def wrap_grpc_packet(json_body, http_method, url, src_party_id, dst_party_id, job_id=None, overall_timeout=DEFAULT_REMOTE_REQUEST_TIMEOUT):
    _src_end_point = basic_meta_pb2.Endpoint(ip=IP, port=GRPC_PORT)
    _src = proxy_pb2.Topic(name=job_id, partyId="{}".format(src_party_id), role=FATEFLOW_SERVICE_NAME, callback=_src_end_point)
    _dst = proxy_pb2.Topic(name=job_id, partyId="{}".format(dst_party_id), role=FATEFLOW_SERVICE_NAME, callback=None)
    _task = proxy_pb2.Task(taskId=job_id)
    _command = proxy_pb2.Command(name=FATEFLOW_SERVICE_NAME)
    _conf = proxy_pb2.Conf(overallTimeout=overall_timeout)
    # 生成元数据作为报头
    _meta = proxy_pb2.Metadata(src=_src, dst=_dst, task=_task, command=_command, operator=http_method, conf=_conf)
    _data = proxy_pb2.Data(key=url, value=bytes(json_dumps(json_body), 'utf-8'))
    return proxy_pb2.Packet(header=_meta, body=_data)


# 获取url地址
# 被调：
# 被fate_flow.utils.grpc_utils.py里面的unaryCall函数所调用
def get_url(_suffix):
    return "http://{}:{}/{}".format(RuntimeConfig.JOB_SERVER_HOST, RuntimeConfig.HTTP_PORT, _suffix.lstrip('/'))


# 定义一元服务类,继承自proxy_pb2_grpc.DataTransferServiceServicer
# 被调：
# 被fate_flow.fate_flow_server.py所调用
class UnaryService(proxy_pb2_grpc.DataTransferServiceServicer):
    # 资料：
    # [Grpc协议消息](https://www.jianshu.com/p/8f5f5bd47a5c)

    # 在客户端发起调用
    # 被调：
    # 被fate_arch.protobuf.python.fate_proxy_pb2_grpc.py里面的add_DataTransferServiceServicer_to_server函数所调用
    # 被fate_flow.tests.grpc.client.py里面的remote_api函数所调用
    # 被fate_flow.utils.api_utils.py里面的federated_coordination_on_grpc、proxy_api函数所调用
    def unaryCall(self, _request, context):
        packet = _request
        header = packet.header
        # suffix 后缀
        _suffix = packet.body.key
        param_bytes = packet.body.value
        param = bytes.decode(param_bytes)
        job_id = header.task.taskId
        src = header.src
        dst = header.dst
        method = header.operator
        param_dict = json_loads(param)
        param_dict['src_party_id'] = str(src.partyId)
        source_routing_header = []
        for key, value in context.invocation_metadata():
            source_routing_header.append((key, value))
        # 生成路由元数据
        _routing_metadata = gen_routing_metadata(src_party_id=src.partyId, dest_party_id=dst.partyId)
        context.set_trailing_metadata(trailing_metadata=_routing_metadata)
        try:
            # 检查节点
            nodes_check(param_dict.get('src_party_id'), param_dict.get('_src_role'), param_dict.get('appKey'),
                        param_dict.get('appSecret'), str(dst.partyId))
        except Exception as e:
            resp_json = {
                "retcode": 100,
                "retmsg": str(e)
            }
            # 返回包装后的grpc数据包
            return wrap_grpc_packet(resp_json, method, _suffix, dst.partyId, src.partyId, job_id)
        param = bytes.decode(bytes(json_dumps(param_dict), 'utf-8'))

        action = getattr(requests, method.lower(), None)
        audit_logger(job_id).info('rpc receive: {}'.format(packet))
        if action:
            audit_logger(job_id).info("rpc receive: {} {}".format(get_url(_suffix), param))
            resp = action(url=get_url(_suffix), data=param, headers=HEADERS)
        else:
            pass
        resp_json = resp.json()
        return wrap_grpc_packet(resp_json, method, _suffix, dst.partyId, src.partyId, job_id)


# 转发grpc数据包
# 被调：
# 被fate_flow.utils.api_utils.py里面的proxy_api函数所调用
def forward_grpc_packet(_json_body, _method, _url, _src_party_id, _dst_party_id, role, job_id=None,
                        overall_timeout=DEFAULT_REMOTE_REQUEST_TIMEOUT):
    _src_end_point = basic_meta_pb2.Endpoint(ip=IP, port=GRPC_PORT)
    _src = proxy_pb2.Topic(name=job_id, partyId="{}".format(_src_party_id), role=FATEFLOW_SERVICE_NAME, callback=_src_end_point)
    _dst = proxy_pb2.Topic(name=job_id, partyId="{}".format(_dst_party_id), role=role, callback=None)
    _task = proxy_pb2.Task(taskId=job_id)
    _command = proxy_pb2.Command(name=FATEFLOW_SERVICE_NAME)
    _conf = proxy_pb2.Conf(overallTimeout=overall_timeout)
    # 生成元数据作为报头
    _meta = proxy_pb2.Metadata(src=_src, dst=_dst, task=_task, command=_command, operator=_method, conf=_conf)
    _data = proxy_pb2.Data(key=_url, value=bytes(json_dumps(_json_body), 'utf-8'))
    return proxy_pb2.Packet(header=_meta, body=_data)