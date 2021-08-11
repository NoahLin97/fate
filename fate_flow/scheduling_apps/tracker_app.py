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

from flask import Flask, request

from fate_arch.common.base_utils import deserialize_b64
from fate_flow.operation.job_tracker import Tracker
from fate_flow.settings import stat_logger
from fate_flow.utils.api_utils import get_json_result
from fate_arch.common import log

manager = Flask(__name__)


@manager.errorhandler(500)
def internal_server_error(e):
    stat_logger.exception(e)
    return get_json_result(retcode=100, retmsg=log.exception_to_trace_string(e))

# 保存metric_data
# 被调用：
# fate_flow.scheduling_apps.client.tracker_client.TrackerClient的log_metric_data_common方法 以POST方式
@manager.route('/<job_id>/<component_name>/<task_id>/<task_version>/<role>/<party_id>/metric_data/save',
               methods=['POST'])
def save_metric_data(job_id, component_name, task_version, task_id, role, party_id):
    request_data = request.json
    #生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=job_id, component_name=component_name, task_id=task_id, task_version=task_version,
                      role=role, party_id=party_id)
    #调用fate_arch.common.base_utils模块的deserialize_b64方法解码metric
    metrics = [deserialize_b64(metric) for metric in request_data['metrics']]
    #通过tracker对象调用save_metric_data方法保存metric
    tracker.save_metric_data(metric_namespace=request_data['metric_namespace'], metric_name=request_data['metric_name'],
                             metrics=metrics, job_level=request_data['job_level'])
    return get_json_result()

# 保存metric_meta
# 被调用：
# fate_flow.scheduling_apps.client.tracker_client.TrackerClient的set_metric_meta_common方法 以POST方式
@manager.route('/<job_id>/<component_name>/<task_id>/<task_version>/<role>/<party_id>/metric_meta/save',
               methods=['POST'])
def save_metric_meta(job_id, component_name, task_version, task_id, role, party_id):
    request_data = request.json
    # 生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=job_id, component_name=component_name, task_id=task_id, task_version=task_version,
                      role=role, party_id=party_id)
    # 调用fate_arch.common.base_utils模块的deserialize_b64方法解码metric_meta
    metric_meta = deserialize_b64(request_data['metric_meta'])
    # 通过tracker对象调用save_metric_meta方法保存metric_meta
    tracker.save_metric_meta(metric_namespace=request_data['metric_namespace'], metric_name=request_data['metric_name'],
                             metric_meta=metric_meta, job_level=request_data['job_level'])
    return get_json_result()

# 保存输出数据信息
# 被调用：
# fate_flow.scheduling_apps.client.tracker_client.TrackerClient的log_output_data_info方法 以POST方式
@manager.route('/<job_id>/<component_name>/<task_id>/<task_version>/<role>/<party_id>/output_data_info/save',
               methods=['POST'])
def save_output_data_info(job_id, component_name, task_version, task_id, role, party_id):
    request_data = request.json
    # 生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=job_id, component_name=component_name, task_id=task_id, task_version=task_version,
                      role=role, party_id=party_id)
    #通过tracker对象调用insert_output_data_info_into_db方法保存输出数据信息到数据库
    tracker.insert_output_data_info_into_db(data_name=request_data["data_name"],
                                            table_namespace=request_data["table_namespace"],
                                            table_name=request_data["table_name"])
    return get_json_result()

# 读取输出数据信息
# 被调用：
# fate_flow.scheduling_apps.client.tracker_client.TrackerClient的get_output_data_info方法 以POST方式
@manager.route('/<job_id>/<component_name>/<task_id>/<task_version>/<role>/<party_id>/output_data_info/read',
               methods=['POST'])
def read_output_data_info(job_id, component_name, task_version, task_id, role, party_id):
    request_data = request.json
    #生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=job_id, component_name=component_name, task_id=task_id, task_version=task_version,
                      role=role, party_id=party_id)
    ##通过tracker对象调用read_output_data_info_from_db方法从数据库读取信息
    output_data_infos = tracker.read_output_data_info_from_db(data_name=request_data["data_name"])
    response_data = []
    for output_data_info in output_data_infos:
        response_data.append(output_data_info.to_human_model_dict())
    return get_json_result(data=response_data)


# 保存组件summary
# 被调用：
# fate_flow.scheduling_apps.client.tracker_client.TrackerClient的log_component_summary方法 以POST方式
@manager.route('/<job_id>/<component_name>/<task_id>/<task_version>/<role>/<party_id>/summary/save',
               methods=['POST'])
def save_component_summary(job_id: str, component_name: str, task_version: int, task_id: str, role: str, party_id: int):
    request_data = request.json
    #生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=job_id, component_name=component_name, task_id=task_id, task_version=task_version,
                      role=role, party_id=party_id)
    summary_data = request_data['summary']
    ##通过tracker对象调insert_summary_into_db方法将summary写入数据库
    tracker.insert_summary_into_db(summary_data)
    return get_json_result()

#查询组件输出数据table
@manager.route('/<job_id>/<component_name>/<role>/<party_id>/output/table', methods=['POST'])
def component_output_data_table(job_id, component_name, role, party_id):
    #调用fate_flow.operation.job_tracker模块的Tracker类的query_output_data_infos方法得到output_data_infos
    output_data_infos = Tracker.query_output_data_infos(job_id=job_id, component_name=component_name, role=role, party_id=party_id)
    if output_data_infos:
        return get_json_result(retcode=0, retmsg='success', data=[{'table_name': output_data_info.f_table_name,
                                                                  'table_namespace': output_data_info.f_table_namespace,
                                                                   "data_name": output_data_info.f_data_name
                                                                   } for output_data_info in output_data_infos])
    else:
        return get_json_result(retcode=100, retmsg='No found table, please check if the parameters are correct')
