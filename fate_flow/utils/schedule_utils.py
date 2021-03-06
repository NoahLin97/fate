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
import os

from fate_arch.common import file_utils
from fate_flow.db.db_models import DB, Job
from fate_flow.scheduler.dsl_parser import DSLParser, DSLParserV2
from fate_flow.utils.config_adapter import JobRuntimeConfigAdapter


# 根据job_id从数据库中读取job的dsl解析器
@DB.connection_context()
def get_job_dsl_parser_by_job_id(job_id):
    # 在t_job表中，从数据库中搜索f_job_id == job_id 对应的f_dsl、f_runtime_conf_on_party、f_train_runtime_conf
    jobs = Job.select(Job.f_dsl, Job.f_runtime_conf_on_party, Job.f_train_runtime_conf).where(Job.f_job_id == job_id)
    if jobs:
        job = jobs[0]
        # 得到对应job_id的dsl解析器
        job_dsl_parser = get_job_dsl_parser(dsl=job.f_dsl, runtime_conf=job.f_runtime_conf_on_party,
                                            train_runtime_conf=job.f_train_runtime_conf)
        return job_dsl_parser
    else:
        return None


# 得到对应job_id的dsl解析器
def get_job_dsl_parser(dsl=None, runtime_conf=None, pipeline_dsl=None, train_runtime_conf=None):

    parser_version = str(runtime_conf.get('dsl_version', '1'))
    # 通过版本号得到dsl的解析器实例，得到的是DSLParser()或DSLParserV2()
    dsl_parser = get_dsl_parser_by_version(parser_version)

    # /data/projects/fate/python/federatedml/conf/default_runtime_conf
    default_runtime_conf_path = os.path.join(file_utils.get_python_base_directory(),
                                             *['federatedml', 'conf', 'default_runtime_conf'])

    # /data/projects/fate/python/federatedml/conf/setting_conf
    setting_conf_path = os.path.join(file_utils.get_python_base_directory(), *['federatedml', 'conf', 'setting_conf'])

    # 从fate_flow.utils.config_adapter中调用JobRuntimeConfigAdapter模块的get_job_type函数
    # 得到job的类型
    job_type = JobRuntimeConfigAdapter(runtime_conf).get_job_type()

    # 从fate_flow.scheduler.dsl_parser中调用DSLParser模块的run函数
    # 运行得到dsl解析器的实例
    dsl_parser.run(dsl=dsl,
                   runtime_conf=runtime_conf,
                   pipeline_dsl=pipeline_dsl,
                   pipeline_runtime_conf=train_runtime_conf,
                   default_runtime_conf_prefix=default_runtime_conf_path,
                   setting_conf_prefix=setting_conf_path,
                   mode=job_type)
    return dsl_parser


# 重置联邦任务的调度顺序
def federated_order_reset(dest_partys, scheduler_partys_info):
    dest_partys_new = []
    scheduler = []
    dest_party_ids_dict = {}
    for dest_role, dest_party_ids in dest_partys:
        from copy import deepcopy
        new_dest_party_ids = deepcopy(dest_party_ids)
        dest_party_ids_dict[dest_role] = new_dest_party_ids
        for scheduler_role, scheduler_party_id in scheduler_partys_info:
            if dest_role == scheduler_role and scheduler_party_id in dest_party_ids:
                dest_party_ids_dict[dest_role].remove(scheduler_party_id)
                scheduler.append((scheduler_role, [scheduler_party_id]))
        if dest_party_ids_dict[dest_role]:
            dest_partys_new.append((dest_role, dest_party_ids_dict[dest_role]))
    if scheduler:
        dest_partys_new.extend(scheduler)
    return dest_partys_new


# 获取对应版本的dsl解析器映射，返回dsl解析器类
def get_parser_version_mapping():
    return {
        # 从fate_flow.scheduler.dsl_parser导入DSLParser函数
        # 初始化DSLParser类
        "1": DSLParser(),
        # 从fate_flow.scheduler.dsl_parser导入DSLParserV2函数
        # 初始化DSLParserV2类
        "2": DSLParserV2()
    }

# 通过版本号得到dsl的解析器，默认为1
def get_dsl_parser_by_version(version: str = "1"):
    # 获取对应版本的dsl解析器映射，返回dsl解析器类
    mapping = get_parser_version_mapping()
    if isinstance(version, int):
        version = str(version)
    if version not in mapping:
        raise Exception("{} version of dsl parser is not currently supported.".format(version))
    return mapping[version]
