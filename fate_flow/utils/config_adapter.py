#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
from fate_flow.entity.types import RunParameters

# 定义JobRuntimeConfigAdapter类
# 被调：
# 被fate_flow.apps.job_app.py里面的submit_job、job_config函数所调用
# 被fate_flow.apps.model_app.py里面的load_model、bind_model_service、operate_model、deploy函数所调用
# 被fate_flow.apps.tracking_app.py里面的component_output_model函数所调用
# 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数所调用
# 被fate_flow.pipelined_model.migrate_model.py里面的migration函数所调用
# 被fate_flow.scheduler.dag_scheduler.py里面的submit函数所调用
# 被fate_flow.utils.schedule_utils.py里面的get_job_dsl_parser函数所调用
class JobRuntimeConfigAdapter(object):
    def __init__(self, job_runtime_conf):
        self.job_runtime_conf = job_runtime_conf


    # 获取公共参数
    # 被调：
    # 被fate_flow.apps.job_app.py里面的job_config函数所调用
    # 被fate_flow.apps.model_app.py里面的load_model、bind_model_service、operate_model函数所调用
    # 被fate_flow.apps.tracking_app.py里面的component_output_model函数所调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数所调用
    # 被fate_flow.scheduler.dag_scheduler.py里面的submit函数所调用
    def get_common_parameters(self):
        if int(self.job_runtime_conf.get('dsl_version', 1)) == 2:
            if "common" not in self.job_runtime_conf["job_parameters"]:
                raise RuntimeError("the configuration format for v2 version must be job_parameters:common")

            # 创建一个RunParameters对象实例
            job_parameters = RunParameters(**self.job_runtime_conf['job_parameters']['common'])
            # to_dict()方法返回非空的RunParameters类变量
            self.job_runtime_conf['job_parameters']['common'] = job_parameters.to_dict()

        else:
            if "processors_per_node" in self.job_runtime_conf['job_parameters']:
                self.job_runtime_conf['job_parameters']["eggroll_run"] = \
                    {"eggroll.session.processors.per.node": self.job_runtime_conf['job_parameters']["processors_per_node"]}

            # 创建一个RunParameters对象实例
            job_parameters = RunParameters(**self.job_runtime_conf['job_parameters'])
            # to_dict()方法返回非空的RunParameters类变量
            self.job_runtime_conf['job_parameters'] = job_parameters.to_dict()
        return job_parameters

    '''
    "dsl_version": 2,
    "initiator": {},
    "role": {},
    "job_parameters": {
        "common": {
            "job_type": "train",
            "backend": 0,
            "work_mode": 0
        }
    },
    '''

    '''
    "job_parameters": {
        "work_mode": 0
    },
    '''



    # 更新公共参数
    # 被调：
    # 被fate_flow.scheduler.dag_scheduler.py里面的submit函数所调用
    def update_common_parameters(self, common_parameters: RunParameters):
        if int(self.job_runtime_conf.get("dsl_version", 1)) == 2:
            if "common" not in self.job_runtime_conf["job_parameters"]:
                raise RuntimeError("the configuration format for v2 version must be job_parameters:common")
            self.job_runtime_conf["job_parameters"]["common"] = common_parameters.to_dict()
        else:
            self.job_runtime_conf["job_parameters"] = common_parameters.to_dict()
        return self.job_runtime_conf


    # 获取job参数字典
    def get_job_parameters_dict(self, job_parameters: RunParameters = None):
        if job_parameters:
            if int(self.job_runtime_conf.get('dsl_version', 1)) == 2:
                self.job_runtime_conf['job_parameters']['common'] = job_parameters.to_dict()
            else:
                self.job_runtime_conf['job_parameters'] = job_parameters.to_dict()
        return self.job_runtime_conf['job_parameters']


    # 获取job的工作模式
    # 被调：
    # 被fate_flow.apps.job_app.py里面的submit_job函数所调用
    # 被fate_flow.apps.model_app.py里面的operate_model、deploy函数所调用
    # 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数所调用
    def get_job_work_mode(self):
        if int(self.job_runtime_conf.get('dsl_version', 1)) == 2:
            work_mode = self.job_runtime_conf['job_parameters'].get('common', {}).get('work_mode')
            '''
            {
            "dsl_version": 2,
            "job_parameters": {
                "common": {
                    "job_type": "train",
                    "backend": 0,
                    "work_mode": 0
                }
            },
            '''
        else:
            work_mode = self.job_runtime_conf['job_parameters'].get('work_mode')
            '''
            "job_parameters": {
            "work_mode": 0
            },
            '''
        return work_mode


    # 获取job类型
    # 被调：
    # 被fate_flow.utils.schedule_utils.py里面的get_job_dsl_parser函数所调用
    def get_job_type(self):
        if int(self.job_runtime_conf.get('dsl_version', 1)) == 2:
            job_type = self.job_runtime_conf['job_parameters'].get('common', {}).get('job_type')
            if not job_type:
                job_type = self.job_runtime_conf['job_parameters'].get('job_type', 'train')
        else:
            job_type = self.job_runtime_conf['job_parameters'].get('job_type', 'train')
        return job_type


    # 更新对应模型id的版本
    # 被调：
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数所调用
    # 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数所调用
    def update_model_id_version(self, model_id=None, model_version=None):
        if int(self.job_runtime_conf.get('dsl_version', 1)) == 2:
            if model_id:
                self.job_runtime_conf['job_parameters'].get('common', {})['model_id'] = model_id
            if model_version:
                self.job_runtime_conf['job_parameters'].get('common', {})['model_version'] = model_version
        else:
            if model_id:
                self.job_runtime_conf['job_parameters']['model_id'] = model_id
            if model_version:
                self.job_runtime_conf['job_parameters']['model_version'] = model_version
        return self.job_runtime_conf






