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
from typing import List

from fate_arch.common import log
from fate_arch.common.base_utils import serialize_b64
from fate_flow.entity.types import RetCode, RunParameters
from fate_flow.entity.metric import Metric, MetricMeta
from fate_flow.operation.job_tracker import Tracker
from fate_flow.utils import api_utils

LOGGER = log.getLogger()


# TrackerClient类，用于调用scheduling_app.tracker_app的部分接口
class TrackerClient(object):
    def __init__(self, job_id: str, role: str, party_id: int,
                 model_id: str = None,
                 model_version: str = None,
                 component_name: str = None,
                 component_module_name: str = None,
                 task_id: str = None,
                 task_version: int = None,
                 job_parameters: RunParameters = None
                 ):
        self.job_id = job_id
        self.role = role
        self.party_id = party_id
        self.model_id = model_id
        self.model_version = model_version
        self.component_name = component_name if component_name else 'pipeline'
        self.module_name = component_module_name if component_module_name else 'Pipeline'
        self.task_id = task_id
        self.task_version = task_version
        self.job_parameters = job_parameters
        # 生成fate_flow.operation.job_tracker模块的Tracker类的对象
        self.job_tracker = Tracker(job_id=job_id, role=role, party_id=party_id, component_name=component_name,
                                   task_id=task_id,
                                   task_version=task_version,
                                   model_id=model_id,
                                   model_version=model_version,
                                   job_parameters=job_parameters)

    # 未被调用
    def log_job_metric_data(self, metric_namespace: str, metric_name: str, metrics: List[Metric]):
        self.log_metric_data_common(metric_namespace=metric_namespace, metric_name=metric_name, metrics=metrics,
                                    job_level=True)

    # 被调用：
    # fate_flow.components.download.Download的callback_metric方法
    # fate_flow.components.download.Upload的save_meta方法
    # federatedml.evaluation.evaluation.Evaluation的__save_single_value和__save_curve_data方法
    # federatedml.feature.sampler的callback方法
    # federatedml.model_base的callback_metric方法
    def log_metric_data(self, metric_namespace: str, metric_name: str, metrics: List[Metric]):
        self.log_metric_data_common(metric_namespace=metric_namespace, metric_name=metric_name, metrics=metrics,
                                    job_level=False)

    # 保存metric_data，被log_job_metric_data和log_metric_data方法调用
    def log_metric_data_common(self, metric_namespace: str, metric_name: str, metrics: List[Metric], job_level=False):
        LOGGER.info("Request save job {} task {} {} on {} {} metric {} {} data".format(self.job_id,
                                                                                       self.task_id,
                                                                                       self.task_version,
                                                                                       self.role,
                                                                                       self.party_id,
                                                                                       metric_namespace,
                                                                                       metric_name))
        request_body = {}
        request_body['metric_namespace'] = metric_namespace
        request_body['metric_name'] = metric_name
        request_body['metrics'] = [serialize_b64(metric, to_str=True) for metric in metrics]
        request_body['job_level'] = job_level
        # fate_flow.utils.api_utils模块的local_api方法
        # 调用fate_flow.scheduling_apps.tracker_app模块的save_metric_data接口来保存metric_data
        response = api_utils.local_api(job_id=self.job_id,
                                       method='POST',
                                       endpoint='/tracker/{}/{}/{}/{}/{}/{}/metric_data/save'.format(
                                           self.job_id,
                                           self.component_name,
                                           self.task_id,
                                           self.task_version,
                                           self.role,
                                           self.party_id),
                                       json_body=request_body)
        return response['retcode'] == RetCode.SUCCESS

    # 未被调用
    def set_job_metric_meta(self, metric_namespace: str, metric_name: str, metric_meta: MetricMeta):
        self.set_metric_meta_common(metric_namespace=metric_namespace, metric_name=metric_name, metric_meta=metric_meta,
                                    job_level=True)

    # 被调用：
    # fate_flow.components.download.Download的callback_metric方法
    # fate_flow.components.reader.Reader的run方法
    # fate_flow.components.upload.Upload的save_meta方法
    # federatedml.evaluation.evaluation.Evaluation中多个方法
    # federatedml.feature.sampler的callback方法
    # federatedml.feature.sbt_feature_transformer.sbt_feature_transformer.HeteroSBTFeatureTransform的_callback_leaf_id_mapping方法
    # federatedml.model_base.ModelBase的callback_meta方法
    # federatedml.model_selection.data_split.data_split.DataSplitter的callback_meta方法
    # federatedml.model_selection.stepwise.hetero_stepwise.HeteroStepwise的record_step_best方法
    # federatedml.secure_information_retrieval.base_secure_information_retrieval.BaseSecureInformatio的_display_result方法
    # federatedml.statistic.correlation.hetero_pearson.HeteroPearson的_callback方法
    # federatedml.statistic.intersect.intersect_model.IntersectModelBase的fit方法
    # federatedml.statistic.union.union.Union的fit方法
    # federatedml.util.sample_weight.SampleWeight的callback_info方法
    def set_metric_meta(self, metric_namespace: str, metric_name: str, metric_meta: MetricMeta):
        self.set_metric_meta_common(metric_namespace=metric_namespace, metric_name=metric_name, metric_meta=metric_meta,
                                    job_level=False)

    # 保存metric_meta，被set_metric_meta和set_job_metric_meta调用
    def set_metric_meta_common(self, metric_namespace: str, metric_name: str, metric_meta: MetricMeta, job_level=False):
        LOGGER.info("Request save job {} task {} {} on {} {} metric {} {} meta".format(self.job_id,
                                                                                       self.task_id,
                                                                                       self.task_version,
                                                                                       self.role,
                                                                                       self.party_id,
                                                                                       metric_namespace,
                                                                                       metric_name))
        request_body = dict()
        request_body['metric_namespace'] = metric_namespace
        request_body['metric_name'] = metric_name
        request_body['metric_meta'] = serialize_b64(metric_meta, to_str=True)
        request_body['job_level'] = job_level
        # fate_flow.utils.api_utils模块的local_api方法
        # 调用fate_flow.scheduling_apps.tracker_app模块的save_metric_meta接口来保存metric_meta
        response = api_utils.local_api(job_id=self.job_id,
                                       method='POST',
                                       endpoint='/tracker/{}/{}/{}/{}/{}/{}/metric_meta/save'.format(
                                           self.job_id,
                                           self.component_name,
                                           self.task_id,
                                           self.task_version,
                                           self.role,
                                           self.party_id),
                                       json_body=request_body)
        return response['retcode'] == RetCode.SUCCESS

    # 保存输出数据信息
    # 被调用：
    # fate_flow.components.reader.Reader的run方法
    # fate_flow.components.upload.Upload的save_meta方法
    def log_output_data_info(self, data_name: str, table_namespace: str, table_name: str):
        LOGGER.info("Request save job {} task {} {} on {} {} data {} info".format(self.job_id,
                                                                                  self.task_id,
                                                                                  self.task_version,
                                                                                  self.role,
                                                                                  self.party_id,
                                                                                  data_name))
        request_body = dict()
        request_body["data_name"] = data_name
        request_body["table_namespace"] = table_namespace
        request_body["table_name"] = table_name
        # fate_flow.utils.api_utils模块的local_api方法
        # 调用fate_flow.scheduling_apps.tracker_app模块的save_output_data_info接口来保存输出数据信息
        response = api_utils.local_api(job_id=self.job_id,
                                       method='POST',
                                       endpoint='/tracker/{}/{}/{}/{}/{}/{}/output_data_info/save'.format(
                                           self.job_id,
                                           self.component_name,
                                           self.task_id,
                                           self.task_version,
                                           self.role,
                                           self.party_id),
                                       json_body=request_body)
        return response['retcode'] == RetCode.SUCCESS

    # 读取输出数据信息
    # 被调用：
    # fate_flow.operation.task_executor.TaskExecutor的get_task_run_args方法
    def get_output_data_info(self, data_name=None):
        LOGGER.info("Request read job {} task {} {} on {} {} data {} info".format(self.job_id,
                                                                                  self.task_id,
                                                                                  self.task_version,
                                                                                  self.role,
                                                                                  self.party_id,
                                                                                  data_name))
        request_body = dict()
        request_body["data_name"] = data_name
        # fate_flow.utils.api_utils模块的local_api方法
        # 调用fate_flow.scheduling_apps.tracker_app模块的read_output_data_info接口来读取输出数据信息
        response = api_utils.local_api(job_id=self.job_id,
                                       method='POST',
                                       endpoint='/tracker/{}/{}/{}/{}/{}/{}/output_data_info/read'.format(
                                           self.job_id,
                                           self.component_name,
                                           self.task_id,
                                           self.task_version,
                                           self.role,
                                           self.party_id),
                                       json_body=request_body)
        if response["retcode"] == RetCode.SUCCESS and "data" in response:
            return response["data"]
        else:
            return None

    # 保存组件summary
    # 被调用：
    # federatedml.model_base.ModelBase的save_summary方法
    def log_component_summary(self, summary_data: dict):
        LOGGER.info("Request save job {} task {} {} on {} {} component summary".format(self.job_id,
                                                                                       self.task_id,
                                                                                       self.task_version,
                                                                                       self.role,
                                                                                       self.party_id))
        request_body = dict()
        request_body["summary"] = summary_data
        # fate_flow.utils.api_utils模块的local_api方法
        # 调用fate_flow.scheduling_apps.tracker_app模块的save_component_summary接口来保存组件summary
        response = api_utils.local_api(job_id=self.job_id,
                                       method='POST',
                                       endpoint='/tracker/{}/{}/{}/{}/{}/{}/summary/save'.format(
                                           self.job_id,
                                           self.component_name,
                                           self.task_id,
                                           self.task_version,
                                           self.role,
                                           self.party_id),
                                       json_body=request_body)
        return response['retcode'] == RetCode.SUCCESS
