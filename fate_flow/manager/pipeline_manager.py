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
from fate_flow.settings import stat_logger
from fate_flow.utils import job_utils, detect_utils, schedule_utils
from fate_flow.operation.job_saver import JobSaver

# 检查pipeline的dag依赖是否正确，其中检查config和dsl
def pipeline_dag_dependency(job_info):
    try:
        # 检查cofig是否正确
        detect_utils.check_config(job_info, required_arguments=["party_id", "role"])
        if job_info.get('job_id'):
            # 查询对应job_id的job
            jobs = JobSaver.query_job(job_id=job_info["job_id"], party_id=job_info["party_id"], role=job_info["role"])
            if not jobs:
                raise Exception('query job {} failed'.format(job_info.get('job_id', '')))
            job = jobs[0]
            # 得到对应job_id的dsl解析器
            job_dsl_parser = schedule_utils.get_job_dsl_parser(dsl=job.f_dsl,
                                                               runtime_conf=job.f_runtime_conf_on_party,
                                                               train_runtime_conf=job.f_train_runtime_conf)
        else:
            job_dsl_parser = schedule_utils.get_job_dsl_parser(dsl=job_info.get('job_dsl', {}),
                                                               runtime_conf=job_info.get('job_runtime_conf', {}),
                                                               train_runtime_conf=job_info.get('job_train_runtime_conf', {}))
        return job_dsl_parser.get_dependency(role=job_info["role"], party_id=int(job_info["party_id"]))
    except Exception as e:
        stat_logger.exception(e)
        raise e
