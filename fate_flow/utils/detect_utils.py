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
import typing

# 检查cofig是否正确
# 被调：
# 被fate_client.flow_client.flow_cli.commands.privilege.py里面的query、grant、delete函数所调用
# 被fate_flow.apps.data_access_app.py里面的download_upload函数所调用
# 被fate_flow.apps.job_app.py里面的submit_job、get_url函数所调用
# 被fate_flow.apps.model_app.py里面的migrate_model_process、bind_model_service、operate_model、deploy、get_predict_conf函数所调用
# 被fate_flow.apps.tracking_app.py里面的component_output_data_table、get_component_summary函数所调用
# 被fate_flow.fate_flow_client.py里面的call_fun函数所调用
# 被fate_flow.manager.pipeline_manager.py里面的pipeline_dag_dependency函数所调用
# 被fate_flow.scheduler.dag_scheduler.py里面的submit函数所调用
# 被fate_flow.utils.job_utils.py里面的check_job_runtime_conf函数所调用
def check_config(config: typing.Dict, required_arguments: typing.List):
    # 检查config里面是否都存在required_arguments
    no_arguments = []
    error_arguments = []
    for require_argument in required_arguments:
        if isinstance(require_argument, tuple):
            config_value = config.get(require_argument[0], None)
            if isinstance(require_argument[1], (tuple, list)):
                if config_value not in require_argument[1]:
                    error_arguments.append(require_argument)
            elif config_value != require_argument[1]:
                error_arguments.append(require_argument)
        elif require_argument not in config:
            no_arguments.append(require_argument)
    if no_arguments or error_arguments:
        error_string = ""
        if no_arguments:
            error_string += "required parameters are missing: {};".format(",".join(no_arguments))
        if error_arguments:
            error_string += "required parameter values: {}".format(",".join(["{}={}".format(a[0], a[1]) for a in error_arguments]))
        raise Exception(error_string)
