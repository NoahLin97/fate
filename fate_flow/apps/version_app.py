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

from fate_arch.common import conf_utils
from fate_flow.entity.runtime_config import RuntimeConfig
from fate_flow.settings import stat_logger
from fate_flow.utils.api_utils import get_json_result

manager = Flask(__name__)


@manager.errorhandler(500)
def internal_server_error(e):
    stat_logger.exception(e)
    return get_json_result(retcode=100, retmsg=str(e))

# 获取版本信息
# http接口
@manager.route('/get', methods=['POST'])
def get_fate_version_info():
    # 调用fate_flow.entity.runtime_config模块的RuntimeConfig类的get_env方法得到版本信息
    version = RuntimeConfig.get_env(request.json.get('module', 'FATE'))
    return get_json_result(data={request.json.get('module'): version})

# 设置federatedId
# http接口
@manager.route('/set', methods=['POST'])
def set_fate_server_info():
    # manager
    federated_id = request.json.get("federatedId")

    # 调用fate_arch.common.conf_utils模块的get_base_config方法得到manager_conf
    manager_conf = conf_utils.get_base_config("fatemanager", {})
    manager_conf["federatedId"] = federated_id

    # 调用fate_arch.common.conf_utils模块的update_config方法更新federatedId
    conf_utils.update_config("fatemanager", manager_conf)
    return get_json_result(data={"federatedId": federated_id})
