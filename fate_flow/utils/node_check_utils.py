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
from fate_arch.common import conf_utils

from fate_flow.settings import CHECK_NODES_IDENTITY, FATE_MANAGER_NODE_CHECK_ENDPOINT
from fate_flow.utils.service_utils import ServiceUtils

# 检查节点
def nodes_check(src_party_id, src_role, appKey, appSecret, dst_party_id):
    # 检查节点身份，默认为false
    # AppKey：公钥（相当于账号）
    # AppSecret：私钥（相当于密码）
    if CHECK_NODES_IDENTITY:
        body = {
            'srcPartyId': int(src_party_id),
            'role': src_role,
            'appKey': appKey,
            'appSecret': appSecret,
            'dstPartyId': int(dst_party_id),
            'federatedId': conf_utils.get_base_config("fatemanager", {}).get("federatedId")
        }
        try:
            response = requests.post(url="http://{}:{}{}".format(
                ServiceUtils.get_item("fatemanager", "host"),
                ServiceUtils.get_item("fatemanager", "port"),
                FATE_MANAGER_NODE_CHECK_ENDPOINT), json=body).json()
            # FATE_MANAGER_NODE_CHECK_ENDPOINT = "/fate-manager/api/site/checksite"
            if response['code'] != 0:
                raise Exception(str(response['msg']))
        except Exception as e:
            raise Exception('role {} party id {} authentication failed: {}'.format(src_role, src_party_id, str(e)))
