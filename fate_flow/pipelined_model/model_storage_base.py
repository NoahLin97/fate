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
import abc

# 定义模型存储基类
# 被调：
# 被fate_flow.pipelined_model.mysql_model_storage.py里面的MysqlModelStorage类继承
# 被fate_flow.pipelined_model.redis_model_storage.py里面的RedisModelStorage类继承
class ModelStorageBase(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    # 定义存储函数，将本地缓存中的模型存储到可靠的系统中
    def store(self, model_id: str, model_version: str, store_address: dict):
        """
        Store the model from local cache to a reliable system
        :param model_id:
        :param model_version:
        :param store_address:
        :return:
        """
        raise Exception("Subclasses must implement this function")

    @abc.abstractmethod
    # 定义恢复函数，从存储系统中读取模型到本地缓存
    def restore(self, model_id: str, model_version: str, store_address: dict):
        """
        Restore model from storage system to local cache
        :param model_id:
        :param model_version:
        :param store_address:
        :return:
        """
        raise Exception("Subclasses must implement this function")
