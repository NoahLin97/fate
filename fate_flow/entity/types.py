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
from fate_arch.common import WorkMode, Backend
from enum import IntEnum


class RunParameters(object):
    # 初始化RunParameters类
    def __init__(self, **kwargs):
        self.job_type = "train"
        # 从fate_arch.common中调用了WorkMode, Backend
        # WorkMode表示工作模式，1为集群，0为单机
        # Backend设置fate使用的计算引擎，有spark和eggroll
        self.work_mode = WorkMode.STANDALONE
        self.backend = Backend.EGGROLL  # Pre-v1.5 configuration item
        self.computing_engine = None
        self.federation_engine = None
        self.storage_engine = None
        self.engines_address = {}
        self.federated_mode = None
        self.federation_info = None
        self.task_cores = None
        self.task_parallelism = None
        self.computing_partitions = None
        self.federated_status_collect_type = None
        self.federated_data_exchange_type = None  # not use in v1.5.0
        self.model_id = None
        self.model_version = None
        self.dsl_version = None
        self.timeout = None
        self.eggroll_run = {}
        self.spark_run = {}
        self.rabbitmq_run = {}
        self.pulsar_run = {}
        self.adaptation_parameters = {}
        self.assistant_role = None
        self.map_table_name = None
        self.map_namespace = None
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)

    # 返回非空的RunParameters类变量
    def to_dict(self):
        d = {}
        for k, v in self.__dict__.items():
            if v is None:
                continue
            d[k] = v
        return d

# 定义返回值编码
class RetCode(IntEnum):
    SUCCESS = 0
    EXCEPTION_ERROR = 100
    PARAMETER_ERROR = 101
    DATA_ERROR = 102
    OPERATING_ERROR = 103
    FEDERATED_ERROR = 104
    CONNECTION_ERROR = 105
    SERVER_ERROR = 500

# 定义调度状态编码
class SchedulingStatusCode(object):
    SUCCESS = 0
    NO_RESOURCE = 1
    PASS = 1
    NO_NEXT = 2
    HAVE_NEXT = 3
    FAILED = 4

# 定义联邦调度状态编码
class FederatedSchedulingStatusCode(object):
    SUCCESS = 0
    PARTIAL = 1
    FAILED = 2
    ERROR = 3

# 定义状态基类
class BaseStatus(object):
    @classmethod
    # 返回状态列表
    def status_list(cls):
        return [cls.__dict__[k] for k in cls.__dict__.keys() if not callable(getattr(cls, k)) and not k.startswith("__")]

    @classmethod
    # 判断状态是否存在于状态列表中
    def contains(cls, status):
        return status in cls.status_list()

# 继承BaseStatus类，定义状态集
class StatusSet(BaseStatus):
    WAITING = 'waiting'
    READY = 'ready'
    RUNNING = "running"
    CANCELED = "canceled"
    TIMEOUT = "timeout"
    FAILED = "failed"
    SUCCESS = "success"

    @classmethod
    # 返回状态值
    def get_level(cls, status):
        return dict(zip(cls.status_list(), range(len(cls.status_list())))).get(status, None)

# 定义状态转换规则基类
class BaseStateTransitionRule(object):
    RULES = {}

    @classmethod
    # 判断是否可以从src_status转换到dest_status
    def if_pass(cls, src_status, dest_status):
        if src_status not in cls.RULES:
            return False
        if dest_status not in cls.RULES[src_status]:
            return False
        else:
            return True

# 继承状态基类，定义job状态集
class JobStatus(BaseStatus):
    WAITING = StatusSet.WAITING
    READY = StatusSet.READY
    RUNNING = StatusSet.RUNNING
    CANCELED = StatusSet.CANCELED
    TIMEOUT = StatusSet.TIMEOUT
    FAILED = StatusSet.FAILED
    SUCCESS = StatusSet.SUCCESS

    # 继承状态转换规则基类
    class StateTransitionRule(BaseStateTransitionRule):
        RULES = {
            StatusSet.WAITING: [StatusSet.READY, StatusSet.RUNNING, StatusSet.CANCELED, StatusSet.TIMEOUT, StatusSet.FAILED, StatusSet.SUCCESS],
            StatusSet.READY: [StatusSet.WAITING, StatusSet.RUNNING, StatusSet.CANCELED, StatusSet.TIMEOUT, StatusSet.FAILED],
            StatusSet.RUNNING: [StatusSet.CANCELED, StatusSet.TIMEOUT, StatusSet.FAILED, StatusSet.SUCCESS],
            StatusSet.CANCELED: [StatusSet.WAITING],
            StatusSet.TIMEOUT: [StatusSet.FAILED, StatusSet.SUCCESS, StatusSet.WAITING],
            StatusSet.FAILED: [StatusSet.WAITING],
            StatusSet.SUCCESS: [StatusSet.WAITING],
        }

# 继承状态基类，定义task状态集
class TaskStatus(BaseStatus):
    WAITING = StatusSet.WAITING
    RUNNING = StatusSet.RUNNING
    CANCELED = StatusSet.CANCELED
    TIMEOUT = StatusSet.TIMEOUT
    FAILED = StatusSet.FAILED
    SUCCESS = StatusSet.SUCCESS

    # 继承状态转换规则基类
    class StateTransitionRule(BaseStateTransitionRule):
        RULES = {
            StatusSet.WAITING: [StatusSet.RUNNING, StatusSet.SUCCESS],
            StatusSet.RUNNING: [StatusSet.CANCELED, StatusSet.TIMEOUT, StatusSet.FAILED, StatusSet.SUCCESS],
            StatusSet.CANCELED: [StatusSet.WAITING],
            StatusSet.TIMEOUT: [StatusSet.FAILED, StatusSet.SUCCESS],
            StatusSet.FAILED: [],
            StatusSet.SUCCESS: [],
        }

# 继承BaseStatus类，定义运行状态
class OngoingStatus(BaseStatus):
    WAITING = StatusSet.WAITING
    RUNNING = StatusSet.RUNNING

# 继承BaseStatus类，定义中断状态
class InterruptStatus(BaseStatus):
    CANCELED = StatusSet.CANCELED
    TIMEOUT = StatusSet.TIMEOUT
    FAILED = StatusSet.FAILED

# 继承BaseStatus类，定义结束状态
class EndStatus(BaseStatus):
    CANCELED = StatusSet.CANCELED
    TIMEOUT = StatusSet.TIMEOUT
    FAILED = StatusSet.FAILED
    SUCCESS = StatusSet.SUCCESS

# 定义模型存储
class ModelStorage(object):
    REDIS = "redis"
    MYSQL = "mysql"

# 定义模型操作
class ModelOperation(object):
    STORE = "store"
    RESTORE = "restore"
    EXPORT = "export"
    IMPORT = "import"
    LOAD = "load"
    BIND = "bind"

# 定义进程角色
class ProcessRole(object):
    DRIVER = "driver"
    EXECUTOR = "executor"

# 定义tag操作
class TagOperation(object):
    CREATE = "create"
    RETRIEVE = "retrieve"
    UPDATE = "update"
    DESTROY = "destroy"
    LIST = "list"

# 定义资源操作
class ResourceOperation(object):
    APPLY = "apply"
    RETURN = "return"

# 定义kill进程状态编码
class KillProcessStatusCode(object):
    KILLED = 0
    NOT_FOUND = 1
    ERROR_PID = 2
