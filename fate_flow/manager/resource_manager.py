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

import math
import typing

from fate_arch.common import EngineType
from fate_arch.common import base_utils
from fate_arch.common.conf_utils import get_base_config
from fate_arch.common.log import schedule_logger
from fate_arch.computing import ComputingEngine
from fate_flow.db.db_models import DB, EngineRegistry, Job
from fate_flow.entity.types import ResourceOperation, RunParameters
from fate_flow.settings import stat_logger, STANDALONE_BACKEND_VIRTUAL_CORES_PER_NODE, SUPPORT_BACKENDS_ENTRANCE, \
    MAX_CORES_PERCENT_PER_JOB, DEFAULT_TASK_CORES, IGNORE_RESOURCE_ROLES, SUPPORT_IGNORE_RESOURCE_ENGINES, TOTAL_CORES_OVERWEIGHT_PERCENT, TOTAL_MEMORY_OVERWEIGHT_PERCENT
from fate_flow.utils import job_utils

# 定义资源管理类
class ResourceManager(object):
    @classmethod
    # 初始化资源管理类
    def initialize(cls):
        # eggroll和spark
        # 从fate_flow.settings调用SUPPORT_BACKENDS_ENTRANCE模块
        # 获得所用到支持的引擎
        for backend_name, backend_engines in SUPPORT_BACKENDS_ENTRANCE.items():
            for engine_type, engine_keys_list in backend_engines.items():
                for engine_keys in engine_keys_list:
                    # 从fate_arch.common.conf_utils中调用get_base_config方法
                    # 获得基础的config配置
                    engine_config = get_base_config(backend_name, {}).get(engine_keys[1], {})
                    if engine_config:
                        # 如果引擎的config存在就注册该引擎
                        cls.register_engine(engine_type=engine_type, engine_name=engine_keys[0], engine_entrance=engine_keys[1], engine_config=engine_config)

        # initialize standalone engine
        for backend_engines in SUPPORT_BACKENDS_ENTRANCE.values():
            for engine_type in backend_engines.keys():
                engine_name = "STANDALONE"
                engine_entrance = "fateflow"
                engine_config = {
                    "nodes": 1,
                    "cores_per_node": STANDALONE_BACKEND_VIRTUAL_CORES_PER_NODE,
                }
                cls.register_engine(engine_type=engine_type, engine_name=engine_name, engine_entrance=engine_entrance, engine_config=engine_config)

    @classmethod
    @DB.connection_context()
    # 引擎的注册
    def register_engine(cls, engine_type, engine_name, engine_entrance, engine_config):
        nodes = engine_config.get("nodes", 1)
        # 从fate_flow.settings调用TOTAL_CORES_OVERWEIGHT_PERCENT
        # 全部核所占的百分比，默认为1，表示不超载
        cores = engine_config.get("cores_per_node", 0) * nodes * TOTAL_CORES_OVERWEIGHT_PERCENT
        # 从fate_flow.settings调用TOTAL_MEMORY_OVERWEIGHT_PERCENT
        # 全部内存所占的百分比，默认为1，表示不超载
        memory = engine_config.get("memory_per_node", 0) * nodes * TOTAL_MEMORY_OVERWEIGHT_PERCENT
        # 从fate_flow.db.db_models中调用EngineRegistry模块
        # 作为数据库搜索的判断条件
        filters = [EngineRegistry.f_engine_type == engine_type, EngineRegistry.f_engine_name == engine_name]
        resources = EngineRegistry.select().where(*filters)
        if resources:
            resource = resources[0]
            update_fields = {}
            update_fields[EngineRegistry.f_engine_config] = engine_config
            update_fields[EngineRegistry.f_cores] = cores
            update_fields[EngineRegistry.f_memory] = memory
            update_fields[EngineRegistry.f_remaining_cores] = EngineRegistry.f_remaining_cores + (
                    cores - resource.f_cores)
            update_fields[EngineRegistry.f_remaining_memory] = EngineRegistry.f_remaining_memory + (
                    memory - resource.f_memory)
            update_fields[EngineRegistry.f_nodes] = nodes
            # 执行数据库更新语句
            operate = EngineRegistry.update(update_fields).where(*filters)
            update_status = operate.execute() > 0
            if update_status:
                # 从fate_flow.settings中调用stat_logger模块
                # 记录sql语句日志
                stat_logger.info(f"update {engine_type} engine {engine_name} {engine_entrance} registration information")
            else:
                stat_logger.info(f"update {engine_type} engine {engine_name} {engine_entrance} registration information takes no effect")
        else:
            resource = EngineRegistry()
            # 调用fate_arch.common中的base_utils
            # current_timestamp方法返回当前时间戳
            resource.f_create_time = base_utils.current_timestamp()
            resource.f_engine_type = engine_type
            resource.f_engine_name = engine_name
            resource.f_engine_entrance = engine_entrance
            resource.f_engine_config = engine_config

            resource.f_cores = cores
            resource.f_memory = memory
            resource.f_remaining_cores = cores
            resource.f_remaining_memory = memory
            resource.f_nodes = nodes
            try:
                resource.save(force_insert=True)
            except Exception as e:
                stat_logger.warning(e)
            stat_logger.info(f"create {engine_type} engine {engine_name} {engine_entrance} registration information")

    @classmethod
    # 检查资源申请
    def check_resource_apply(cls, job_parameters: RunParameters, role, party_id, engines_info):
        # 计算job所需资源
        computing_engine, cores, memory = cls.calculate_job_resource(job_parameters=job_parameters, role=role, party_id=party_id)
        # 从fate_flow.settings调用MAX_CORES_PERCENT_PER_JOB
        # 每个job可以使用的最大核百分比
        max_cores_per_job = math.floor(engines_info[EngineType.COMPUTING].f_cores * MAX_CORES_PERCENT_PER_JOB)
        if cores > max_cores_per_job:
            return False, cores, max_cores_per_job
        else:
            return True, cores, max_cores_per_job

    @classmethod
    # 申请job资源
    def apply_for_job_resource(cls, job_id, role, party_id):
        return cls.resource_for_job(job_id=job_id, role=role, party_id=party_id, operation_type=ResourceOperation.APPLY)

    @classmethod
    # 返回job资源
    def return_job_resource(cls, job_id, role, party_id):
        return cls.resource_for_job(job_id=job_id, role=role, party_id=party_id,
                                    operation_type=ResourceOperation.RETURN)

    @classmethod
    @DB.connection_context()
    # 根据ResourceOperation类型决定是申请job资源还是返回job资源
    def resource_for_job(cls, job_id, role, party_id, operation_type):
        operate_status = False
        # 计算job所需资源
        engine_name, cores, memory = cls.calculate_job_resource(job_id=job_id, role=role, party_id=party_id)
        try:
            with DB.atomic():
                updates = {
                    # 从fate_flow.db.db_models调用Job模块
                    # 进行数据库写入
                    Job.f_engine_type: EngineType.COMPUTING,
                    Job.f_engine_name: engine_name,
                    Job.f_cores: cores,
                    Job.f_memory: memory,
                }
                filters = [
                    Job.f_job_id == job_id,
                    Job.f_role == role,
                    Job.f_party_id == party_id,
                ]
                if operation_type == ResourceOperation.APPLY:
                    updates[Job.f_remaining_cores] = cores
                    updates[Job.f_remaining_memory] = memory
                    updates[Job.f_resource_in_use] = True
                    updates[Job.f_apply_resource_time] = base_utils.current_timestamp()
                    filters.append(Job.f_resource_in_use == False)
                elif operation_type == ResourceOperation.RETURN:
                    updates[Job.f_resource_in_use] = False
                    updates[Job.f_return_resource_time] = base_utils.current_timestamp()
                    filters.append(Job.f_resource_in_use == True)
                operate = Job.update(updates).where(*filters)
                record_status = operate.execute() > 0
                if not record_status:
                    raise RuntimeError(f"record job {job_id} resource {operation_type} failed on {role} {party_id}")

                if cores or memory:
                    filters, updates = cls.update_resource_sql(resource_model=EngineRegistry,
                                                               cores=cores,
                                                               memory=memory,
                                                               operation_type=operation_type,
                                                               )
                    filters.append(EngineRegistry.f_engine_type == EngineType.COMPUTING)
                    filters.append(EngineRegistry.f_engine_name == engine_name)
                    operate = EngineRegistry.update(updates).where(*filters)
                    apply_status = operate.execute() > 0
                else:
                    apply_status = True
                if not apply_status:
                    raise RuntimeError(
                        f"update engine {engine_name} record for job {job_id} resource {operation_type} on {role} {party_id} failed")
            operate_status = True
        except Exception as e:
            schedule_logger(job_id=job_id).warning(e)
            schedule_logger(job_id=job_id).warning(
                f"{operation_type} job {job_id} resource(cores {cores} memory {memory}) on {role} {party_id} failed")
            operate_status = False
        finally:
            remaining_cores, remaining_memory = cls.get_remaining_resource(EngineRegistry,
                                                                           [
                                                                               EngineRegistry.f_engine_type == EngineType.COMPUTING,
                                                                               EngineRegistry.f_engine_name == engine_name])
            operate_msg = "successfully" if operate_status else "failed"
            schedule_logger(job_id=job_id).info(
                f"{operation_type} job {job_id} resource(cores {cores} memory {memory}) on {role} {party_id} {operate_msg}, remaining cores: {remaining_cores} remaining memory: {remaining_memory}")
            return operate_status

    @classmethod
    # 修正引擎参数
    def adapt_engine_parameters(cls, role, job_parameters: RunParameters, create_initiator_baseline=False):
        # 获取引擎注册信息
        computing_engine_info = ResourceManager.get_engine_registration_info(engine_type=EngineType.COMPUTING,
                                                                             engine_name=job_parameters.computing_engine)
        if create_initiator_baseline:
            job_parameters.adaptation_parameters = {
                "task_nodes": 0,
                "task_cores_per_node": 0,
                "task_memory_per_node": 0,
                # request_task_cores base on initiator and distribute to all parties, using job conf parameters or initiator fateflow server default settings
                "request_task_cores": int(job_parameters.task_cores) if job_parameters.task_cores else DEFAULT_TASK_CORES,
                "if_initiator_baseline": True
            }
        else:
            # use initiator baseline
            if role == "arbiter":
                job_parameters.adaptation_parameters["request_task_cores"] = 1
            elif "request_task_cores" not in job_parameters.adaptation_parameters:
                # compatibility 1.5.0
                job_parameters.adaptation_parameters["request_task_cores"] = job_parameters.adaptation_parameters["task_nodes"] * job_parameters.adaptation_parameters["task_cores_per_node"]

            job_parameters.adaptation_parameters["if_initiator_baseline"] = False
        adaptation_parameters = job_parameters.adaptation_parameters

        if job_parameters.computing_engine in {ComputingEngine.STANDALONE, ComputingEngine.EGGROLL}:
            adaptation_parameters["task_nodes"] = computing_engine_info.f_nodes
            if int(job_parameters.eggroll_run.get("eggroll.session.processors.per.node", 0)) > 0:
                adaptation_parameters["task_cores_per_node"] = int(job_parameters.eggroll_run["eggroll.session.processors.per.node"])
            else:
                adaptation_parameters["task_cores_per_node"] = max(1, int(adaptation_parameters["request_task_cores"] / adaptation_parameters["task_nodes"]))
            if not create_initiator_baseline:
                # set the adaptation parameters to the actual engine operation parameters
                job_parameters.eggroll_run["eggroll.session.processors.per.node"] = adaptation_parameters["task_cores_per_node"]
        elif job_parameters.computing_engine == ComputingEngine.SPARK:
            adaptation_parameters["task_nodes"] = int(job_parameters.spark_run.get("num-executors", computing_engine_info.f_nodes))
            if int(job_parameters.spark_run.get("executor-cores", 0)) > 0:
                adaptation_parameters["task_cores_per_node"] = int(job_parameters.spark_run["executor-cores"])
            else:
                adaptation_parameters["task_cores_per_node"] = max(1, int(adaptation_parameters["request_task_cores"] / adaptation_parameters["task_nodes"]))
            if not create_initiator_baseline:
                # set the adaptation parameters to the actual engine operation parameters
                job_parameters.spark_run["num-executors"] = adaptation_parameters["task_nodes"]
                job_parameters.spark_run["executor-cores"] = adaptation_parameters["task_cores_per_node"]

    @classmethod
    # 计算job所需资源
    def calculate_job_resource(cls, job_parameters: RunParameters = None, job_id=None, role=None, party_id=None):
        if not job_parameters:
            # 从fate_flow.utils导入job_utils模块
            # get_job_parameters方法得到job的参数
            job_parameters = job_utils.get_job_parameters(job_id=job_id,
                                                          role=role,
                                                          party_id=party_id)
            job_parameters = RunParameters(**job_parameters)
        if role in IGNORE_RESOURCE_ROLES and job_parameters.computing_engine in SUPPORT_IGNORE_RESOURCE_ENGINES:
            cores = 0
            memory = 0
        else:
            cores = job_parameters.adaptation_parameters["task_cores_per_node"] * job_parameters.adaptation_parameters[
                "task_nodes"] * job_parameters.task_parallelism
            memory = job_parameters.adaptation_parameters["task_memory_per_node"] * job_parameters.adaptation_parameters[
                "task_nodes"] * job_parameters.task_parallelism
        return job_parameters.computing_engine, cores, memory

    @classmethod
    # 计算task所需资源
    def calculate_task_resource(cls, task_parameters: RunParameters = None, task_info: dict = None):
        if not task_parameters:
            # 从fate_flow.utils导入job_utils模块
            # get_job_parameters方法得到job的参数
            job_parameters = job_utils.get_job_parameters(job_id=task_info["job_id"],
                                                          role=task_info["role"],
                                                          party_id=task_info["party_id"])
            task_parameters = RunParameters(**job_parameters)
        if task_info["role"] in IGNORE_RESOURCE_ROLES and task_parameters.computing_engine in SUPPORT_IGNORE_RESOURCE_ENGINES:
            cores_per_task = 0
            memory_per_task = 0
        else:
            cores_per_task = task_parameters.adaptation_parameters["task_cores_per_node"] * \
                             task_parameters.adaptation_parameters["task_nodes"]
            memory_per_task = task_parameters.adaptation_parameters["task_memory_per_node"] * \
                              task_parameters.adaptation_parameters["task_nodes"]
        return cores_per_task, memory_per_task

    @classmethod
    # 申请task资源
    def apply_for_task_resource(cls, task_info):
        return ResourceManager.resource_for_task(task_info=task_info, operation_type=ResourceOperation.APPLY)

    @classmethod
    # 返回task资源
    def return_task_resource(cls, task_info):
        return ResourceManager.resource_for_task(task_info=task_info, operation_type=ResourceOperation.RETURN)

    @classmethod
    # 根据ResourceOperation类型决定是申请task资源还是返回task资源
    def resource_for_task(cls, task_info, operation_type):
        # 计算task所需资源
        cores_per_task, memory_per_task = cls.calculate_task_resource(task_info=task_info)

        if cores_per_task or memory_per_task:
            filters, updates = cls.update_resource_sql(resource_model=Job,
                                                       cores=cores_per_task,
                                                       memory=memory_per_task,
                                                       operation_type=operation_type,
                                                       )
            filters.append(Job.f_job_id == task_info["job_id"])
            filters.append(Job.f_role == task_info["role"])
            filters.append(Job.f_party_id == task_info["party_id"])
            filters.append(Job.f_resource_in_use == True)
            operate = Job.update(updates).where(*filters)
            operate_status = operate.execute() > 0
        else:
            operate_status = True
        if operate_status:
            schedule_logger(job_id=task_info["job_id"]).info(
                "task {} {} {} resource successfully".format(task_info["task_id"],
                                                             task_info["task_version"], operation_type))
        else:
            schedule_logger(job_id=task_info["job_id"]).warning(
                "task {} {} {} resource failed".format(task_info["task_id"],
                                                       task_info["task_version"], operation_type))
        return operate_status

    @classmethod
    # 执行更新资源的sql语句
    def update_resource_sql(cls, resource_model: typing.Union[EngineRegistry, Job], cores, memory, operation_type):
        if operation_type == ResourceOperation.APPLY:
            filters = [
                resource_model.f_remaining_cores >= cores,
                resource_model.f_remaining_memory >= memory
            ]
            updates = {resource_model.f_remaining_cores: resource_model.f_remaining_cores - cores,
                       resource_model.f_remaining_memory: resource_model.f_remaining_memory - memory}
        elif operation_type == ResourceOperation.RETURN:
            filters = []
            updates = {resource_model.f_remaining_cores: resource_model.f_remaining_cores + cores,
                       resource_model.f_remaining_memory: resource_model.f_remaining_memory + memory}
        else:
            raise RuntimeError(f"can not support {operation_type} resource operation type")
        return filters, updates

    @classmethod
    @DB.connection_context()
    # 获取剩余资源
    def get_remaining_resource(cls, resource_model: typing.Union[EngineRegistry, Job], filters):
        remaining_cores, remaining_memory = None, None
        try:
            objs = resource_model.select(resource_model.f_remaining_cores, resource_model.f_remaining_memory).where(
                *filters)
            if objs:
                remaining_cores, remaining_memory = objs[0].f_remaining_cores, objs[0].f_remaining_memory
        except Exception as e:
            schedule_logger().exception(e)
        finally:
            return remaining_cores, remaining_memory

    @classmethod
    @DB.connection_context()
    # 获取引擎注册信息
    def get_engine_registration_info(cls, engine_type, engine_name) -> EngineRegistry:
        # 从数据库中读取
        engines = EngineRegistry.select().where(EngineRegistry.f_engine_type == engine_type,
                                                EngineRegistry.f_engine_name == engine_name)
        if engines:
            return engines[0]
        else:
            return None
