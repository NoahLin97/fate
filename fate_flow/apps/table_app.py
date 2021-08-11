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
from fate_arch import storage
from fate_flow.entity.types import RunParameters
from fate_flow.operation.job_saver import JobSaver
from fate_flow.operation.job_tracker import Tracker
from fate_flow.operation.task_executor import TaskExecutor
from fate_flow.utils.api_utils import get_json_result
from fate_flow.utils import detect_utils, job_utils, schedule_utils
from fate_flow.settings import stat_logger
from flask import Flask, request

manager = Flask(__name__)

# 对table进行操作的http接口

@manager.errorhandler(500)
def internal_server_error(e):
    stat_logger.exception(e)
    return get_json_result(retcode=100, retmsg=str(e))

##增加table
@manager.route('/add', methods=['post'])
def table_add():
    request_data = request.json
    #调用fate_flow.utils.detect_utils模块的check_config方法检查参数
    detect_utils.check_config(request_data, required_arguments=["engine", "address", "namespace", "name", ("head", (0, 1)), "id_delimiter"])

    ##读取参数存入变量
    address_dict = request_data.get('address')
    engine = request_data.get('engine')
    name = request_data.get('name')
    namespace = request_data.get('namespace')

    #以engine和address_dict为参数，调用fate_arch.storage模块的create_address方法生成address对象
    address = storage.StorageTableMeta.create_address(storage_engine=engine, address_dict=address_dict)

    in_serialized = request_data.get("in_serialized", 1 if engine in {storage.StorageEngine.STANDALONE, storage.StorageEngine.EGGROLL} else 0)
    destroy = (int(request_data.get("drop", 0)) == 1)

    #以name和namespace为参数，生成fate_arch.storage._table.StorageTableMeta类的对象data_table_meta
    data_table_meta = storage.StorageTableMeta(name=name, namespace=namespace)
    if data_table_meta:#data_table_meta不为空
        if destroy:
            #若drop参数为1，则通过data_table_meta对象调用destroy_metas方法从数据库中删除元数据
            data_table_meta.destroy_metas()
        else:#drop为0则抛出错误
            return get_json_result(retcode=100,
                                   retmsg='The data table already exists.'
                                          'If you still want to continue uploading, please add the parameter -drop.'
                                          '1 means to add again after deleting the table')
    #以engine、options为参数，调用fate_arch.storage._session模块的build方法生成session
    with storage.Session.build(storage_engine=engine, options=request_data.get("options")) as storage_session:
        #通过session对象调用create_table方法新增table
        storage_session.create_table(address=address, name=name, namespace=namespace, partitions=request_data.get('partitions', None),
                                     hava_head=request_data.get("head"), id_delimiter=request_data.get("id_delimiter"), in_serialized=in_serialized)
    return get_json_result(data={"table_name": name, "namespace": namespace})


#删除table
@manager.route('/delete', methods=['post'])
def table_delete():
    request_data = request.json
    #将参数存入变量
    table_name = request_data.get('table_name')
    namespace = request_data.get('namespace')
    data = None
    # 以table_name、namespace为参数，调用fate_arch.storage._session模块的build方法生成storage_session
    with storage.Session.build(name=table_name, namespace=namespace) as storage_session:
        table = storage_session.get_table()#通过storage_session对象调用get_table方法获取table对象
        if table:
            table.destroy()#通过table对象调用destroy方法从数据库中删除table
            data = {'table_name': table_name, 'namespace': namespace}
    if data:
        return get_json_result(data=data)
    return get_json_result(retcode=101, retmsg='no find table')

#获取table列表
@manager.route('/list', methods=['post'])
def get_job_table_list():
    # 调用fate_flow.utils.detect_utils模块的check_config方法检查参数
    detect_utils.check_config(config=request.json, required_arguments=['job_id', 'role', 'party_id'])
    #调用fate_flow.utils.detect_utils模块的query_job方法获取job
    jobs = JobSaver.query_job(**request.json)
    if jobs:
        job = jobs[0]
        tables = get_job_all_table(job)#获取job中的table并返回
        return get_json_result(data=tables)
    else:
        return get_json_result(retcode=101, retmsg='no find job')

#获取table信息
@manager.route('/<table_func>', methods=['post'])
def table_api(table_func):
    config = request.json
    if table_func == 'table_info':
        table_key_count = 0
        table_partition = None
        table_schema = None
        table_name, namespace = config.get("name") or config.get("table_name"), config.get("namespace")
        #以table_name和namespace为参数，生成fate_arch.storage._table.StorageTableMeta类的对象table_meta
        table_meta = storage.StorageTableMeta(name=table_name, namespace=namespace)

        #table_meta不为空
        if table_meta:
            table_key_count = table_meta.get_count()
            table_partition = table_meta.get_partitions()
            table_schema = table_meta.get_schema()
            exist = 1
        else:
            exist = 0
        return get_json_result(data={"table_name": table_name,#table名
                                     "namespace": namespace,#命名空间
                                     "exist": exist,#是否存在
                                     "count": table_key_count,
                                     "partition": table_partition,
                                     "schema": table_schema})
    else:
        return get_json_result()


#获取job中的所有table，被get_job_table_list（获取table列表）方法调用
def get_job_all_table(job):
    #将job的dsl、runtime_conf、train_runtime_conf作为参数，调用fate_flow.utils.schedule_utils模块的get_job_dsl_parser方法得到dsl_parser对象
    dsl_parser = schedule_utils.get_job_dsl_parser(dsl=job.f_dsl,
                                                   runtime_conf=job.f_runtime_conf,
                                                   train_runtime_conf=job.f_train_runtime_conf
                                                   )
    #通过dsl_parser对象调用get_dsl_hierarchical_structure方法得到hierarchical_structure
    _, hierarchical_structure = dsl_parser.get_dsl_hierarchical_structure()
    component_table = {}
    #将job_id，role，party_id作为参数，调用fate_flow.operation.job_tracker模块的Tracker类的query_output_data_infos方法获取组件输出表
    component_output_tables = Tracker.query_output_data_infos(job_id=job.f_job_id, role=job.f_role,
                                                              party_id=job.f_party_id)
    #获取每个组件的输入table和输出table
    for component_name_list in hierarchical_structure:
        for component_name in component_name_list:
            component_table[component_name] = {}
            component_input_table = get_component_input_table(dsl_parser, job, component_name)#调用get_component_input_table方法
            component_table[component_name]['input'] = component_input_table
            component_table[component_name]['output'] = {}
            for output_table in component_output_tables:
                if output_table.f_component_name == component_name:
                    component_table[component_name]['output'][output_table.f_data_name] = \
                        {'name': output_table.f_table_name, 'namespace': output_table.f_table_namespace}
    return component_table

#获取组件输入table，被get_job_all_table（前一个方法）方法调用
def get_component_input_table(dsl_parser, job, component_name):
    #通过dsl_parser调用get_component_info获取component
    component = dsl_parser.get_component_info(component_name=component_name)
    if 'reader' in component_name:
        component_parameters = component.get_role_parameters()
        return component_parameters[job.f_role][0]['ReaderParam']

    task_input_dsl = component.get_input()#从component中获取input dsl

    #调用fate_flow.operation.task_executor模块的TaskExecutor类的get_job_args_on_party方法获取job中的args
    job_args_on_party = TaskExecutor.get_job_args_on_party(dsl_parser=dsl_parser,
                                                           job_runtime_conf=job.f_runtime_conf, role=job.f_role,
                                                           party_id=job.f_party_id)
    #调用fate_flow.utils.job_utils模块的get_job_parameters方法获取job_parameters得到config
    config = job_utils.get_job_parameters(job.f_job_id, job.f_role, job.f_party_id)
    task_parameters = RunParameters(**config)#以config为参数，生成fate_flow.entity.types.RunParameters类的对象task_parameters
    job_parameters = task_parameters

    #调用fate_flow.operation.task_executor模块的get_task_run_args方法获取组件输入table
    component_input_table = TaskExecutor.get_task_run_args(job_id=job.f_job_id, role=job.f_role,
                                                           party_id=job.f_party_id,
                                                           task_id=None,
                                                           task_version=None,
                                                           job_args=job_args_on_party,
                                                           job_parameters=job_parameters,
                                                           task_parameters=task_parameters,
                                                           input_dsl=task_input_dsl,
                                                           get_input_table=True
                                                           )
    return component_input_table