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
import os
import shutil

from flask import Flask, request

from fate_flow.entity.types import StatusSet
from fate_arch import storage
from fate_arch.common.base_utils import json_loads
from fate_flow.settings import stat_logger, UPLOAD_DATA_FROM_CLIENT
from fate_flow.utils.api_utils import get_json_result
from fate_flow.utils import detect_utils, job_utils
from fate_flow.scheduler.dag_scheduler import DAGScheduler
from fate_flow.operation.job_saver import JobSaver

manager = Flask(__name__)

# 服务器内部错误异常
@manager.errorhandler(500)
def internal_server_error(e):
    # 记录日志
    stat_logger.exception(e)
    return get_json_result(retcode=100, retmsg=str(e))


@manager.route('/<access_module>', methods=['post'])
def download_upload(access_module):
    job_id = job_utils.generate_job_id()##调用fate_flow\utils\job_utils模块的generate_job_id方法生成job_id
    ##自行上传数据文件，user_local_data==1
    if access_module == "upload" and UPLOAD_DATA_FROM_CLIENT and not (request.json and request.json.get("use_local_data") == 0):
        file = request.files['file']
        filename = os.path.join(job_utils.get_job_directory(job_id), 'fate_upload_tmp', file.filename)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        try:
            file.save(filename)##保存上传的文件
        except Exception as e:
            shutil.rmtree(os.path.join(job_utils.get_job_directory(job_id), 'fate_upload_tmp'))
            raise e
        job_config = request.args.to_dict()##将参数转为字典类型
        if "namespace" in job_config and "table_name" in job_config:##判断参数中是否存在namespace和table_name
            pass
        else:
            # higher than version 1.5.1, support eggroll run parameters
            job_config = json_loads(list(job_config.keys())[0])##取出第一个参数的key
        job_config['file'] = filename
    ##使用Fate服务器数据文件，use_local_data==0
    else:
        job_config = request.json
    required_arguments = ['work_mode', 'namespace', 'table_name']##required_arguments为需要的参数key集合
    if access_module == 'upload':
        required_arguments.extend(['file', 'head', 'partition'])
    elif access_module == 'download':
        required_arguments.extend(['output_path'])
    else:
        raise Exception('can not support this operating: {}'.format(access_module))
    detect_utils.check_config(job_config, required_arguments=required_arguments)#调用fate_flow/utils/detect_utils模块的check_config方法检查参数是否合法
    data = {}
    # compatibility
    if "table_name" in job_config:
        job_config["name"] = job_config["table_name"]
    if "backend" not in job_config:
        job_config["backend"] = 0 ##默认backend为eggroll
    for _ in ["work_mode", "backend", "head", "partition", "drop"]:
        if _ in job_config:
            job_config[_] = int(job_config[_])
    if access_module == "upload":
        if job_config.get('drop', 0) == 1:
            job_config["destroy"] = True
        else:
            job_config["destroy"] = False
        data['table_name'] = job_config["table_name"]
        data['namespace'] = job_config["namespace"]
        data_table_meta = storage.StorageTableMeta(name=job_config["table_name"], namespace=job_config["namespace"])##调用fate_arch/storage模块存储元数据
        if data_table_meta and not job_config["destroy"]:
            return get_json_result(retcode=100,
                                   retmsg='The data table already exists.'
                                          'If you still want to continue uploading, please add the parameter -drop.'
                                          ' 0 means not to delete and continue uploading, '
                                          '1 means to upload again after deleting the table')
    job_dsl, job_runtime_conf = gen_data_access_job_config(job_config, access_module)##生成dsl和conf
    submit_result = DAGScheduler.submit({'job_dsl': job_dsl, 'job_runtime_conf': job_runtime_conf}, job_id=job_id)##调用fate_flow/scheduler/dag_scheduler的submit方法提交job
    data.update(submit_result)
    return get_json_result(job_id=job_id, data=data)


@manager.route('/upload/history', methods=['POST'])
def upload_history():
    request_data = request.json
    #根据参数调用fate_flow/operation/job_saver模块的query_task方法获取使用upload组件的task列表
    if request_data.get('job_id'):
        tasks = JobSaver.query_task(component_name='upload_0', status=StatusSet.SUCCESS, job_id=request_data.get('job_id'), run_on_this_party=True)
    else:
        tasks = JobSaver.query_task(component_name='upload_0', status=StatusSet.SUCCESS, run_on_this_party=True)
    limit = request_data.get('limit')
    if not limit:
        tasks = tasks[-1::-1]#从最后一个元素往前读取所有元素
    else:
        tasks = tasks[-1:-limit - 1:-1]#从最后一个元素往前读取limit个元素
    jobs_run_conf = job_utils.get_job_configuration(None, None, None, tasks)##调用fate_flow/utils/job_utils模块的get_job_configuration方法获取task对应job的conf信息
    data = get_upload_info(jobs_run_conf=jobs_run_conf)#从job的conf信息中得到数据上传的信息
    return get_json_result(retcode=0, retmsg='success', data=data)


def get_upload_info(jobs_run_conf):
    data = []

    for job_id, job_run_conf in jobs_run_conf.items():
        info = {}
        table_name = job_run_conf["name"]
        namespace = job_run_conf["namespace"]
        table_meta = storage.StorageTableMeta(name=table_name, namespace=namespace)
        if table_meta:
            partition = job_run_conf["partition"]
            info["upload_info"] = {
                "table_name": table_name,
                "namespace": namespace,
                "partition": partition,
                'upload_count': table_meta.get_count()
            }
            info["notes"] = job_run_conf["notes"]
            info["schema"] = table_meta.get_schema()
            data.append({job_id: info})
    return data

##生成job_runtime_conf和job_dsl
def gen_data_access_job_config(config_data, access_module):
    ##job_runtime_conf
    job_runtime_conf = {
        "initiator": {},
        "job_parameters": {"common": {}},
        "role": {},
        "component_parameters": {"role": {"local": {"0": {}}}}
    }
    ##initiator
    initiator_role = "local"
    initiator_party_id = config_data.get('party_id', 0)##获取job_config中的party_id，不存在返回0
    job_runtime_conf["initiator"]["role"] = initiator_role
    job_runtime_conf["initiator"]["party_id"] = initiator_party_id
    ##job_parameters
    job_parameters_fields = {"work_mode", "backend", "task_cores", "eggroll_run", "spark_run"}
    for _ in job_parameters_fields:
        if _ in config_data:
            job_runtime_conf["job_parameters"]["common"][_] = config_data[_]
    ##role
    job_runtime_conf["role"][initiator_role] = [initiator_party_id]

    job_dsl = {
        "components": {}
    }

    if access_module == 'upload':
        parameters = {
                "head",
                "partition",
                "file",
                "namespace",
                "name",
                "delimiter",#分隔符
                "storage_engine",#存储引擎
                "storage_address",#存储地址
                "destroy",
            }
        job_runtime_conf["component_parameters"]["role"][initiator_role]["0"]["upload_0"] = {}
        for p in parameters:
            if p in config_data:
                job_runtime_conf["component_parameters"]["role"][initiator_role]["0"]["upload_0"][p] = config_data[p]
        job_runtime_conf['dsl_version'] = 2
        job_dsl["components"]["upload_0"] = {
            "module": "Upload"
        }

    if access_module == 'download':
        parameters = {
                "delimiter",
                "output_path",
                "namespace",
                "name"
        }
        job_runtime_conf["component_parameters"]['role'][initiator_role]["0"]["download_0"] = {}
        for p in parameters:
            if p in config_data:
                job_runtime_conf["component_parameters"]['role'][initiator_role]["0"]["download_0"][p] = config_data[p]
        job_runtime_conf['dsl_version'] = 2
        job_dsl["components"]["download_0"] = {
            "module": "Download"
        }

    return job_dsl, job_runtime_conf
