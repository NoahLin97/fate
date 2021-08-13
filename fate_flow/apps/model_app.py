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
import glob
import os
import shutil
import traceback

import peewee
import json
from copy import deepcopy
from datetime import date, datetime

from fate_arch.common.base_utils import json_loads, json_dumps
from fate_arch.common.file_utils import get_project_base_directory
from fate_flow.db.db_models import MachineLearningModelInfo as MLModel
from fate_flow.db.db_models import Tag, DB, ModelTag, ModelOperationLog as OperLog
from flask import Flask, request, send_file, Response

from fate_flow.pipelined_model.migrate_model import compare_roles
from fate_flow.pipelined_model.pipelined_model import PipelinedModel
from fate_flow.scheduler.dag_scheduler import DAGScheduler
from fate_flow.settings import stat_logger, MODEL_STORE_ADDRESS, TEMP_DIRECTORY
from fate_flow.pipelined_model import migrate_model, pipelined_model, publish_model, deploy_model
from fate_flow.utils.api_utils import get_json_result, federated_api, error_response
from fate_flow.utils import job_utils, model_utils, schedule_utils
from fate_flow.utils.service_utils import ServiceUtils
from fate_flow.utils.detect_utils import check_config
from fate_flow.utils.model_utils import gen_party_model_id, check_if_deployed
from fate_flow.entity.types import ModelOperation, TagOperation
from fate_arch.common import file_utils, WorkMode, FederatedMode
from fate_flow.utils.config_adapter import JobRuntimeConfigAdapter

manager = Flask(__name__)


@manager.errorhandler(500)
def internal_server_error(e):
    stat_logger.exception(e)
    return get_json_result(retcode=100, retmsg=str(e))


@manager.route('/load', methods=['POST'])
def load_model():
    request_config = request.json#获取参数，将参数放入变量request_config
    if request_config.get('job_id', None):##请求参数中包含job_id
        #将job_id作为参数，调用fate_flow/utils/model_utils模块的query_model_info方法获取job中的模型信息model_info
        retcode, retmsg, res_data = model_utils.query_model_info(model_version=request_config['job_id'], role='guest')#fate_flow/utils/model_utils.py通过model_version获取model信息
        if res_data:
            model_info = res_data[0]
            #从model_info中得到发起方的角色、id和runtime_conf，将得到的数据放入变量request_config
            request_config['initiator'] = {}
            request_config['initiator']['party_id'] = str(model_info.get('f_initiator_party_id'))
            request_config['initiator']['role'] = model_info.get('f_initiator_role')
            runtime_conf = model_info.get('f_runtime_conf', {}) if model_info.get('f_runtime_conf', {}) else model_info.get('f_train_runtime_conf', {})

            #将runtime_conf作为参数，调用fate_flow/utils/config_adapter模块得到adapter对象
            adapter = JobRuntimeConfigAdapter(runtime_conf)
            #通过adapter对象调用fate_flow/utils/config_adapter模块的get_common_parameters方法得到job的参数放入变量request_config
            job_parameters = adapter.get_common_parameters().to_dict()
            request_config['job_parameters'] = job_parameters if job_parameters else model_info.get('f_train_runtime_conf', {}).get('job_parameters')

            #从runtime_conf中得到role信息，将得到的数据放入变量request_config
            roles = runtime_conf.get('role')
            request_config['role'] = roles if roles else model_info.get('f_train_runtime_conf', {}).get('role')
            for key, value in request_config['role'].items():
                for i, v in enumerate(value):
                    value[i] = str(v)
            request_config.pop('job_id')
        else:
            return get_json_result(retcode=101,
                                   retmsg="model with version {} can not be found in database. "
                                          "Please check if the model version is valid.".format(request_config.get('job_id')))


    _job_id = job_utils.generate_job_id()#调用fate_flow/utils/job_utils模块的generate_job_id方法生成新的job_id
    initiator_party_id = request_config['initiator']['party_id']#发起方id
    initiator_role = request_config['initiator']['role']#发起方角色
    #将request_config作为参数，调用fate_flow\pipelined_model\publish_model模块的generate_publish_model_info方法生成发布模型的信息
    publish_model.generate_publish_model_info(request_config)
    load_status = True
    load_status_info = {}
    load_status_msg = 'success'
    load_status_info['detail'] = {}

    ##根据参数job_parameters中的work_mode处理变量request_config的job_parameters字段中的federated_mode字段
    if "federated_mode" not in request_config['job_parameters']:
        if request_config["job_parameters"]["work_mode"] == WorkMode.STANDALONE:
            request_config['job_parameters']["federated_mode"] = FederatedMode.SINGLE
        elif request_config["job_parameters"]["work_mode"] == WorkMode.CLUSTER:
            request_config['job_parameters']["federated_mode"] = FederatedMode.MULTIPLE

    for role_name, role_partys in request_config.get("role").items():
        if role_name == 'arbiter':
            continue
        load_status_info[role_name] = load_status_info.get(role_name, {})
        load_status_info['detail'][role_name] = {}
        for _party_id in role_partys:
            request_config['local'] = {'role': role_name, 'party_id': _party_id}
            try:
                #将request_config作为参数，循环调用fate_flow/utils/api_utils模块的federated_api方法执行模型加载
                response = federated_api(job_id=_job_id,
                                         method='POST',
                                         endpoint='/model/load/do',
                                         src_party_id=initiator_party_id,
                                         dest_party_id=_party_id,
                                         src_role = initiator_role,
                                         json_body=request_config,
                                         federated_mode=request_config['job_parameters']['federated_mode'])#fate_flow/utils/api_utils.py调用联邦接口
                load_status_info[role_name][_party_id] = response['retcode']
                detail = {_party_id: {}}
                detail[_party_id]['retcode'] = response['retcode']
                detail[_party_id]['retmsg'] = response['retmsg']
                load_status_info['detail'][role_name].update(detail)
                if response['retcode']:
                    load_status = False
                    load_status_msg = 'failed'
            except Exception as e:
                stat_logger.exception(e)
                load_status = False
                load_status_msg = 'failed'
                load_status_info[role_name][_party_id] = 100
    return get_json_result(job_id=_job_id, retcode=(0 if load_status else 101), retmsg=load_status_msg,
                           data=load_status_info)


# 迁移模型进程的api接口
@manager.route('/migrate', methods=['POST'])
def migrate_model_process():
    request_config = request.json#获取参数，将参数放入变量request_config
    _job_id = job_utils.generate_job_id()#调用fate_flow/utils/job_utils模块的generate_job_id方法生成job_id

    initiator_party_id = request_config['migrate_initiator']['party_id']
    initiator_role = request_config['migrate_initiator']['role']

    if not request_config.get("unify_model_version"):#判断参数unify_model_version是否为空，若为空则令unify_model_version=job_id
        request_config["unify_model_version"] = _job_id##迁移后的模型版本=job_id
    migrate_status = True
    migrate_status_info = {}
    migrate_status_msg = 'success'
    migrate_status_info['detail'] = {}

    require_arguments = ["migrate_initiator", "role", "migrate_role", "model_id",
                         "model_version", "execute_party", "job_parameters"]
    check_config(request_config, require_arguments)#调用fate_flow/utils/detect_utils模块的check_config方法检查参数

    try:
        #5调用fate_flow/pipelined/model/migrate_model模块的compare_roles方法对比迁移前的角色配置和迁移后的角色配置，若相同返回错误信息
        if compare_roles(request_config.get("migrate_role"), request_config.get("role")):
            return get_json_result(retcode=100,
                                   retmsg="The config of previous roles is the same with that of migrate roles. "
                                          "There is no need to migrate model. Migration process aborting.")
    except Exception as e:
        return get_json_result(retcode=100, retmsg=str(e))

    local_template = {
        "role": "",
        "party_id": "",
        "migrate_party_id": ""
    }

    res_dict = {}

    #为每一个执行迁移的参与方生成local_res，包含要模型迁移到的参与方id
    for role_name, role_partys in request_config.get("migrate_role").items():
        for offset, party_id in enumerate(role_partys):
            local_res = deepcopy(local_template)
            local_res["role"] = role_name##角色名，例如guest
            local_res["party_id"] = request_config.get("role").get(role_name)[offset]##参与训练的参与方id
            local_res["migrate_party_id"] = party_id##迁移后的参与方id
            if not res_dict.get(role_name):
                res_dict[role_name] = {}
            res_dict[role_name][local_res["party_id"]] = local_res

    for role_name, role_partys in request_config.get("execute_party").items():
        migrate_status_info[role_name] = migrate_status_info.get(role_name, {})
        migrate_status_info['detail'][role_name] = {}
        for party_id in role_partys:
            request_config["local"] = res_dict.get(role_name).get(party_id)##获取某个参与方的配置
            try:
                #7将request_config作为参数，循环调用fate_flow/utils/api_utils模块的federated_api方法执行模型迁移
                response = federated_api(job_id=_job_id,
                                         method='POST',
                                         endpoint='/model/migrate/do',
                                         src_party_id=initiator_party_id,
                                         dest_party_id=party_id,
                                         src_role=initiator_role,
                                         json_body=request_config,
                                         federated_mode=request_config['job_parameters']['federated_mode'])
                migrate_status_info[role_name][party_id] = response['retcode']
                detail = {party_id: {}}
                detail[party_id]['retcode'] = response['retcode']
                detail[party_id]['retmsg'] = response['retmsg']
                migrate_status_info['detail'][role_name].update(detail)
            except Exception as e:
                stat_logger.exception(e)
                migrate_status = False
                migrate_status_msg = 'failed'
                migrate_status_info[role_name][party_id] = 100
    return get_json_result(job_id=_job_id, retcode=(0 if migrate_status else 101),
                           retmsg=migrate_status_msg, data=migrate_status_info)

# 执行迁移模型的api接口
@manager.route('/migrate/do', methods=['POST'])
def do_migrate_model():
    request_data = request.json
    retcode, retmsg, data = migrate_model.migration(config_data=request_data)
    operation_record(request_data, "migrate", "success" if not retcode else "failed")
    return get_json_result(retcode=retcode, retmsg=retmsg, data=data)


@manager.route('/load/do', methods=['POST'])
def do_load_model():
    request_data = request.json
    adapter_servings_config(request_data)
    if not check_if_deployed(role=request_data['local']['role'],
                             party_id=request_data['local']['party_id'],
                             model_id=request_data['job_parameters']['model_id'],
                             model_version=request_data['job_parameters']['model_version']):##fate_flow.utils.model_utils检查模型是否部署
        return get_json_result(retcode=100,
                               retmsg="Only deployed models could be used to execute process of loading. "
                                      "Please deploy model before loading.")
    retcode, retmsg = publish_model.load_model(config_data=request_data)#fate_flow\pipelined_model\publish_model.py加载模型
    try:
        if not retcode:
            with DB.connection_context():#fate_flow.db.db_models
                model = MLModel.get_or_none(MLModel.f_role == request_data.get("local").get("role"),
                                            MLModel.f_party_id == request_data.get("local").get("party_id"),
                                            MLModel.f_model_id == request_data.get("job_parameters").get("model_id"),
                                            MLModel.f_model_version == request_data.get("job_parameters").get("model_version"))
                if model:
                    count = model.f_loaded_times
                    model.f_loaded_times = count + 1
                    model.save()
    except Exception as modify_err:
        stat_logger.exception(modify_err)

    try:
        party_model_id = gen_party_model_id(role=request_data.get("local").get("role"),
                                            party_id=request_data.get("local").get("party_id"),
                                            model_id=request_data.get("job_parameters").get("model_id"))#fate_flow.utils.model_utils
        src_model_path = os.path.join(file_utils.get_project_base_directory(), 'model_local_cache', party_model_id,
                                      request_data.get("job_parameters").get("model_version"))#fate_arch\common\file_utils.py
        dst_model_path = os.path.join(file_utils.get_project_base_directory(), 'loaded_model_backup',
                                      party_model_id, request_data.get("job_parameters").get("model_version"))
        if not os.path.exists(dst_model_path):
            shutil.copytree(src=src_model_path, dst=dst_model_path)
    except Exception as copy_err:
        stat_logger.exception(copy_err)
    operation_record(request_data, "load", "success" if not retcode else "failed")
    return get_json_result(retcode=retcode, retmsg=retmsg)


@manager.route('/bind', methods=['POST'])
def bind_model_service():
    request_config = request.json#获取参数，将参数放入变量request_config
    if request_config.get('job_id', None):#若请求参数包含job_id
        #将job_id作为参数，调用fate_flow / utils / model_utils模块的query_model_info方法获取job中的模型信息model_info
        retcode, retmsg, res_data = model_utils.query_model_info(model_version=request_config['job_id'], role='guest')
        if res_data:
            model_info = res_data[0]
            #从model_info中得到发起方的角色、id和runtime_conf，将得到的数据放入变量request_config
            request_config['initiator'] = {}
            request_config['initiator']['party_id'] = str(model_info.get('f_initiator_party_id'))
            request_config['initiator']['role'] = model_info.get('f_initiator_role')
            runtime_conf = model_info.get('f_runtime_conf', {}) if model_info.get('f_runtime_conf', {}) else model_info.get('f_train_runtime_conf', {})

            #将runtime_conf作为参数，调用fate_flow/utils/config_adapter模块得到adapter对象
            adapter = JobRuntimeConfigAdapter(runtime_conf)

            #通过adapter对象调用fate_flow/utils/config_adapter模块的get_common_parameters方法得到job的参数放入变量request_config
            job_parameters = adapter.get_common_parameters().to_dict()
            request_config['job_parameters'] = job_parameters if job_parameters else model_info.get('f_train_runtime_conf', {}).get('job_parameters')

            #从runtime_conf中得到role信息，将得到的数据放入变量request_config
            roles = runtime_conf.get('role')
            request_config['role'] = roles if roles else model_info.get('f_train_runtime_conf', {}).get('role')

            for key, value in request_config['role'].items():
                for i, v in enumerate(value):
                    value[i] = str(v)
            request_config.pop('job_id')
        else:
            return get_json_result(retcode=101,
                                   retmsg="model {} can not be found in database. "
                                          "Please check if the model version is valid.".format(request_config.get('job_id')))
    if not request_config.get('servings'):
        # get my party all servings
        adapter_servings_config(request_config)
    service_id = request_config.get('service_id')
    if not service_id:
        return get_json_result(retcode=101, retmsg='no service id')
    check_config(request_config, ['initiator', 'role', 'job_parameters'])#调用fate_flow/utils/detect_utils模块的check_config方法检查参数

    # 将request_config作为参数，调用fate_flow / pipelined_model / publish_model模块的bind_model_service方法绑定模型
    bind_status, retmsg = publish_model.bind_model_service(config_data=request_config)
    operation_record(request_config, "bind", "success" if not bind_status else "failed")
    return get_json_result(retcode=bind_status, retmsg='service id is {}'.format(service_id) if not retmsg else retmsg)


@manager.route('/transfer', methods=['post'])
def transfer_model():
    model_data = publish_model.download_model(request.json)#调用fate_flow/pipelined_model/publish_model模块的download_model方法下载模型信息
    return get_json_result(retcode=0, retmsg="success", data=model_data)


@manager.route('/<model_operation>', methods=['post', 'get'])
def operate_model(model_operation):
    request_config = request.json or request.form.to_dict()#获取参数，存入变量request_config
    job_id = job_utils.generate_job_id()#调用fate_flow/utils/job_utils模块的generate_job_id方法生成job_id
    if model_operation not in [ModelOperation.STORE, ModelOperation.RESTORE, ModelOperation.EXPORT, ModelOperation.IMPORT]:
        raise Exception('Can not support this operating now: {}'.format(model_operation))
    required_arguments = ["model_id", "model_version", "role", "party_id"]
    check_config(request_config, required_arguments=required_arguments)#调用fate_flow/utils/detect_utils模块的check_config方法检查参数
    #将model_id、role、party_id作为参数，调用fate_flow/utils/model_utils模块的gen_party_model_id方法生成mode_id，并替换request_config中的mode_id
    request_config["model_id"] = gen_party_model_id(model_id=request_config["model_id"], role=request_config["role"], party_id=request_config["party_id"])
    if model_operation in [ModelOperation.EXPORT, ModelOperation.IMPORT]:
        if model_operation == ModelOperation.IMPORT:
            try:
                #读取请求中的文件，并保存
                file = request.files.get('file')
                file_path = os.path.join(TEMP_DIRECTORY, file.filename)#TEMP_DIRECTORY/file.filename
                # if not os.path.exists(file_path):
                #     raise Exception('The file is obtained from the fate flow client machine, but it does not exist, '
                #                     'please check the path: {}'.format(file_path))
                try:
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    file.save(file_path)
                except Exception as e:
                    shutil.rmtree(file_path)
                    raise e
                request_config['file'] = file_path#将文件保存的目录存入request_config的file字段
                #将mode_id、model_version作为参数，调用fate_flow/pipelined_model/pipelined_model模块的PipelinedModel方法生成模型对象model
                model = pipelined_model.PipelinedModel(model_id=request_config["model_id"], model_version=request_config["model_version"])
                #通过model对象调用fate_flow/pipelined_model/pipelined_model模块的unpack_model方法解析模型文件
                model.unpack_model(file_path)

                #从pipeline中读取train_runtime_conf，并从train_runtime_conf中读取参与方角色
                pipeline = model.read_component_model('pipeline', 'pipeline')['Pipeline']
                train_runtime_conf = json_loads(pipeline.train_runtime_conf)
                permitted_party_id = []
                for key, value in train_runtime_conf.get('role', {}).items():
                    for v in value:
                        permitted_party_id.extend([v, str(v)])
                if request_config["party_id"] not in permitted_party_id:
                    shutil.rmtree(model.model_path)
                    raise Exception("party id {} is not in model roles, please check if the party id is valid.")
                try:
                    #将train_runtime_conf作为参数，调用fate_flow/utils/config_adapter模块获取一个adapter对象
                    adapter = JobRuntimeConfigAdapter(train_runtime_conf)
                    # 通过adapter对象调用fate_flow / utils / config_adapter模块的get_common_parameters方法得到job的参数job_paramters
                    job_parameters = adapter.get_common_parameters().to_dict()

                    #调用fate_flow/db/db_models模块连接模型数据库
                    with DB.connection_context():
                        db_model = MLModel.get_or_none(
                            MLModel.f_job_id == job_parameters.get("model_version"),
                            MLModel.f_role == request_config["role"]
                        )
                    if not db_model:
                        model_info = model_utils.gather_model_info_data(model)#将model对象作为参数，调用fate_flow/utils/model_utils模块的gather_model_info_data方法获取模型信息
                        model_info['imported'] = 1
                        model_info['job_id'] = model_info['f_model_version']
                        model_info['size'] = model.calculate_model_file_size()
                        model_info['role'] = request_config["model_id"].split('#')[0]
                        model_info['party_id'] = request_config["model_id"].split('#')[1]
                        if model_utils.compare_version(model_info['f_fate_version'], '1.5.1') == 'lt':
                            model_info['roles'] = model_info.get('f_train_runtime_conf', {}).get('role', {})
                            model_info['initiator_role'] = model_info.get('f_train_runtime_conf', {}).get('initiator', {}).get('role')
                            model_info['initiator_party_id'] = model_info.get('f_train_runtime_conf', {}).get( 'initiator', {}).get('party_id')
                            model_info['work_mode'] = adapter.get_job_work_mode()
                            model_info['parent'] = False if model_info.get('f_inference_dsl') else True
                        model_utils.save_model_info(model_info)#调用fate_flow/utils/model_utils模块的save_model_info方法保存模型信息
                    else:#若模型已经存在则返回错误
                        stat_logger.info(f'job id: {job_parameters.get("model_version")}, '
                                         f'role: {request_config["role"]} model info already existed in database.')
                except peewee.IntegrityError as e:
                    stat_logger.exception(e)
                operation_record(request_config, "import", "success")
                return get_json_result()
            except Exception:
                operation_record(request_config, "import", "failed")
                raise
        else:#模型输出
            try:
                #将mode_id、model_version作为参数，调用fate_flow/pipelined_model/pipelined_model模块的PipelinedModel方法生成模型对象model
                model = pipelined_model.PipelinedModel(model_id=request_config["model_id"], model_version=request_config["model_version"])
                if model.exists():
                    archive_file_path = model.packaging_model()#通过model对象调用packaging_model方法打包模型并返回模型文件
                    operation_record(request_config, "export", "success")
                    return send_file(archive_file_path, attachment_filename=os.path.basename(archive_file_path), as_attachment=True)
                else:
                    operation_record(request_config, "export", "failed")
                    res = error_response(response_code=210,
                                         retmsg="Model {} {} is not exist.".format(request_config.get("model_id"),
                                                                                    request_config.get("model_version")))
                    return res
            except Exception as e:
                operation_record(request_config, "export", "failed")
                stat_logger.exception(e)
                return error_response(response_code=210, retmsg=str(e))
    else:#模型存储、模型恢复
        data = {}
        #生成dsl和runtime_conf
        job_dsl, job_runtime_conf = gen_model_operation_job_config(request_config, model_operation)
        #调用fate_flow/scheduler/dag_scheduler模块的submit方法提交job
        submit_result = DAGScheduler.submit({'job_dsl': job_dsl, 'job_runtime_conf': job_runtime_conf}, job_id=job_id)
        data.update(submit_result)
        operation_record(data=job_runtime_conf, oper_type=model_operation, oper_status='')
        return get_json_result(job_id=job_id, data=data)


@manager.route('/model_tag/<operation>', methods=['POST'])
@DB.connection_context()
def tag_model(operation):
    if operation not in ['retrieve', 'create', 'remove']:
        return get_json_result(100, "'{}' is not currently supported.".format(operation))

    request_data = request.json
    model = MLModel.get_or_none(MLModel.f_model_version == request_data.get("job_id"))#以job_id为参数，调用fate_flow/db/db_models获取model对象
    if not model:
        raise Exception("Can not found model by job id: '{}'.".format(request_data.get("job_id")))

    if operation == 'retrieve':#检索模型tag
        res = {'tags': []}
        tags = (Tag.select().join(ModelTag, on=ModelTag.f_t_id == Tag.f_id).where(ModelTag.f_m_id == model.f_model_version))##连接模型数据库根据模型版本检索tag
        for tag in tags:
            res['tags'].append({'name': tag.f_name, 'description': tag.f_desc})
        res['count'] = tags.count()
        return get_json_result(data=res)
    elif operation == 'remove':##删除模型tag
        tag = Tag.get_or_none(Tag.f_name == request_data.get('tag_name'))#将tag_name作为参数，调用fate_flow/db/db_models模块检查数据库中是否存在名为tag_name的tag
        if not tag:#若不存在抛出异常
            raise Exception("Can not found '{}' tag.".format(request_data.get('tag_name')))
        tags = (Tag.select().join(ModelTag, on=ModelTag.f_t_id == Tag.f_id).where(ModelTag.f_m_id == model.f_model_version))##根据模型版本检索tag
        if tag.f_name not in [t.f_name for t in tags]:#若模型不拥有tag抛出异常
            raise Exception("Model {} {} does not have tag '{}'.".format(model.f_model_id,
                                                                         model.f_model_version,
                                                                         tag.f_name))
        delete_query = ModelTag.delete().where(ModelTag.f_m_id == model.f_model_version, ModelTag.f_t_id == tag.f_id)
        delete_query.execute()##根据模型版本和tag id删除对应的tag
        return get_json_result(retmsg="'{}' tag has been removed from tag list of model {} {}.".format(request_data.get('tag_name'),
                                                                                                       model.f_model_id,
                                                                                                       model.f_model_version))
    else:##模型tag创建
        if not str(request_data.get('tag_name')):
            raise Exception("Tag name should not be an empty string.")
        tag = Tag.get_or_none(Tag.f_name == request_data.get('tag_name'))#将tag_name作为参数，调用fate_flow/db/db_models模块检查数据库中是否存在
        if not tag:##若不存在则写入
            tag = Tag()
            tag.f_name = request_data.get('tag_name')
            tag.save(force_insert=True)
        else:#将模型版本作为参数，判断模型是否拥有该tag
            tags = (Tag.select().join(ModelTag, on=ModelTag.f_t_id == Tag.f_id).where(ModelTag.f_m_id == model.f_model_version))
            if tag.f_name in [t.f_name for t in tags]:##若模型被tag标记抛出异常
                raise Exception("Model {} {} already been tagged as tag '{}'.".format(model.f_model_id,
                                                                                      model.f_model_version,
                                                                                      tag.f_name))
        ModelTag.create(f_t_id=tag.f_id, f_m_id=model.f_model_version)#将模型版本和id存入tag中
        return get_json_result(retmsg="Adding {} tag for model with job id: {} successfully.".format(request_data.get('tag_name'),
                                                                                                     request_data.get('job_id')))


@manager.route('/tag/<tag_operation>', methods=['POST'])
@DB.connection_context()
def operate_tag(tag_operation):
    request_data = request.json
    if tag_operation not in [TagOperation.CREATE, TagOperation.RETRIEVE, TagOperation.UPDATE,
                             TagOperation.DESTROY, TagOperation.LIST]:#判断操作是否合法
        raise Exception('The {} operation is not currently supported.'.format(tag_operation))

    tag_name = request_data.get('tag_name')
    tag_desc = request_data.get('tag_desc')
    if tag_operation == TagOperation.CREATE:##tag创建
        try:
            if not tag_name:
                return get_json_result(100, "'{}' tag created failed. Please input a valid tag name.".format(tag_name))
            else:
                Tag.create(f_name=tag_name, f_desc=tag_desc)#以tag_name和tag_desc为参数，调用fate_flow.db.db_models模块在数据库中创建tag
        except peewee.IntegrityError:
            raise Exception("'{}' has already exists in database.".format(tag_name))
        else:
            return get_json_result("'{}' tag has been created successfully.".format(tag_name))

    elif tag_operation == TagOperation.LIST:##tag列表
        tags = Tag.select()#调用fate_flow.db.db_models模块从数据库获取tag列表
        limit = request_data.get('limit')
        res = {"tags": []}

        if limit > len(tags):
            count = len(tags)
        else:
            count = limit
        for tag in tags[:count]:
            res['tags'].append({'name': tag.f_name, 'description': tag.f_desc,
                                'model_count': ModelTag.filter(ModelTag.f_t_id == tag.f_id).count()})
        return get_json_result(data=res)

    else:##检索、更新、销毁
        if not (tag_operation == TagOperation.RETRIEVE and not request_data.get('with_model')):#操作不是检索或者参数中有with_model？？
            try:
                tag = Tag.get(Tag.f_name == tag_name)#以tag_name为参数，调用fate_flow.db.db_models模块从数据库获取tag
            except peewee.DoesNotExist:
                raise Exception("Can not found '{}' tag.".format(tag_name))

        if tag_operation == TagOperation.RETRIEVE:#tag检索
            if request_data.get('with_model', False):#有with_model参数
                res = {'models': []}
                #从数据库中查找该tag标记的模型
                models = (MLModel.select().join(ModelTag, on=ModelTag.f_m_id == MLModel.f_model_version).where(ModelTag.f_t_id == tag.f_id))
                for model in models:#获取模型信息
                        res["models"].append({
                        "model_id": model.f_model_id,
                        "model_version": model.f_model_version,
                        "model_size": model.f_size,
                        "role": model.f_role,
                        "party_id": model.f_party_id
                    })
                res["count"] = models.count()#计算模型数量
                return get_json_result(data=res)#返回模型的信息
            else:#参数不包含with_model
                tags = Tag.filter(Tag.f_name.contains(tag_name))#从数据库获取名称中包含tag_name的tag
                if not tags:
                    return get_json_result(100, retmsg="No tags found.")
                res = {'tags': []}
                for tag in tags:
                    res['tags'].append({'name': tag.f_name, 'description': tag.f_desc})
                return get_json_result(data=res)#返回tag的信息

        elif tag_operation == TagOperation.UPDATE:
            new_tag_name = request_data.get('new_tag_name', None)
            new_tag_desc = request_data.get('new_tag_desc', None)
            if (tag.f_name == new_tag_name) and (tag.f_desc == new_tag_desc):#判断新的名称、描述与原名称、描述是否相同，相同则抛出错误
                return get_json_result(100, "Nothing to be updated.")
            else:
                if request_data.get('new_tag_name'):
                    if not Tag.get_or_none(Tag.f_name == new_tag_name):##判断名称为new_tag_name的tag是否存在，存在着抛出错误
                        tag.f_name = new_tag_name
                    else:
                        return get_json_result(100, retmsg="'{}' tag already exists.".format(new_tag_name))

                tag.f_desc = new_tag_desc
                tag.save()
                return get_json_result(retmsg="Infomation of '{}' tag has been updated successfully.".format(tag_name))

        else:#tag删除
            delete_query = ModelTag.delete().where(ModelTag.f_t_id == tag.f_id)
            delete_query.execute()
            Tag.delete_instance(tag)#在数据库中执行删除操作
            return get_json_result(retmsg="'{}' tag has been deleted successfully.".format(tag_name))


def gen_model_operation_job_config(config_data: dict, model_operation: ModelOperation):
    job_runtime_conf = job_utils.runtime_conf_basic(if_local=True)
    initiator_role = "local"
    job_dsl = {
        "components": {}
    }

    if model_operation in [ModelOperation.STORE, ModelOperation.RESTORE]:
        component_name = "{}_0".format(model_operation)
        component_parameters = dict()
        component_parameters["model_id"] = [config_data["model_id"]]
        component_parameters["model_version"] = [config_data["model_version"]]
        component_parameters["store_address"] = [MODEL_STORE_ADDRESS]
        if model_operation == ModelOperation.STORE:
            component_parameters["force_update"] = [config_data.get("force_update", False)]
        job_runtime_conf["role_parameters"][initiator_role] = {component_name: component_parameters}
        job_dsl["components"][component_name] = {
            "module": "Model{}".format(model_operation.capitalize())
        }
    else:
        raise Exception("Can not support this model operation: {}".format(model_operation))
    return job_dsl, job_runtime_conf


@DB.connection_context()
def operation_record(data: dict, oper_type, oper_status):
    try:
        if oper_type == 'migrate':
            OperLog.create(f_operation_type=oper_type,
                           f_operation_status=oper_status,
                           f_initiator_role=data.get("migrate_initiator", {}).get("role"),
                           f_initiator_party_id=data.get("migrate_initiator", {}).get("party_id"),
                           f_request_ip=request.remote_addr,
                           f_model_id=data.get("model_id"),
                           f_model_version=data.get("model_version"))
        elif oper_type == 'load':
            OperLog.create(f_operation_type=oper_type,
                           f_operation_status=oper_status,
                           f_initiator_role=data.get("initiator").get("role"),
                           f_initiator_party_id=data.get("initiator").get("party_id"),
                           f_request_ip=request.remote_addr,
                           f_model_id=data.get('job_parameters').get("model_id"),
                           f_model_version=data.get('job_parameters').get("model_version"))
        elif oper_type == 'bind':
            OperLog.create(f_operation_type=oper_type,
                           f_operation_status=oper_status,
                           f_initiator_role=data.get("initiator").get("role"),
                           f_initiator_party_id=data.get("party_id") if data.get("party_id") else data.get("initiator").get("party_id"),
                           f_request_ip=request.remote_addr,
                           f_model_id=data.get("model_id") if data.get("model_id") else data.get('job_parameters').get("model_id"),
                           f_model_version=data.get("model_version") if data.get("model_version") else data.get('job_parameters').get("model_version"))
        else:
            OperLog.create(f_operation_type=oper_type,
                           f_operation_status=oper_status,
                           f_initiator_role=data.get("role") if data.get("role") else data.get("initiator").get("role"),
                           f_initiator_party_id=data.get("party_id") if data.get("party_id") else data.get("initiator").get("party_id"),
                           f_request_ip=request.remote_addr,
                           f_model_id=data.get("model_id") if data.get("model_id") else data.get('job_parameters').get("model_id"),
                           f_model_version=data.get("model_version") if data.get("model_version") else data.get('job_parameters').get("model_version"))
    except Exception:
        stat_logger.error(traceback.format_exc())


@manager.route('/query', methods=['POST'])
def query_model():
    #调用fate_flow\utils\model_utils模块的query_model_info方法获取模型
    retcode, retmsg, data = model_utils.query_model_info(**request.json)
    result = {"retcode": retcode, "retmsg": retmsg, "data": data}
    return Response(json.dumps(result, sort_keys=False, cls=DatetimeEncoder), mimetype="application/json")


@manager.route('/deploy', methods=['POST'])
def deploy():
    request_data = request.json
    require_parameters = ['model_id', 'model_version']
    check_config(request_data, require_parameters)#调用fate_flow.utils.detect_utils模块的check_config方法检查参数
    model_id = request_data.get("model_id")
    model_version = request_data.get("model_version")
    #将model_version和model_id作为参数，调用fate_flow\utils\model_utils模块的query_model_info_from_file方法获取模型信息model_info
    retcode, retmsg, model_info = model_utils.query_model_info_from_file(model_id=model_id, model_version=model_version, to_dict=True)
    if not model_info:
        raise Exception(f'Deploy model failed, no model {model_id} {model_version} found.')
    else:
        #遍历model_info中每个key、value，找到key中发起方角色、id和value中发起方角色、id相同的(key,value)对
        for key, value in model_info.items():
            version_check = model_utils.compare_version(value.get('f_fate_version'), '1.5.0')##检查fate版本
            if version_check == 'lt':
                continue
            else:
                #从key中获取发起方角色和id
                init_role = key.split('/')[-2].split('#')[0]
                init_party_id = key.split('/')[-2].split('#')[1]
                #从value中获取发起方角色和id
                model_init_role = value.get('f_initiator_role') if value.get('f_initiator_role') else value.get('f_train_runtime_conf', {}).get('initiator', {}).get('role', '')
                model_init_party_id = value.get('f_initiator_role_party_id') if value.get('f_initiator_role_party_id') else value.get('f_train_runtime_conf', {}).get('initiator', {}).get('party_id', '')
                if (init_role == model_init_role) and (init_party_id == str(model_init_party_id)):##判断从key中获取的发起方角色、id与value中获取的发起方角色、id是否相同
                    break
        else:#每个key中获取的发起方角色、id与value中获取的发起方角色、id都不相同
            raise Exception("Deploy model failed, can not found model of initiator role or the fate version of model is older than 1.5.0")

        # distribute federated deploy task
        _job_id = job_utils.generate_job_id()#调用fate_flow\utils\job_utils模块的generate_job_id方法生成job_id
        request_data['child_model_version'] = _job_id

        initiator_party_id = model_init_party_id
        initiator_role = model_init_role
        request_data['initiator'] = {'role': initiator_role, 'party_id': initiator_party_id}
        deploy_status = True
        deploy_status_info = {}
        deploy_status_msg = 'success'
        deploy_status_info['detail'] = {}

        for role_name, role_partys in value.get("f_train_runtime_conf", {}).get('role', {}).items():
            if role_name not in ['arbiter', 'host', 'guest']:
                continue
            deploy_status_info[role_name] = deploy_status_info.get(role_name, {})
            deploy_status_info['detail'][role_name] = {}
            adapter = JobRuntimeConfigAdapter(value.get("f_train_runtime_conf", {}))#将从value得到的f_train_runtime_conf作为参数，调用fate_flow.utils.config_adapter模块生成adapter对象
            work_mode = adapter.get_job_work_mode()#通过adapter对象调用get_job_work_mode方法获取work_mode

            for _party_id in role_partys:
                request_data['local'] = {'role': role_name, 'party_id': _party_id}
                try:#对于f_train_runtime_conf中的每一方，调用fate_flow.utils.api_utils模块的federated_api方法发起联邦部署任务
                    response = federated_api(job_id=_job_id,
                                             method='POST',
                                             endpoint='/model/deploy/do',
                                             src_party_id=initiator_party_id,
                                             dest_party_id=_party_id,
                                             src_role = initiator_role,
                                             json_body=request_data,
                                             federated_mode=FederatedMode.MULTIPLE if work_mode else FederatedMode.SINGLE)
                    deploy_status_info[role_name][_party_id] = response['retcode']
                    detail = {_party_id: {}}
                    detail[_party_id]['retcode'] = response['retcode']
                    detail[_party_id]['retmsg'] = response['retmsg']
                    deploy_status_info['detail'][role_name].update(detail)
                    if response['retcode']:
                        deploy_status = False
                        deploy_status_msg = 'failed'
                except Exception as e:
                    stat_logger.exception(e)
                    deploy_status = False
                    deploy_status_msg = 'failed'
                    deploy_status_info[role_name][_party_id] = 100

        deploy_status_info['model_id'] = request_data['model_id']
        deploy_status_info['model_version'] = _job_id
        return get_json_result(retcode=(0 if deploy_status else 101),
                               retmsg=deploy_status_msg, data=deploy_status_info)

# 部署的api接口
@manager.route('/deploy/do', methods=['POST'])
def do_deploy():
    retcode, retmsg = deploy_model.deploy(request.json)
    operation_record(request.json, "deploy", "success" if not retcode else "failed")
    return get_json_result(retcode=retcode, retmsg=retmsg)


@manager.route('/get/predict/dsl', methods=['POST'])
def get_predict_dsl():
    request_data = request.json
    request_data['query_filters'] = ['inference_dsl']
    retcode, retmsg, data = model_utils.query_model_info_from_file(**request_data)#调用fate_flow.utils.model_utils模块的query_model_info_from_file方法获取模型信息
    if data:
        if request_data.get("filename"):
            os.makedirs(TEMP_DIRECTORY, exist_ok=True)
            temp_filepath = os.path.join(TEMP_DIRECTORY, request_data.get("filename"))
            with open(temp_filepath, "w") as fout:
                fout.write(json_dumps(data[0]['f_inference_dsl'], indent=4))
            return send_file(open(temp_filepath, "rb"), as_attachment=True,
                             attachment_filename=request_data.get("filename"))
        else:
            return get_json_result(data=data[0]['f_inference_dsl'])
    return error_response(210, "No model found, please check if arguments are specified correctly.")



@manager.route('/get/predict/conf', methods=['POST'])
def get_predict_conf():
    request_data = request.json
    required_parameters = ['model_id', 'model_version']
    check_config(request_data, required_parameters)#调用fate_flow.utils.detect_utils模块的check_config方法检查参数
    model_dir = os.path.join(get_project_base_directory(), 'model_local_cache')#调用fate_arch.common.file_utils模块的get_project_base_directory方法生成模型路径model_dir
    model_fp_list = glob.glob(model_dir + f"/guest#*#{request_data['model_id']}/{request_data['model_version']}")#将model_dir、model_id、model_version作为参数，生成model_fp_list
    if model_fp_list:
        fp = model_fp_list[0]#获取model_fp_list[0]，从中获取model_id和model_version
        pipeline_model = PipelinedModel(model_id=fp.split('/')[-2], model_version=fp.split('/')[-1])#将得到的model_id和model_version作为参数，调用fate_flow.pipelined_model.pipelined_model模块生成pipeline_model对象
        #通过pipeline_model对象调用read_component_model方法得到pipeline
        pipeline = pipeline_model.read_component_model('pipeline', 'pipeline')['Pipeline']
        #从pipeline获取predict_dsl和train_runtime_conf
        predict_dsl = json_loads(pipeline.inference_dsl)

        train_runtime_conf = json_loads(pipeline.train_runtime_conf)
        parser = schedule_utils.get_dsl_parser_by_version(train_runtime_conf.get('dsl_version', '1') )#调用fate_flow.utils.schedule_utils模块的get_dsl_parser_by_version方法得到parser对象
        predict_conf = parser.generate_predict_conf_template(predict_dsl=predict_dsl, train_conf=train_runtime_conf,
                                                     model_id=request_data['model_id'],
                                                     model_version=request_data['model_version'])#将predict dsl、train_runtime_conf、model_version、model_id作为参数，通过parser对象调用generate_predict_conf_template方法生成predict_conf
    else:
        predict_conf = ''
    if predict_conf:
        if request_data.get("filename"):#存储到文件
            os.makedirs(TEMP_DIRECTORY, exist_ok=True)
            temp_filepath = os.path.join(TEMP_DIRECTORY, request_data.get("filename"))
            with open(temp_filepath, "w") as fout:

                fout.write(json_dumps(predict_conf, indent=4))
            return send_file(open(temp_filepath, "rb"), as_attachment=True,
                             attachment_filename=request_data.get("filename"))
        else:
            return get_json_result(data=predict_conf)
    return error_response(210, "No model found, please check if arguments are specified correctly.")


def adapter_servings_config(request_data):
    servings_conf = ServiceUtils.get("servings", {})
    if isinstance(servings_conf, dict):
        request_data["servings"] = servings_conf.get('hosts', [])
    elif isinstance(servings_conf, list):
        request_data["servings"] = servings_conf
    else:
        raise Exception('Please check the servings config')


class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)