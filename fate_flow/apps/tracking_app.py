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
import io
import json
import os
import shutil
import tarfile

from flask import Flask, request, send_file, jsonify
from google.protobuf import json_format

from fate_arch.common.base_utils import fate_uuid
from fate_arch import storage
from fate_flow.db.db_models import Job, DB
from fate_flow.manager.data_manager import delete_metric_data
from fate_flow.operation.job_tracker import Tracker
from fate_flow.operation.job_saver import JobSaver
from fate_flow.scheduler.federated_scheduler import FederatedScheduler
from fate_flow.settings import stat_logger, TEMP_DIRECTORY
from fate_flow.utils import job_utils, data_utils, detect_utils, schedule_utils
from fate_flow.utils.api_utils import get_json_result, error_response
from fate_flow.utils.config_adapter import JobRuntimeConfigAdapter
from federatedml.feature.instance import Instance

manager = Flask(__name__)


@manager.errorhandler(500)
def internal_server_error(e):
    stat_logger.exception(e)
    return get_json_result(retcode=100, retmsg=str(e))


# 获取指定job的data_view
@manager.route('/job/data_view', methods=['post'])
def job_view():
    request_data = request.json
    check_request_parameters(request_data)#检查参数
    #以job_id，role，party_id为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象job_tracker
    job_tracker = Tracker(job_id=request_data['job_id'], role=request_data['role'], party_id=request_data['party_id'])

    #通过job_tracker对象调用get_job_view方法得到job_view_data
    job_view_data = job_tracker.get_job_view()
    if job_view_data:
        ##通过job_tracker对象调用get_metric_list方法得到job_metric_list
        job_metric_list = job_tracker.get_metric_list(job_level=True)
        #在job_view_data中加入model_summary字段
        job_view_data['model_summary'] = {}
        #用job_metric_list变量处理model_summary字段
        for metric_namespace, namespace_metrics in job_metric_list.items():
            job_view_data['model_summary'][metric_namespace] = job_view_data['model_summary'].get(metric_namespace, {})
            for metric_name in namespace_metrics:
                job_view_data['model_summary'][metric_namespace][metric_name] = job_view_data['model_summary'][
                    metric_namespace].get(metric_name, {})
                for metric_data in job_tracker.get_job_metric_data(metric_namespace=metric_namespace,
                                                                   metric_name=metric_name):
                    job_view_data['model_summary'][metric_namespace][metric_name][metric_data.key] = metric_data.value
        #返回job_view_data
        return get_json_result(retcode=0, retmsg='success', data=job_view_data)
    else:
        return get_json_result(retcode=101, retmsg='error')

#获取job中指定component的所有metric的数据
@manager.route('/component/metric/all', methods=['post'])
def component_metric_all():
    request_data = request.json
    check_request_parameters(request_data)#检查参数
    #以job_id，role，party_id，component_name为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=request_data['job_id'], component_name=request_data['component_name'],
                      role=request_data['role'], party_id=request_data['party_id'])
    metrics = tracker.get_metric_list()#通过tracker对象调用get_metric_list方法得到metric列表
    all_metric_data = {}
    if metrics:
        #处理metrics，将结果存入all_metric_data
        for metric_namespace, metric_names in metrics.items():#获取metrics中每一项的metric_namespace和metric_names
            all_metric_data[metric_namespace] = all_metric_data.get(metric_namespace, {})
            for metric_name in metric_names:
                all_metric_data[metric_namespace][metric_name] = all_metric_data[metric_namespace].get(metric_name, {})
                #以tracker对象、metric_name、metric_namespace为参数，调用get_metric_all_data方法获取metric_data, metric_meta
                metric_data, metric_meta = get_metric_all_data(tracker=tracker, metric_namespace=metric_namespace,
                                                               metric_name=metric_name)
                all_metric_data[metric_namespace][metric_name]['data'] = metric_data
                all_metric_data[metric_namespace][metric_name]['meta'] = metric_meta

        #返回all_metric_data
        return get_json_result(retcode=0, retmsg='success', data=all_metric_data)
    else:
        return get_json_result(retcode=0, retmsg='no data', data={})

#获取job中指定component的所有metric
@manager.route('/component/metrics', methods=['post'])
def component_metrics():
    request_data = request.json
    check_request_parameters(request_data)#检查参数

    # 以job_id，role，party_id，component_name为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=request_data['job_id'], component_name=request_data['component_name'],
                      role=request_data['role'], party_id=request_data['party_id'])
    metrics = tracker.get_metric_list()#通过tracker对象调用get_metric_list方法得到metrics
    if metrics:
        #返回metrics
        return get_json_result(retcode=0, retmsg='success', data=metrics)
    else:
        return get_json_result(retcode=0, retmsg='no data', data={})


#获取job中指定component的指定metric的数据
@manager.route('/component/metric_data', methods=['post'])
def component_metric_data():
    request_data = request.json
    check_request_parameters(request_data)#检查参数
    # 以job_id，role，party_id，component_name为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=request_data['job_id'], component_name=request_data['component_name'],
                      role=request_data['role'], party_id=request_data['party_id'])
    #以tracker对象、metric_name、metric_namespace为参数，调用get_metric_all_data方法获取metric_data, metric_meta
    metric_data, metric_meta = get_metric_all_data(tracker=tracker, metric_namespace=request_data['metric_namespace'],
                                                   metric_name=request_data['metric_name'])
    if metric_data or metric_meta:
        #返回metric_data, metric_meta
        return get_json_result(retcode=0, retmsg='success', data=metric_data,
                               meta=metric_meta)
    else:
        return get_json_result(retcode=0, retmsg='no data', data=[], meta={})

#获取metric_data和metric_meta，被component_metric_all方法、component_metric_data方法调用
def get_metric_all_data(tracker, metric_namespace, metric_name):
    #以metric_namespace和metric_name为参数，通过tracker对象分别调用get_metric_data和get_metric_meta方法获取metric_data和metric_meta
    metric_data = tracker.get_metric_data(metric_namespace=metric_namespace,
                                          metric_name=metric_name)
    metric_meta = tracker.get_metric_meta(metric_namespace=metric_namespace,
                                          metric_name=metric_name)
    if metric_data or metric_meta:
        metric_data_list = [(metric.key, metric.value) for metric in metric_data]
        metric_data_list.sort(key=lambda x: x[0])
        return metric_data_list, metric_meta.to_dict() if metric_meta else {}
    else:
        return [], {}

#删除指定job的metric数据
@manager.route('/component/metric/delete', methods=['post'])
def component_metric_delete():
    #调用fate_flow.manager.data_manager模块的delete_metric_data方法执行删除操作
    sql = delete_metric_data(request.json)
    return get_json_result(retcode=0, retmsg='success', data=sql)

#获取job中指定component的parameters
@manager.route('/component/parameters', methods=['post'])
def component_parameters():
    request_data = request.json
    check_request_parameters(request_data)#检查参数
    job_id = request_data.get('job_id', '')
    #以job_id为参数，调用fate_flow.utils.schedule_utils模块的get_job_dsl_parser_by_job_id方法得到job_dsl_parser对象
    job_dsl_parser = schedule_utils.get_job_dsl_parser_by_job_id(job_id=job_id)
    if job_dsl_parser:
        #以component_name为参数，通过job_dsl_parser对象调用get_component_info方法得到对应component
        component = job_dsl_parser.get_component_info(request_data['component_name'])

        #通过component调用get_role_parameters方法得到参数parameters
        parameters = component.get_role_parameters()

        #循环遍历parameters，将结果放入output_parameters
        for role, partys_parameters in parameters.items():
            for party_parameters in partys_parameters:
                if party_parameters.get('local', {}).get('role', '') == request_data['role'] and party_parameters.get(
                        'local', {}).get('party_id', '') == int(request_data['party_id']):
                    output_parameters = {}
                    output_parameters['module'] = party_parameters.get('module', '')
                    for p_k, p_v in party_parameters.items():
                        if p_k.endswith('Param'):
                            output_parameters[p_k] = p_v
                    #返回output_parameters
                    return get_json_result(retcode=0, retmsg='success', data=output_parameters)
        else:
            return get_json_result(retcode=0, retmsg='can not found this component parameters')
    else:
        return get_json_result(retcode=101, retmsg='can not found this job')

#获取job中指定component的输出模型
@manager.route('/component/output/model', methods=['post'])
def component_output_model():
    request_data = request.json
    check_request_parameters(request_data)#检查参数
    #以job_id、role、party_id为参数，调用fate_flow.utils.job_utils模块的get_job_configuration方法
    job_dsl, job_runtime_conf, runtime_conf_on_party, train_runtime_conf = job_utils.get_job_configuration(job_id=request_data['job_id'],
                                                                                                           role=request_data['role'],
                                                                                                           party_id=request_data['party_id'])
    try:
        #从runtime_conf_on_party中读取model_id和model_version
        model_id = runtime_conf_on_party['job_parameters']['model_id']
        model_version = runtime_conf_on_party['job_parameters']['model_version']
    except Exception as e:
        job_dsl, job_runtime_conf, train_runtime_conf = job_utils.get_model_configuration(job_id=request_data['job_id'],
                                                                                          role=request_data['role'],
                                                                                          party_id=request_data['party_id'])
        if any([job_dsl, job_runtime_conf, train_runtime_conf]):
            adapter = JobRuntimeConfigAdapter(job_runtime_conf)
            model_id = adapter.get_common_parameters().to_dict().get('model_id')
            model_version = adapter.get_common_parameters().to_dict.get('model_version')
        else:
            stat_logger.exception(e)
            stat_logger.error(f"Can not find model info by filters: job id: {request_data.get('job_id')}, "
                              f"role: {request_data.get('role')}, party id: {request_data.get('party_id')}")
            raise Exception(f"Can not find model info by filters: job id: {request_data.get('job_id')}, "
                            f"role: {request_data.get('role')}, party id: {request_data.get('party_id')}")

    #以job_id、component_name、role、party_id、model_id、model_version为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=request_data['job_id'], component_name=request_data['component_name'],
                      role=request_data['role'], party_id=request_data['party_id'], model_id=model_id,
                      model_version=model_version)
    #以job_dsl、job_runtime_conf、train_runtime_conf为参数，调用fate_flow.utils.schedule_utils模块的get_job_dsl_parser得到dag
    dag = schedule_utils.get_job_dsl_parser(dsl=job_dsl, runtime_conf=job_runtime_conf,
                                            train_runtime_conf=train_runtime_conf)
    #以component_name为参数，通过dag对象调用get_component_info方法得到component
    component = dag.get_component_info(request_data['component_name'])
    output_model_json = {}
    # There is only one model output at the current dsl version.
    #通过tracker对象调用get_output_model方法获取输出模型output_model
    output_model = tracker.get_output_model(component.get_output()['model'][0] if component.get_output().get('model') else 'default')
    #遍历output_model，得到output_model_json
    for buffer_name, buffer_object in output_model.items():
        if buffer_name.endswith('Param'):
            output_model_json = json_format.MessageToDict(buffer_object, including_default_value_fields=True)
    if output_model_json:
        component_define = tracker.get_component_define()#通过tracker对象调用get_component_define方法得到component_define
        this_component_model_meta = {}
        #遍历output_model_json，将结果放入this_component_model_meta
        for buffer_name, buffer_object in output_model.items():
            if buffer_name.endswith('Meta'):
                this_component_model_meta['meta_data'] = json_format.MessageToDict(buffer_object,
                                                                                   including_default_value_fields=True)
        this_component_model_meta.update(component_define)
        #返回this_component_model_meta和output_model_json
        return get_json_result(retcode=0, retmsg='success', data=output_model_json, meta=this_component_model_meta)
    else:
        return get_json_result(retcode=0, retmsg='no data', data={})

#获取job的指定component的输出data
@manager.route('/component/output/data', methods=['post'])
def component_output_data():
    request_data = request.json
    #调用get_component_output_tables_meta方法获取组件输出表元数据output_tables_meta
    output_tables_meta = get_component_output_tables_meta(task_data=request_data)
    if not output_tables_meta:
        return get_json_result(retcode=0, retmsg='no data', data=[])
    output_data_list = []
    headers = []
    totals = []
    data_names = []
    #遍历output_tables_meta
    for output_name, output_table_meta in output_tables_meta.items():
        output_data = []
        num = 100
        have_data_label = False
        is_str = False
        have_weight = False
        if output_table_meta:
            # part_of_data format: [(k, v)]
            for k, v in output_table_meta.get_part_of_data():#从output_table_meta中获取data，并遍历
                if num == 0:
                    break
                #将k, v作为参数，调用get_component_output_data_line方法
                data_line, have_data_label, is_str, have_weight = get_component_output_data_line(src_key=k, src_value=v)
                #将data_line存入output_data
                output_data.append(data_line)
                num -= 1
            total = output_table_meta.get_count()#通过output_table_meta调用get_count方法得到数量total
            output_data_list.append(output_data)#将output_data存入output_data_list
            data_names.append(output_name)#将output_name存入data_names
            totals.append(total)#将total存入totals
        if output_data:
            #调用get_component_output_data_schema方法
            header = get_component_output_data_schema(output_table_meta=output_table_meta, have_data_label=have_data_label, is_str=is_str, have_weight=have_weight)
            headers.append(header)#将header存入headers
        else:
            headers.append(None)
    if len(output_data_list) == 1 and not output_data_list[0]:
        return get_json_result(retcode=0, retmsg='no data', data=[])
    #返回output_data_list、headers、totals、data_names
    return get_json_result(retcode=0, retmsg='success', data=output_data_list, meta={'header': headers, 'total': totals, 'names':data_names})


#下载job的指定component的data
@manager.route('/component/output/data/download', methods=['get'])
def component_output_data_download():
    request_data = request.json
    try:
        #调用get_component_output_tables_meta方法得到output_tables_meta
        output_tables_meta = get_component_output_tables_meta(task_data=request_data)
    except Exception as e:
        stat_logger.exception(e)
        return error_response(210, str(e))
    limit = request_data.get('limit', -1)
    if not output_tables_meta:
        return error_response(response_code=210, retmsg='no data')
    if limit == 0:
        return error_response(response_code=210, retmsg='limit is 0')
    have_data_label = False
    have_weight = False

    output_data_file_list = []
    output_data_meta_file_list = []
    output_tmp_dir = os.path.join(os.getcwd(), 'tmp/{}'.format(fate_uuid()))#新建output_tmp_dir目录，fate_arch.common.base_utils
    #遍历output_tables_meta，生成文件
    for output_name, output_table_meta in output_tables_meta.items():
        output_data_count = 0
        is_str = False
        #.csv路径output_data_file_path
        output_data_file_path = "{}/{}.csv".format(output_tmp_dir, output_name)
        os.makedirs(os.path.dirname(output_data_file_path), exist_ok=True)
        with open(output_data_file_path, 'w') as fw:#打开文件
            #调用fate_arch.storage._session模块的Session类的build方法得到storage_session对象
            with storage.Session.build(name=output_table_meta.get_name(), namespace=output_table_meta.get_namespace()) as storage_session:
                output_table = storage_session.get_table()#通过storage_session对象调用get_table方法得到output_table
                #遍历output_table
                for k, v in output_table.collect():
                    #以k，v为参数，调用get_component_output_data_line方法
                    data_line, have_data_label, is_str, have_weight = get_component_output_data_line(src_key=k, src_value=v)
                    fw.write('{}\n'.format(','.join(map(lambda x: str(x), data_line))))#data_line写入文件
                    output_data_count += 1
                    if output_data_count == limit:
                        break

        if output_data_count:
            # get meta
            output_data_file_list.append(output_data_file_path)#output_data_file_path存入output_data_file_list
            #调用get_component_output_data_schema方法得到header
            header = get_component_output_data_schema(output_table_meta=output_table_meta, have_data_label=have_data_label,
                                                      is_str=is_str, have_weight=have_weight)
            output_data_meta_file_path = "{}/{}.meta".format(output_tmp_dir, output_name)#.meta路径
            output_data_meta_file_list.append(output_data_meta_file_path)#output_metafile_path存入output_meta_file_list
            with open(output_data_meta_file_path, 'w') as fw:#打开文件
                json.dump({'header': header}, fw, indent=4)#写入header
            if request_data.get('head', True) and header:
                with open(output_data_file_path, 'r+') as f:
                    content = f.read()
                    f.seek(0, 0)
                    f.write('{}\n'.format(','.join(header)) + content)
    # tar
    #压缩文件并返回
    memory_file = io.BytesIO()
    tar = tarfile.open(fileobj=memory_file, mode='w:gz')
    for index in range(0, len(output_data_file_list)):
        tar.add(output_data_file_list[index], os.path.relpath(output_data_file_list[index], output_tmp_dir))
        tar.add(output_data_meta_file_list[index], os.path.relpath(output_data_meta_file_list[index], output_tmp_dir))
    tar.close()
    memory_file.seek(0)
    output_data_file_list.extend(output_data_meta_file_list)
    for path in output_data_file_list:
        try:
            shutil.rmtree(os.path.dirname(path))
        except Exception as e:
            # warning
            stat_logger.warning(e)
        tar_file_name = 'job_{}_{}_{}_{}_output_data.tar.gz'.format(request_data['job_id'],
                                                                    request_data['component_name'],
                                                                    request_data['role'], request_data['party_id'])
        return send_file(memory_file, attachment_filename=tar_file_name, as_attachment=True)

#获取job的指定component的data table
@manager.route('/component/output/data/table', methods=['post'])
def component_output_data_table():
    request_data = request.json
    #调用fate_flow.utils.detect_utils模块的check_config方法检查参数
    detect_utils.check_config(config=request_data, required_arguments=['job_id', 'role', 'party_id', 'component_name'])
    #调用fate_flow.operation.job_saver模块的JobSaver类的query_job方法得到job对象
    jobs = JobSaver.query_job(job_id=request_data.get('job_id'))
    if jobs:
        job = jobs[0]
        #以job、request_data和命令为参数，调用fate_flow.scheduler.federated_scheduler模块的FederatedScheduler类的tracker_command方法
        #得到结果并返回
        return jsonify(FederatedScheduler.tracker_command(job, request_data, 'output/table'))
    else:
        return get_json_result(retcode=100, retmsg='No found job')


#下载job中指定component的summary
@manager.route('/component/summary/download', methods=['POST'])
def get_component_summary():
    request_data = request.json
    try:
        required_params = ["job_id", "component_name", "role", "party_id"]
        detect_utils.check_config(request_data, required_params)#调用fate_flow.utils.detect_utils模块的check_config方法检查参数

        #以job_id，component_name、role、party_id、task_id、task_version为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
        tracker = Tracker(job_id=request_data["job_id"], component_name=request_data["component_name"],
                          role=request_data["role"], party_id=request_data["party_id"],
                          task_id=request_data.get("task_id", None), task_version=request_data.get("task_version", None))
        summary = tracker.read_summary_from_db()#通过tracker对象调用read_summary_from_db从数据库读取summary
        if summary:
            if request_data.get("filename"):#如果参数包含filename，则将结果写入文件并返回文件
                temp_filepath = os.path.join(TEMP_DIRECTORY, request_data.get("filename"))
                with open(temp_filepath, "w") as fout:
                    fout.write(json.dumps(summary, indent=4))#写入文件
                return send_file(open(temp_filepath, "rb"), as_attachment=True,
                                 attachment_filename=request_data.get("filename"))
            else:
                return get_json_result(data=summary)#返回数据
        return error_response(210, "No component summary found, please check if arguments are specified correctly.")
    except Exception as e:
        stat_logger.exception(e)
        return error_response(210, str(e))

#获取job的组件列表
@manager.route('/component/list', methods=['POST'])
def component_list():
    request_data = request.json
    #以job_id为参数，调用fate_flow.utils.schedule_utils模块的get_job_dsl_parser_by_job_id方法得到parser对象
    parser = schedule_utils.get_job_dsl_parser_by_job_id(job_id=request_data.get('job_id'))
    if parser:
        #通过parser对象调用get_dsl()方法获取dsl，并从dsl中得到组件列表后返回结果
        return get_json_result(data={'components': list(parser.get_dsl().get('components').keys())})
    else:
        return get_json_result(retcode=100, retmsg='No job matched, please make sure the job id is valid.')

#获取job中的组件输出表元数据
def get_component_output_tables_meta(task_data):
    check_request_parameters(task_data)#检查参数
    #以job_id、component_name、role、party_id为参数，生成fate_flow.operation.job_tracker模块的Tracker类的对象tracker
    tracker = Tracker(job_id=task_data['job_id'], component_name=task_data['component_name'],
                      role=task_data['role'], party_id=task_data['party_id'])
    #以job_id为参数，调用fate_flow.utils.schedule_utils模块的get_job_dsl_parser_by_job_id方法得到job_dsl_parser对象
    job_dsl_parser = schedule_utils.get_job_dsl_parser_by_job_id(job_id=task_data['job_id'])
    if not job_dsl_parser:
        raise Exception('can not get dag parser, please check if the parameters are correct')
    component = job_dsl_parser.get_component_info(task_data['component_name'])#通过job_dsl_parser对象调用get_component_info方法得到component
    if not component:#判断component是否为空
        raise Exception('can not found component, please check if the parameters are correct')
    output_data_table_infos = tracker.get_output_data_info()#通过tracker对象调用get_output_data_info方法得到output_data_table_infos
    # 以output_data_table_infos为参数，通过tracker对象调用get_output_data_table方法得到output_tables_meta
    output_tables_meta = tracker.get_output_data_table(output_data_infos=output_data_table_infos)
    return output_tables_meta#返回结果

#获取组件输出data_line
def get_component_output_data_line(src_key, src_value):
    have_data_label = False
    have_weight = False
    data_line = [src_key]
    is_str = False
    #对参数进行判断和处理，得到结果
    if isinstance(src_value, Instance):#判断src_value是否是Instance类型
        if src_value.label is not None:#判断src_value的label是否为空
            data_line.append(src_value.label)#将src_value的label存入data_line
            have_data_label = True
        # 若src_value是Instance类型，
        # 调用fate_flow.utils.data_utils模块的dataset_to_list方法将src_value的features转为list，并存入data_line
        data_line.extend(data_utils.dataset_to_list(src_value.features))
        if src_value.weight is not None:##判断src_value的weight是否为空
            have_weight = True
            data_line.append(src_value.weight)#将src_value的weight存入data_line
    elif isinstance(src_value, str):#判断src_value是否是str类型
        data_line.extend([value for value in src_value.split(',')])#用，划分字符串并将结果存入data_line
        is_str = True
    else:
        ##src_value是其他类型则调用fate_flow.utils.data_utils模块的dataset_to_list方法将src_value转为list，并存入data_line
        data_line.extend(data_utils.dataset_to_list(src_value))
    return data_line, have_data_label, is_str, have_weight

#获取组件输出data_schema
def get_component_output_data_schema(output_table_meta, have_data_label, is_str=False, have_weight=False):
    # get schema
    #通过output_table_meta对象调用get_schema方法得到schema
    schema = output_table_meta.get_schema()
    # 对参数和schema进行判断和处理，得到结果
    if not schema:
         return ['sid']
    header = [schema.get('sid_name', 'sid')]#将schema中的sid_name加入header
    if have_data_label:#若hava_data_label为true
        header.append(schema.get('label_name'))#将schema中的label_name加入header
    if is_str:#若is_str为true
        if not schema.get('header'):#若schema的header为空
            if schema.get('sid'):#若schema的sid不为空
                return [schema.get('sid')]#返回schema的sid
            else:
                return None
        header.extend([feature for feature in schema.get('header').split(',')])#用，划分schema的header并将结果存入header
    else:##若is_str为false
        header.extend(schema.get('header', []))#将schema的header存入header
    if have_weight:#若have_weight为true
        header.append('weight')
    return header#返回header


@DB.connection_context()
#检查参数
def check_request_parameters(request_data):
    if 'role' not in request_data and 'party_id' not in request_data:
        #调用fate_flow.db.db_models模块从数据库中获取job
        jobs = Job.select(Job.f_runtime_conf_on_party).where(Job.f_job_id == request_data.get('job_id', ''),
                                                             Job.f_is_initiator == True)
        #检查参数是否相等
        if jobs:
            job = jobs[0]
            job_runtime_conf = job.f_runtime_conf_on_party
            job_initiator = job_runtime_conf.get('initiator', {})
            role = job_initiator.get('role', '')
            party_id = job_initiator.get('party_id', 0)
            request_data['role'] = role
            request_data['party_id'] = party_id
