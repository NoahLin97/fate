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
import os
import json
import tarfile

from flask import Flask, request, send_file

from fate_arch.common import WorkMode
from fate_arch.common.base_utils import json_loads, json_dumps
from fate_flow.scheduler.dag_scheduler import DAGScheduler
from fate_flow.scheduler.federated_scheduler import FederatedScheduler
from fate_flow.settings import stat_logger, TEMP_DIRECTORY
from fate_flow.utils import job_utils, detect_utils, schedule_utils
from fate_flow.utils.api_utils import get_json_result, error_response, server_error_response
from fate_flow.entity.types import FederatedSchedulingStatusCode, RetCode, JobStatus
from fate_flow.operation.job_tracker import Tracker
from fate_flow.operation.job_saver import JobSaver
from fate_flow.operation.job_clean import JobClean
from fate_flow.utils.config_adapter import JobRuntimeConfigAdapter
from fate_arch.common.log import schedule_logger
from fate_flow.controller.job_controller import JobController

manager = Flask(__name__)

# 服务端内部错误异常
@manager.errorhandler(500)
def internal_server_error(e):
    return server_error_response(e)


@manager.route('/submit', methods=['POST'])
def submit_job():
    # 将job_runtime_conf作为参数，调用fate_flow/utils/config_adapter模块的JobRuntimeConfigAdapter方法获取一个Adapter对象，
    # 从Adapter对象中调用get_job_work_mode获取work_mode
    work_mode = JobRuntimeConfigAdapter(request.json.get('job_runtime_conf', {})).get_job_work_mode()
    # 调用fate_flow/utils/detect_utils的check_config方法检查work_mode是否合法
    # 调用fate_arch.common._types.py中的 WorkMode.CLUSTER = 1, WorkMode.STANDALONE = 0
    detect_utils.check_config({'work_mode': work_mode}, required_arguments=[('work_mode', (WorkMode.CLUSTER, WorkMode.STANDALONE))])
    # 调用fate_flow/scheduler/dat_scheduler模块的submit方法提交job
    submit_result = DAGScheduler.submit(request.json)
    return get_json_result(retcode=0, retmsg='success',
                           job_id=submit_result.get("job_id"),
                           data=submit_result)


@manager.route('/stop', methods=['POST'])
def stop_job():
    job_id = request.json.get('job_id')
    stop_status = request.json.get("stop_status", "canceled")
    jobs = JobSaver.query_job(job_id=job_id)#将job_id作为参数，调用fate_flow/operation/job_saver模块的query_job方法获取job
    if jobs:
        schedule_logger(job_id).info(f"stop job on this party")#调用fate_arch/common/log模块的schedule_logger.info方法输出日志
        kill_status, kill_details = JobController.stop_jobs(job_id=job_id, stop_status=stop_status)#将job_id作为参数，调用fate_flow/controller/job_controller模块的stop_jobs方法停止本方job
        schedule_logger(job_id).info(f"stop job on this party status {kill_status}")
        schedule_logger(job_id).info(f"request stop job {jobs[0]} to {stop_status}")
        #将job作为参数，调用fate_flow/scheduler/federated_scheduler模块的request_stop_job方法停止其他方job
        status_code, response = FederatedScheduler.request_stop_job(job=jobs[0], stop_status=stop_status, command_body=jobs[0].to_json())#fate_flow/scheduler/federated_scheduler.py停止其他方job
        if status_code == FederatedSchedulingStatusCode.SUCCESS:
            return get_json_result(retcode=RetCode.SUCCESS, retmsg=f"stop job on this party {kill_status};\n"
                                                                   f"stop job on all party success")
        else:
            return get_json_result(retcode=RetCode.OPERATING_ERROR, retmsg="stop job on this party {};\n"
                                                                           "stop job failed:\n{}".format(kill_status, json_dumps(response, indent=4)))
    else:
        schedule_logger(job_id).info(f"can not found job {job_id} to stop")
        return get_json_result(retcode=RetCode.DATA_ERROR, retmsg="can not found job")


@manager.route('/rerun', methods=['POST'])
def rerun_job():
    job_id = request.json.get("job_id")
    jobs = JobSaver.query_job(job_id=job_id)#fate_flow/operation/job_saver.py根据job_id获取job
    if jobs:
        status_code, response = FederatedScheduler.request_rerun_job(job=jobs[0], command_body=request.json)#将job作为参数，调用fate_flow/scheduler/federated_scheduler模块的request_rerun_job方法启动job
        if status_code == FederatedSchedulingStatusCode.SUCCESS:
            return get_json_result(retcode=RetCode.SUCCESS, retmsg="rerun job success")
        else:
            return get_json_result(retcode=RetCode.OPERATING_ERROR, retmsg="rerun job failed:\n{}".format(json_dumps(response)))
    else:
        return get_json_result(retcode=RetCode.DATA_ERROR, retmsg="can not found job")


@manager.route('/query', methods=['POST'])
def query_job():
    jobs = JobSaver.query_job(**request.json)#调用fate_flow/operation/job_saver模块的query_job方法获取job
    if not jobs:
        return get_json_result(retcode=0, retmsg='no job could be found', data=[])
    return get_json_result(retcode=0, retmsg='success', data=[job.to_json() for job in jobs])


@manager.route('/list/job', methods=['POST'])
def list_job():
    jobs = job_utils.list_job(request.json.get('limit'))#调用fate_flow/utils/job_utils模块的list_job方法获取job列表
    if not jobs:
        return get_json_result(retcode=101, retmsg='No job found')
    return get_json_result(retcode=0, retmsg='success', data=[job.to_json() for job in jobs])


@manager.route('/update', methods=['POST'])
def update_job():
    job_info = request.json
    #将job_id、party_id和role作为参数，调用fate_flow/operation/job_saver模块的query_job方法获取job
    jobs = JobSaver.query_job(job_id=job_info['job_id'], party_id=job_info['party_id'], role=job_info['role'])
    if not jobs:
        return get_json_result(retcode=101, retmsg='find job failed')
    else:
        #将notes、job_id、role、party_id作为参数，调用fate_flow/operation/job_saver模块的update_job方法更新job的描述信息
        JobSaver.update_job(job_info={'description': job_info.get('notes', ''), 'job_id': job_info['job_id'], 'role': job_info['role'],
                                      'party_id': job_info['party_id']})#fate_flow/operation/job_saver.py更新job
        return get_json_result(retcode=0, retmsg='success')


@manager.route('/config', methods=['POST'])
def job_config():
    jobs = JobSaver.query_job(**request.json)#调用fate_flow/operation/job_saver模块的query_job方法获取job
    if not jobs:
        return get_json_result(retcode=101, retmsg='find job failed')
    else:
        job = jobs[0]
        #获取job的dsl、job_id、runtime_conf和train_runtime_conf
        response_data = dict()
        response_data['job_id'] = job.f_job_id
        response_data['dsl'] = job.f_dsl
        response_data['runtime_conf'] = job.f_runtime_conf
        response_data['train_runtime_conf'] = job.f_train_runtime_conf

        #将runtime_conf作为参数，调用fate_flow/utils/config_adapter模块获取一个adapter对象
        adapter = JobRuntimeConfigAdapter(job.f_runtime_conf)

        #通过adapter对象调用fate_flow/utils/config_adapter模块的get_common_parameters方法获取job的参数job_parameters
        job_parameters = adapter.get_common_parameters().to_dict()

        #从job_parameters中得到model_id和model_version
        response_data['model_info'] = {'model_id': job_parameters.get('model_id'),
                                       'model_version': job_parameters.get('model_version')}
        return get_json_result(retcode=0, retmsg='success', data=response_data)


@manager.route('/log', methods=['get'])
def job_log():
    job_id = request.json.get('job_id', '')
    job_log_dir = job_utils.get_job_log_directory(job_id=job_id)#将job_id作为参数，调用fate_flow/utils/job_utils模块的get_job_log_directory方法得到job的日志目录
    if os.path.exists(job_log_dir):
        #将目录中的文件打包成一个压缩文件
        memory_file = io.BytesIO()
        tar = tarfile.open(fileobj=memory_file, mode='w:gz')
        for root, dir, files in os.walk(job_log_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, job_log_dir)
                tar.add(full_path, rel_path)
        tar.close()
        memory_file.seek(0)
        return send_file(memory_file, attachment_filename='job_{}_log.tar.gz'.format(job_id), as_attachment=True)
    else:
        return error_response(210, "Log file path: {} not found. Please check if the job id is valid.".format(job_log_dir))


@manager.route('/task/query', methods=['POST'])
def query_task():
    tasks = JobSaver.query_task(**request.json)#调用fate_flow/operation/job_saver模块的query_task方法获取task
    if not tasks:
        return get_json_result(retcode=101, retmsg='find task failed')
    return get_json_result(retcode=0, retmsg='success', data=[task.to_json() for task in tasks])


@manager.route('/list/task', methods=['POST'])
def list_task():
    tasks = job_utils.list_task(request.json.get('limit'))#调用fate_flow/utils/job_utils模块的list_task方法获取task列表
    if not tasks:
        return get_json_result(retcode=100, retmsg='No task found')
    return get_json_result(retcode=0, retmsg='success', data=[task.to_json() for task in tasks])


@manager.route('/data/view/query', methods=['POST'])
def query_component_output_data_info():
    output_data_infos = Tracker.query_output_data_infos(**request.json)#调用fate_flow/operation/job_tracker模块的query_output_data_infos方法获取组件输出数据信息
    if not output_data_infos:
        return get_json_result(retcode=101, retmsg='find data view failed')
    return get_json_result(retcode=0, retmsg='success', data=[output_data_info.to_json() for output_data_info in output_data_infos])


@manager.route('/clean', methods=['POST'])
def clean_job():
    JobClean.start_clean_job(**request.json)#调用fate_flow/operation/job_clean模块的start_clean_job方法清理job
    return get_json_result(retcode=0, retmsg='success')


@manager.route('/clean/queue', methods=['POST'])
def clean_queue():
    jobs = JobSaver.query_job(is_initiator=True, status=JobStatus.WAITING)#调用fate_flow/operation/job_saver模块的query_job方法获取本方发起的处于等待状态的job
    clean_status = {}
    for job in jobs:
        #将job作为参数，调用fate_flow/scheduler/federated_scheduler模块的request_stop_job方法取消job
        status_code, response = FederatedScheduler.request_stop_job(job=job, stop_status=JobStatus.CANCELED)
        clean_status[job.f_job_id] = status_code
    return get_json_result(retcode=0, retmsg='success', data=clean_status)


@manager.route('/dsl/generate', methods=['POST'])
def dsl_generator():
    data = request.json
    cpn_str = data.get("cpn_str", "")
    try:
        if not cpn_str:
            raise Exception("Component list should not be empty.")
        if isinstance(cpn_str, list):
            cpn_list = cpn_str
        else:
            if (cpn_str.find("/") and cpn_str.find("\\")) != -1:
                raise Exception("Component list string should not contain '/' or '\\'.")
            cpn_str = cpn_str.replace(" ", "").replace("\n", "").strip(",[]")
            cpn_list = cpn_str.split(",")
        train_dsl = json_loads(data.get("train_dsl"))

        # 调用fate_flow/utils/scheduler_utils模块的get_dsl_parser_by_version方法通过version获取dsl parser对象
        parser = schedule_utils.get_dsl_parser_by_version(data.get("version", "2"))
        #通过dsl parser对象调用fate_flow/scheduler/dsl_parser模块的deploy_component方法生成predict_dsl
        predict_dsl = parser.deploy_component(cpn_list, train_dsl)

        if data.get("filename"):
            os.makedirs(TEMP_DIRECTORY, exist_ok=True)
            temp_filepath = os.path.join(TEMP_DIRECTORY, data.get("filename"))
            with open(temp_filepath, "w") as fout:
                fout.write(json.dumps(predict_dsl, indent=4))
            return send_file(open(temp_filepath, 'rb'), as_attachment=True, attachment_filename=data.get("filename"))
        return get_json_result(data=predict_dsl)
    except Exception as e:
        stat_logger.exception(e)
        return error_response(210, "DSL generating failed. For more details, "
                                   "please check logs/fate_flow/fate_flow_stat.log.")


@manager.route('/url/get', methods=['POST'])
def get_url():
    request_data = request.json
    #调用fate_flow/utils/detect_utils模块的check_config方法检查参数
    detect_utils.check_config(config=request_data, required_arguments=['job_id', 'role', 'party_id'])
    #将job_id、role和party_id作为参数，调用fate_flow/operation/job_save模块的query_job方法获取job
    jobs = JobSaver.query_job(job_id=request_data.get('job_id'), role=request_data.get('role'),
                              party_id=request_data.get('party_id'))
    if jobs:
        board_urls = []
        for job in jobs:
            board_url = job_utils.get_board_url(job.f_job_id, job.f_role, job.f_party_id)#调用fate_flow/utils/job_utils模块的get_board_url方法得到url
            board_urls.append(board_url)
        return get_json_result(data={'board_url': board_urls})
    else:
        return get_json_result(retcode=101, retmsg='no found job')
