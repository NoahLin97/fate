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
import datetime
import errno
import os
import subprocess
import sys
import threading
import typing

import psutil

from fate_arch.common import file_utils
from fate_arch.common.base_utils import json_dumps, fate_uuid, current_timestamp
from fate_arch.common.log import schedule_logger
from fate_flow.db.db_models import DB, Job, Task
from fate_flow.entity.types import JobStatus
from fate_flow.entity.types import TaskStatus, RunParameters, KillProcessStatusCode
from fate_flow.settings import stat_logger, JOB_DEFAULT_TIMEOUT, WORK_MODE, FATE_BOARD_DASHBOARD_ENDPOINT
from fate_flow.utils import detect_utils, model_utils
from fate_flow.utils import session_utils
from fate_flow.utils.service_utils import ServiceUtils


# 定义一个id计数器
class IdCounter(object):
    _lock = threading.RLock()

    def __init__(self, initial_value=0):
        self._value = initial_value

    def incr(self, delta=1):
        '''
        Increment the counter with locking
        '''
        with IdCounter._lock:
            self._value += delta
            return self._value


id_counter = IdCounter()


# 生成jobid
def generate_job_id():
    return '{}{}'.format(datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"), str(id_counter.incr()))


# 生成taskid
def generate_task_id(job_id, component_name):
    return '{}_{}'.format(job_id, component_name)


# 生成task版本id
def generate_task_version_id(task_id, task_version):
    return "{}_{}".format(task_id, task_version)


# 生成session的id
def generate_session_id(task_id, task_version, role, party_id, suffix=None, random_end=False):
    items = [task_id, str(task_version), role, str(party_id)]
    # suffix：后缀
    if suffix:
        items.append(suffix)
    # 是否添加随机id作为结尾
    if random_end:
        items.append(fate_uuid())
    return "_".join(items)


# 生成task输入数据的namespace
def generate_task_input_data_namespace(task_id, task_version, role, party_id):
    return "input_data_{}".format(generate_session_id(task_id=task_id,
                                                      task_version=task_version,
                                                      role=role,
                                                      party_id=party_id))


# 获取对应job_id所在的os路径
def get_job_directory(job_id):
    return os.path.join(file_utils.get_project_base_directory(), 'jobs', job_id)


# 获取对应job_id的日志所在的os路径
def get_job_log_directory(job_id):
    return os.path.join(file_utils.get_project_base_directory(), 'logs', job_id)


# 检查config的正确性，是否都有required_parameters
def check_config(config: typing.Dict, required_parameters: typing.List):
    for parameter in required_parameters:
        if parameter not in config:
            return False, 'configuration no {} parameter'.format(parameter)
    else:
        return True, 'ok'


# 检查job的runtime config是否有必要的参数
def check_job_runtime_conf(runtime_conf: typing.Dict):
    '''
    "job_runtime_conf":{
        "initiator": {
            "role": "guest",
            "party_id": 9999
        },
        "job_parameters": {
            "work_mode": 1
        },
        "role": {
            "guest": [9999],
            "host": [10000]
        },
    '''

    # 先检查是否有必须的initiator、job_parameters、role三个部分
    detect_utils.check_config(runtime_conf, ['initiator', 'job_parameters', 'role'])
    # 再检查initiator中是否有对应的role、party_id
    detect_utils.check_config(runtime_conf['initiator'], ['role', 'party_id'])

    # deal party id
    # 把partyid和role里面的partyid转化为int
    runtime_conf['initiator']['party_id'] = int(runtime_conf['initiator']['party_id'])
    for r in runtime_conf['role'].keys():
        # 可能一个role有多个partyid
        for i in range(len(runtime_conf['role'][r])):
            runtime_conf['role'][r][i] = int(runtime_conf['role'][r][i])


# 定义基础的runtime config的参数
def runtime_conf_basic(if_local=False):
    # 默认非本地执行
    job_runtime_conf = {
        "initiator": {},
        "job_parameters": {"work_mode": WORK_MODE},
        "role": {},
        "role_parameters": {}
    }
    # 若为本地执行
    if if_local:
        job_runtime_conf["initiator"]["role"] = "local"
        job_runtime_conf["initiator"]["party_id"] = 0
        job_runtime_conf["role"]["local"] = [0]
    return job_runtime_conf


# 创建新的runtime config
def new_runtime_conf(job_dir, method, module, role, party_id):
    # 是否本地
    if role:
        conf_path_dir = os.path.join(job_dir, method, module, role, str(party_id))
    else:
        conf_path_dir = os.path.join(job_dir, method, module, str(party_id))
    os.makedirs(conf_path_dir, exist_ok=True)
    return os.path.join(conf_path_dir, 'runtime_conf.json')


# 保存job的config
def save_job_conf(job_id, role, job_dsl, job_runtime_conf, job_runtime_conf_on_party, train_runtime_conf, pipeline_dsl=None):
    # 根据对应的jobid和role来获取job的config路径
    path_dict = get_job_conf_path(job_id=job_id, role=role)

    os.makedirs(os.path.dirname(path_dict.get('job_dsl_path')), exist_ok=True)
    os.makedirs(os.path.dirname(path_dict.get('job_runtime_conf_on_party_path')), exist_ok=True)

    for data, conf_path in [(job_dsl, path_dict['job_dsl_path']),
                            (job_runtime_conf, path_dict['job_runtime_conf_path']),
                            (job_runtime_conf_on_party, path_dict['job_runtime_conf_on_party_path']),
                            (train_runtime_conf, path_dict['train_runtime_conf_path']),
                            (pipeline_dsl, path_dict['pipeline_dsl_path'])]:
        with open(conf_path, 'w+') as f:
            # truncate() 方法用于截断文件，如果指定了可选参数 size，则表示截断文件为 size 个字符。
            # 如果没有指定 size，则从当前位置起截断；截断之后 size 后面的所有字符被删除。
            f.truncate()
            if not data:
                data = {}
            f.write(json_dumps(data, indent=4))
            # flush() 方法是用来刷新缓冲区的，即将缓冲区中的数据立刻写入文件，同时清空缓冲区，不需要是被动的等待输出缓冲区写入。
            # 一般情况下，文件关闭后会自动刷新缓冲区，但有时你需要在关闭前刷新它，这时就可以使用 flush() 方法。
            f.flush()
    return path_dict


# 根据对应的jobid和role来获取job的config路径
def get_job_conf_path(job_id, role):
    # 获取对应job_id所在的os路径
    job_dir = get_job_directory(job_id)
    # 获取dsl和config
    job_dsl_path = os.path.join(job_dir, 'job_dsl.json')
    job_runtime_conf_path = os.path.join(job_dir, 'job_runtime_conf.json')
    # 获取对应role的config
    job_runtime_conf_on_party_path = os.path.join(job_dir, role, 'job_runtime_on_party_conf.json')
    # 获取train_runtime_conf
    train_runtime_conf_path = os.path.join(job_dir, 'train_runtime_conf.json')
    # 获取pipeline_dsl
    pipeline_dsl_path = os.path.join(job_dir, 'pipeline_dsl.json')
    return {'job_dsl_path': job_dsl_path,
            'job_runtime_conf_path': job_runtime_conf_path,
            'job_runtime_conf_on_party_path': job_runtime_conf_on_party_path,
            'train_runtime_conf_path': train_runtime_conf_path,
            'pipeline_dsl_path': pipeline_dsl_path}


# 根据对应的jobid和role来获取job的config内容
def get_job_conf(job_id, role):
    conf_dict = {}
    for key, path in get_job_conf_path(job_id, role).items():
        # 载入json格式的config内容
        config = file_utils.load_json_conf(path)
        conf_dict[key] = config
    return conf_dict


# 从数据库中读取对应jobid，role，partyid的config
@DB.connection_context()
def get_job_configuration(job_id, role, party_id, tasks=None):
    # 根据是否还在执行对task进行判断
    if tasks:
        jobs_run_conf = {}
        for task in tasks:
            # 从数据库中搜索f_job_id == task.f_job_id 对应的f_job_id、f_runtime_conf_on_party、f_description
            jobs = Job.select(Job.f_job_id, Job.f_runtime_conf_on_party, Job.f_description).where(Job.f_job_id == task.f_job_id)
            job = jobs[0]
            jobs_run_conf[job.f_job_id] = job.f_runtime_conf_on_party["component_parameters"]["role"]["local"]["0"]["upload_0"]
            jobs_run_conf[job.f_job_id]["notes"] = job.f_description
        return jobs_run_conf
    else:
        # 从数据库中搜索Job.f_job_id == job_id && Job.f_role == role && Job.f_party_id == party_id 对应的f_dsl、f_runtime_conf、f_train_runtime_conf、f_runtime_conf_on_party
        jobs = Job.select(Job.f_dsl, Job.f_runtime_conf, Job.f_train_runtime_conf, Job.f_runtime_conf_on_party).where(Job.f_job_id == job_id,
                                                                                                                      Job.f_role == role,
                                                                                                                      Job.f_party_id == party_id)
    if jobs:
        job = jobs[0]
        return job.f_dsl, job.f_runtime_conf, job.f_runtime_conf_on_party, job.f_train_runtime_conf
    else:
        return {}, {}, {}, {}


# 从数据库中读取对应jobid、role和partyid的模型的config
@DB.connection_context()
def get_model_configuration(job_id, role, party_id):
    # 查询对应model的相关信息，返回retcode, retmsg, data
    res = model_utils.query_model_info(model_version=job_id, role=role, party_id=party_id,
                                       query_filters=['train_dsl', 'dsl', 'train_runtime_conf', 'runtime_conf'])
    if res:
        dsl = res[0].get('train_dsl') if res[0].get('train_dsl') else res[0].get('dsl')
        runtime_conf = res[0].get('runtime_conf')
        train_runtime_conf = res[0].get('train_runtime_conf')
        return dsl, runtime_conf, train_runtime_conf
    return {}, {}, {}


    # models = MLModel.select(MLModel.f_dsl, MLModel.f_runtime_conf,
    #                         MLModel.f_train_runtime_conf).where(MLModel.f_job_id == job_id,
    #                                                             MLModel.f_role == role,
    #                                                             MLModel.f_party_id == party_id)
    # if models:
    #     model = models[0]
    #     return model.f_dsl, model.f_runtime_conf, model.f_train_runtime_conf
    # else:
    #     return {}, {}, {}


# 根据job_id、role、party_id从数据库中获取对应的job参数
@DB.connection_context()
def get_job_parameters(job_id, role, party_id):
    # 从数据库中搜索f_job_id == job_id && f_role == role && f_party_id == party_id 对应的f_runtime_conf_on_party
    jobs = Job.select(Job.f_runtime_conf_on_party).where(Job.f_job_id == job_id,
                                                         Job.f_role == role,
                                                         Job.f_party_id == party_id)
    if jobs:
        job = jobs[0]
        return job.f_runtime_conf_on_party.get("job_parameters")
    else:
        return {}


# 根据job_id、role、party_id从数据库中获取对应job的dsl参数
@DB.connection_context()
def get_job_dsl(job_id, role, party_id):
    # 从数据库中搜索f_job_id == job_id && f_role == role && f_party_id == party_id 对应的f_dsl
    jobs = Job.select(Job.f_dsl).where(Job.f_job_id == job_id,
                                       Job.f_role == role,
                                       Job.f_party_id == party_id)
    if jobs:
        job = jobs[0]
        return job.f_dsl
    else:
        return {}


# 获取job的虚拟组件名
def job_virtual_component_name():
    return "pipeline"


# 获取job的虚拟组件模块名
def job_virtual_component_module_name():
    return "Pipeline"


# 从数据库中读取前limit条job信息
@DB.connection_context()
def list_job(limit):
    if limit > 0:
        # 按照job创建时间降序排序取前limit个
        jobs = Job.select().order_by(Job.f_create_time.desc()).limit(limit)
    else:
        # 按照job创建时间降序排序，返回全部
        jobs = Job.select().order_by(Job.f_create_time.desc())
    return [job for job in jobs]


# 从数据库中读取前limit条task信息
@DB.connection_context()
def list_task(limit):
    if limit > 0:
        # 按照task创建时间降序排序取前limit个
        tasks = Task.select().order_by(Task.f_create_time.desc()).limit(limit)
    else:
        # 按照task创建时间降序排序，返回全部
        tasks = Task.select().order_by(Task.f_create_time.desc())
    return [task for task in tasks]


# 根据pid检查job进程，并关闭
def check_job_process(pid):
    if pid < 0:
        return False
    if pid == 0:
        raise ValueError('invalid PID 0')
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to "man 2 kill" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise
    else:
        return True


# 检查job任务执行是否超时
def check_job_is_timeout(job: Job):
    job_parameters = job.f_runtime_conf_on_party["job_parameters"]
    # JOB_DEFAULT_TIMEOUT = 3 * 24 * 60 * 60
    timeout = job_parameters.get("timeout", JOB_DEFAULT_TIMEOUT)
    now_time = current_timestamp()
    running_time = (now_time - job.f_create_time)/1000
    if running_time > timeout:
        schedule_logger(job_id=job.f_job_id).info('job {}  run time {}s timeout'.format(job.f_job_id, running_time))
        return True
    else:
        return False


# 通过关键词检查对应的进程是否存在
def check_process_by_keyword(keywords):
    if not keywords:
        return True
    # Linux grep 命令用于查找文件里符合条件的字符串。
    keyword_filter_cmd = ' |'.join(['grep %s' % keyword for keyword in keywords])
    # os.system()方法在子shell中执行命令(字符串)。
    ret = os.system('ps aux | {} | grep -v grep | grep -v "ps aux "'.format(keyword_filter_cmd))
    return ret == 0


# 运行子进程
def run_subprocess(job_id, config_dir, process_cmd, log_dir=None, job_dir=None):
    schedule_logger(job_id=job_id).info('start process command: {}'.format(' '.join(process_cmd)))

    os.makedirs(config_dir, exist_ok=True)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    std_log = open(os.path.join(log_dir if log_dir else config_dir, 'std.log'), 'w')
    pid_path = os.path.join(config_dir, 'pid')

    # os.name
    # 该变量返回当前操作系统的类型，当前只注册了3个值：分别是posix , nt , java， 对应linux/windows/java虚拟机
    if os.name == 'nt':
        # subprocess 模块允许我们启动一个新进程，并连接到它们的输入/输出/错误管道，从而获取返回值。

        # 子进程的启动信息
        startupinfo = subprocess.STARTUPINFO()
        # 参数未知，还没找到相关资料
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = subprocess.SW_HIDE
    else:
        startupinfo = None
    '''
    subprocess模块定义了一个类： Popen
    资料：[python中的subprocess.Popen()使用详解---以及注意的问题（死锁）](https://www.cnblogs.com/lgj8/p/12132829.html)
   
    class subprocess.Popen( args, 
      bufsize=0, 
      executable=None,
      stdin=None,
      stdout=None, 
      stderr=None, 
      preexec_fn=None, 
      close_fds=False, 
      shell=False, 
      cwd=None, 
      env=None, 
      universal_newlines=False, 
      startupinfo=None, 
      creationflags=0)
    '''

    p = subprocess.Popen(process_cmd,
                         stdout=std_log,
                         stderr=std_log,
                         startupinfo=startupinfo,
                         cwd=job_dir,
                         )
    with open(pid_path, 'w') as f:
        # truncate() 方法用于截断文件，如果指定了可选参数 size，则表示截断文件为 size 个字符。 如果没有指定 size，则从当前位置起截断；截断之后 size 后面的所有字符被删除。
        f.truncate()
        f.write(str(p.pid) + "\n")
        # flush() 方法是用来刷新缓冲区的，即将缓冲区中的数据立刻写入文件，同时清空缓冲区，不需要是被动的等待输出缓冲区写入。
        # 一般情况下，文件关闭后会自动刷新缓冲区，但有时你需要在关闭前刷新它，这时就可以使用 flush() 方法。
        f.flush()
    schedule_logger(job_id=job_id).info('start process command: {} successfully, pid is {}'.format(' '.join(process_cmd), p.pid))
    return p


# 等待子进程结束
def wait_child_process(signum, frame):
    try:
        while True:
            # 资料：[Python::OS 模块 -- 进程管理](https://www.cnblogs.com/now-fighting/p/3534185.html)
            # os.waitpid(pid, options)
            # 等待进程id为pid的进程结束，返回一个tuple，包括进程的进程ID和退出信息(和os.wait()一样)，参数options会影响该函数的行为。
            # 在默认情况下，options的值为0。
            # 如果pid是一个正数，waitpid()请求获取一个pid指定的进程的退出信息，如果pid为0，则等待并获取当前进程组中的任何子进程的值。
            # 如果pid为-1，则等待当前进程的任何子进程，如果pid小于-1，则获取进程组id为pid的绝对值的任何一个进程。当系统调用返回-1时，抛出一个OSError异常。
            # os.WNOHANG - 如果没有子进程退出，则不阻塞waitpid()调用
            child_pid, status = os.waitpid(-1, os.WNOHANG)
            if child_pid == 0:
                stat_logger.info('no child process was immediately available')
                break
            exitcode = status >> 8
            stat_logger.info('child process %s exit with exitcode %s', child_pid, exitcode)
    except OSError as e:
        if e.errno == errno.ECHILD:
            stat_logger.warning('current process has no existing unwaited-for child processes.')
        else:
            raise


# 通过命令检查进程是否为任务执行者
# psutil.Process（获得当前的进程对象）
def is_task_executor_process(task: Task, process: psutil.Process):
    """
    check the process if task executor or not by command
    :param task:
    :param process:
    :return:
    """
    # Todo: The same map should be used for run task command
    run_cmd_map = {
        3: "f_job_id",
        5: "f_component_name",
        7: "f_task_id",
        9: "f_task_version",
        11: "f_role",
        13: "f_party_id"
    }

    try:
        # 调用进程的命令：cmdline()
        cmdline = process.cmdline()
        schedule_logger(task.f_job_id).info(cmdline)
    except Exception as e:
        # Not sure whether the process is a task executor process, operations processing is required
        schedule_logger(task.f_job_id).warning(e)
        return False

    for i, k in run_cmd_map.items():
        if len(cmdline) > i and cmdline[i] == str(getattr(task, k)):
            continue
        else:
            # todo: The logging level should be obtained first
            if len(cmdline) > i:
                schedule_logger(task.f_job_id).debug(f"cmd map {i} {k}, cmd value {cmdline[i]} task value {getattr(task, k)}")
            return False
    else:
        return True


# 关闭task的执行进程
def kill_task_executor_process(task: Task, only_child=False):
    try:
        # 没有找到正在运行的pid
        if not task.f_run_pid:
            schedule_logger(task.f_job_id).info("job {} task {} {} {} with {} party status no process pid".format(
                task.f_job_id, task.f_task_id, task.f_role, task.f_party_id, task.f_party_status))
            return KillProcessStatusCode.NOT_FOUND

        pid = int(task.f_run_pid)
        schedule_logger(task.f_job_id).info("try to stop job {} task {} {} {} with {} party status process pid:{}".format(
            task.f_job_id, task.f_task_id, task.f_role, task.f_party_id, task.f_party_status, pid))

        # check_job_process根据pid检查job进程，检查pid是否合法，并关闭
        if not check_job_process(pid):
            schedule_logger(task.f_job_id).info("can not found job {} task {} {} {} with {} party status process pid:{}".format(
                task.f_job_id, task.f_task_id, task.f_role, task.f_party_id, task.f_party_status, pid))
            return KillProcessStatusCode.NOT_FOUND

        p = psutil.Process(int(pid))
        # is_task_executor_process通过命令检查进程是否为任务执行者
        if not is_task_executor_process(task=task, process=p):
            schedule_logger(task.f_job_id).warning("this pid {} is not job {} task {} {} {} executor".format(
                pid, task.f_job_id, task.f_task_id, task.f_role, task.f_party_id))
            return KillProcessStatusCode.ERROR_PID

        # 关闭所有子进程
        for child in p.children(recursive=True):
            # 检查子进程
            if check_job_process(child.pid) and is_task_executor_process(task=task, process=child):
                child.kill()
        if not only_child:
            if check_job_process(p.pid) and is_task_executor_process(task=task, process=p):
                p.kill()
        schedule_logger(task.f_job_id).info("successfully stop job {} task {} {} {} process pid:{}".format(
            task.f_job_id, task.f_task_id, task.f_role, task.f_party_id, pid))
        return KillProcessStatusCode.KILLED

    except Exception as e:
        raise e


# session的开始和停止
def start_session_stop(task):
    # 初始化RunParameters类
    # get_job_parameters根据job_id、role、party_id从数据库中获取对应的job参数
    job_parameters = RunParameters(**get_job_parameters(job_id=task.f_job_id, role=task.f_role, party_id=task.f_party_id))
    # 生成session的id
    computing_session_id = generate_session_id(task.f_task_id, task.f_task_version, task.f_role, task.f_party_id)
    # 判断task的状态是否是waiting状态
    if task.f_status != TaskStatus.WAITING:
        schedule_logger(task.f_job_id).info(f'start run subprocess to stop task session {computing_session_id}')
    else:
        # task是waiting状态 就不运行子进程
        schedule_logger(task.f_job_id).info(f'task is waiting, pass stop session {computing_session_id}')
        return
    # get_job_directory获取对应job_id所在的os路径
    task_dir = os.path.join(get_job_directory(job_id=task.f_job_id), task.f_role,
                            task.f_party_id, task.f_component_name, 'session_stop')
    os.makedirs(task_dir, exist_ok=True)
    # 执行cmd，stop task任务 并开始运行子进程
    process_cmd = [
        'python3', sys.modules[session_utils.SessionStop.__module__].__file__,
        '-j', computing_session_id,
        '--computing', job_parameters.computing_engine,
        '--federation', job_parameters.federation_engine,
        '--storage', job_parameters.storage_engine,
        '-c', 'stop' if task.f_status == JobStatus.SUCCESS else 'kill'
    ]
    # 运行子进程
    p = run_subprocess(job_id=task.f_job_id, config_dir=task_dir, process_cmd=process_cmd, log_dir=None)


# 设置超时时间
def get_timeout(job_id, timeout, runtime_conf, dsl):
    try:
        if timeout > 0:
            schedule_logger(job_id).info('setting job {} timeout {}'.format(job_id, timeout))
            return timeout
        else:
            default_timeout = job_default_timeout(runtime_conf, dsl)
            schedule_logger(job_id).info('setting job {} timeout {} not a positive number, using the default timeout {}'.format(
                job_id, timeout, default_timeout))
            return default_timeout
    except:
        default_timeout = job_default_timeout(runtime_conf, dsl)
        schedule_logger(job_id).info('setting job {} timeout {} is incorrect, using the default timeout {}'.format(
            job_id, timeout, default_timeout))
        return default_timeout


# 获取job的默认超时时间
def job_default_timeout(runtime_conf, dsl):
    # future versions will improve
    # JOB_DEFAULT_TIMEOUT = 3 * 24 * 60 * 60
    timeout = JOB_DEFAULT_TIMEOUT
    return timeout


# 获取fateboard的url地址
def get_board_url(job_id, role, party_id):
    board_url = "http://{}:{}{}".format(
        ServiceUtils.get_item("fateboard", "host"),
        ServiceUtils.get_item("fateboard", "port"),
        FATE_BOARD_DASHBOARD_ENDPOINT).format(job_id, role, party_id)
    # FATE_BOARD_DASHBOARD_ENDPOINT = "/index.html#/dashboard?job_id={}&role={}&party_id={}"
    return board_url
