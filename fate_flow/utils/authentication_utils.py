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
import functools
import json
import os

from flask import request

from fate_arch.common import file_utils
from fate_flow.settings import USE_AUTHENTICATION, PRIVILEGE_COMMAND_WHITELIST, stat_logger


# 定义一个权限认证类
class PrivilegeAuth(object):
    privilege_cache = {}
    local_storage_file = ''
    USE_LOCAL_STORAGE = True
    # 所有的许可
    ALL_PERMISSION = {'privilege_role': ['guest', 'host', 'arbiter'],
                      'privilege_command': ['create', 'stop', 'run'],
                      'privilege_component': []}
    # 白名单
    command_whitelist = None

    # 认证权限
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的request_authority_certification函数所调用
    @classmethod
    def authentication_privilege(cls, src_party_id, src_role, request_path, party_id_index, role_index, command, component_index=None):
        if not src_party_id:
            src_party_id = 0
        src_party_id = str(src_party_id)
        # 获取目的party_id,如果src和dest的party_id相同则return
        if src_party_id == PrivilegeAuth.get_dest_party_id(request_path, party_id_index):
            return
        stat_logger.info("party {} role {} start authentication".format(src_party_id, src_role))
        # 获取认证项目，返回privilege字典
        privilege_dic = PrivilegeAuth.get_authentication_items(request_path, role_index, command)
        for privilege_type, value in privilege_dic.items():
            # 跳过组件认证
            if value and privilege_type == 'privilege_component':
                continue
            # 跳过白名单
            if value in PrivilegeAuth.command_whitelist:
                continue
            if value:
                # 进行权限认证
                PrivilegeAuth.authentication_privilege_do(value, src_party_id, src_role, privilege_type)
        stat_logger.info('party {} role {} authenticated success'.format(src_party_id, src_role))
        return True


    # 认证组件
    # 被调：
    # 被fate_flow.controller.task_controller.py里面的start_task函数所调用
    @classmethod
    def authentication_component(cls, job_dsl, src_party_id, src_role, party_id, component_name):
        # 是否认证，默认为false
        if USE_AUTHENTICATION:
            if not src_party_id:
                src_party_id = 0
            src_party_id = str(src_party_id)
            if src_party_id == party_id:
                return
            stat_logger.info('party {} role {} start authenticated component'.format(src_party_id, src_role))
            # 获取组件模块
            component_module = get_component_module(component_name, job_dsl)
            # 进行权限认证
            PrivilegeAuth.authentication_privilege_do(component_module, src_party_id, src_role, privilege_type="privilege_component")
            stat_logger.info('party {} role {} authenticated component success'.format(src_party_id, src_role))
            return True


    # 进行权限认证
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的authentication_privilege、authentication_component函数所调用
    @classmethod
    def authentication_privilege_do(cls, value, src_party_id, src_role, privilege_type):
        if value not in PrivilegeAuth.privilege_cache.get(src_party_id, {}).get(src_role, {}).get(privilege_type, []):
            # 获取权限config
            if value not in PrivilegeAuth.get_permission_config(src_party_id, src_role).get(privilege_type, []):
                stat_logger.info('{} {} not authorized'.format(privilege_type.split('_')[1], value))
                raise Exception('{} {} not authorized'.format(privilege_type.split('_')[1], value))


    # 获取权限config
    # 被调:
    # 被fate_flow.apps.permission_app.py里面的query_privilege函数所调用
    # 被fate_flow.utils.authentication_utils.py里面的authentication_privilege_do、authentication_check函数所调用
    @classmethod
    def get_permission_config(cls, src_party_id, src_role, use_local=True):
        # 是否使用本地存储，默认为true
        if PrivilegeAuth.USE_LOCAL_STORAGE:
            # 读取本地存储
            return PrivilegeAuth.read_local_storage(src_party_id, src_role)
        else:
            # 读取云端config
            return PrivilegeAuth.read_cloud_config_center(src_party_id, src_role)


    # 读取本地存储
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的get_permission_config函数所调用
    @classmethod
    def read_local_storage(cls, src_party_id, src_role):
        # 打开本地存储文件
        with open(PrivilegeAuth.local_storage_file) as fp:
            local_storage_conf = json.load(fp)
            PrivilegeAuth.privilege_cache = local_storage_conf
        return local_storage_conf.get(src_party_id, {}).get(src_role, {})


    # 获取新的权限config
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的modify_permission函数所调用
    @classmethod
    def get_new_permission_config(cls, src_party_id, src_role, privilege_role, privilege_command, privilege_component, delete):
        # 打开本地存储文件
        with open(PrivilegeAuth.local_storage_file) as f:
            stat_logger.info(
                "add permissions: src_party_id {} src_role {} privilege_role {} privilege_command {} privilege_component {}".format(
                    src_party_id, src_role, privilege_role, privilege_command, privilege_component))
            json_data = json.load(f)
            local_storage = json_data
            # 获取privilege字典
            privilege_dic = PrivilegeAuth.get_privilege_dic(privilege_role, privilege_command, privilege_component)
            for privilege_type, values in privilege_dic.items():
                if values:
                    for value in values.split(','):
                        # Python strip() 方法用于移除字符串头尾指定的字符（默认为空格或换行符）或字符序列。
                        value = value.strip()
                        if value == 'all':
                            # 所有的许可
                            value = PrivilegeAuth.ALL_PERMISSION[privilege_type]
                        if value not in PrivilegeAuth.ALL_PERMISSION.get(privilege_type) and \
                                value != PrivilegeAuth.ALL_PERMISSION.get(privilege_type):
                            stat_logger.info('{} does not exist in the permission {}'.format(value, privilege_type.split('_')[1]))
                            raise Exception('{} does not exist in the permission {}'.format(value, privilege_type.split('_')[1]))
                        # 是否删除
                        if not delete:
                            if local_storage.get(src_party_id, {}):
                                if local_storage.get(src_party_id).get(src_role, {}):
                                    if local_storage.get(src_party_id).get(src_role).get(privilege_type):
                                        # \(处于行尾位置)续行符
                                        local_storage[src_party_id][src_role][privilege_type].append(value) \
                                            if isinstance(value, str) \
                                            else local_storage[src_party_id][src_role][privilege_type].extend(value)
                                    else:
                                        # [value] 列表
                                        local_storage[src_party_id][src_role][privilege_type] = [value] \
                                            if isinstance(value, str) \
                                            else value
                                else:
                                    local_storage[src_party_id][src_role] = {privilege_type: [value]} \
                                        if isinstance(value, str) \
                                        else {privilege_type: value}
                            else:
                                local_storage[src_party_id] = {src_role: {privilege_type: [value]}} \
                                    if isinstance(value, str)\
                                    else {src_role: {privilege_type: value}}
                            local_storage[src_party_id][src_role][privilege_type] = \
                                list(set(local_storage[src_party_id][src_role][privilege_type]))
                        else:
                            try:
                                if isinstance(value, str):
                                    local_storage[src_party_id][src_role][privilege_type].remove(value)
                                else:
                                    local_storage[src_party_id][src_role][ privilege_type] = []
                            except:
                                stat_logger.info('{} {} is not authorized ,it cannot be deleted'.format(privilege_type.split('_')[1], value)
                                    if isinstance(value, str) else "No permission to delete")
                                raise Exception(
                                    '{} {} is not authorized ,it cannot be deleted'.format(privilege_type.split('_')[1], value)
                                    if isinstance(value, str) else "No permission to delete")
            stat_logger.info('add permission successfully')
            f.close()
            return local_storage


    # 重写本地存储
    # 被调:
    # 被fate_flow.utils.authentication_utils.py里面的modify_permission函数所调用
    @classmethod
    def rewrite_local_storage(cls, new_json):
        # 打开本地存储文件
        with open(PrivilegeAuth.local_storage_file, 'w') as fp:
            PrivilegeAuth.privilege_cache = new_json
            json.dump(new_json, fp, indent=4, separators=(',', ': '))
            fp.close()


    # 读取云端config
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的get_permission_config函数所调用
    @classmethod
    def read_cloud_config_center(cls, src_party_id, src_role):
        pass

    # 写入云端config
    @classmethod
    def write_cloud_config_center(cls, ):
        pass


    # 获取认证项目，返回privilege字典
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的authentication_privilege函数所调用
    @classmethod
    def get_authentication_items(cls, request_path, role_index, command):
        dest_role = request_path.split('/')[role_index]
        component = None
        return PrivilegeAuth.get_privilege_dic(dest_role, command, component)


    # 获取目的party_id
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的authentication_privilege函数所调用
    @classmethod
    def get_dest_party_id(cls, request_path, party_id_index):
        # Python split() 通过指定分隔符对字符串进行切片，如果参数 num 有指定值，则分隔 num+1 个子字符串
        # str.split(str="", num=string.count(str)).
        return request_path.split('/')[party_id_index]


    # 获取privilege字典
    # 被调：
    # 被fate_flow.utils.authentication_utils.py里面的get_authentication_items、get_new_permission_config函数所调用
    @classmethod
    def get_privilege_dic(cls, privilege_role, privilege_command, privilege_component):
        return {'privilege_role': privilege_role,
                'privilege_command': privilege_command,
                'privilege_component': privilege_component}


    # 初始化
    # 被调：
    # 被fate_flow.fate_flow_server.py所调用
    @classmethod
    def init(cls):
        # 是否认证，默认为false
        if USE_AUTHENTICATION:
            # init local storage
            stat_logger.info('init local authorization library')
            # 获取python基础路径
            file_dir = os.path.join(file_utils.get_python_base_directory(), 'fate_flow')
            os.makedirs(file_dir, exist_ok=True)
            # 初始化本地存储文件
            PrivilegeAuth.local_storage_file = os.path.join(file_dir, 'authorization_config.json')
            if not os.path.exists(PrivilegeAuth.local_storage_file):
                with open(PrivilegeAuth.local_storage_file, 'w') as fp:
                    fp.write(json.dumps({}))

            # init whitelist，默认为空
            PrivilegeAuth.command_whitelist = PRIVILEGE_COMMAND_WHITELIST

            # init ALL_PERMISSION
            component_path = os.path.join(file_utils.get_python_base_directory(),
                                          'federatedml', 'conf', 'setting_conf')
            # 搜索命令
            search_command()
            stat_logger.info('search components from {}'.format(component_path))
            # 搜搜组件
            search_component(component_path)


# 修改权限
# 被调：
# 被fate_flow.apps.permission_app.py里面的grant_permission、delete_permission函数调用
def modify_permission(permission_info, delete=False):
    # 是否使用本地存储，默认为true
    if PrivilegeAuth.USE_LOCAL_STORAGE:
        # 获取新的权限config
        new_json = PrivilegeAuth.get_new_permission_config(src_party_id=permission_info.get('src_party_id'),
                                                           src_role=permission_info.get('src_role'),
                                                           privilege_role=permission_info.get('privilege_role', None),
                                                           privilege_command=permission_info.get('privilege_command',
                                                                                                 None),
                                                           privilege_component=permission_info.get(
                                                               'privilege_component', None),
                                                           delete=delete)
        # 重写本地存储
        PrivilegeAuth.rewrite_local_storage(new_json)


# 申请授权证书
# 被调：
# 被fate_flow.scheduling_apps.party_app.py里面的create_job、start_task、stop_task函数调用
def request_authority_certification(party_id_index, role_index, command, component_index=None):
    def request_authority_certification_do(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            if USE_AUTHENTICATION:
                # 认证权限
                PrivilegeAuth.authentication_privilege(src_party_id=request.json.get('src_party_id'),
                                                       src_role=request.json.get('src_role'),
                                                       request_path=request.path,
                                                       party_id_index=party_id_index,
                                                       role_index=role_index,
                                                       command=command,
                                                       component_index=component_index
                                                       )
            return func(*args, **kwargs)
        return _wrapper
    return request_authority_certification_do


# 获取组件模块
# 被调：
# 被fate_flow.utils.authentication_utils.py里面的authentication_component函数所调用
def get_component_module(component_name, job_dsl):
    return job_dsl["components"][component_name]["module"].lower()


# 搜索命令
# 被调：
# 被fate_flow.utils.authentication_utils.py里面的init函数所调用
def search_command():
    command_list = []
    # extend() 函数用于在列表末尾一次性追加另一个序列中的多个值（用新列表扩展原来的列表）。
    PrivilegeAuth.ALL_PERMISSION['privilege_command'].extend(command_list)


# 搜索组件
# 被调：
# 被fate_flow.utils.authentication_utils.py里面的init函数所调用
def search_component(path):
    component_list = [file_name.split('.')[0].lower() for file_name in os.listdir(path) if 'json' in file_name]
    component_list = list(set(component_list) - {'upload', 'download'})
    PrivilegeAuth.ALL_PERMISSION['privilege_component'].extend(component_list)


# 认证检查
# 被调：
# 被fate_flow.controller.job_controller.py里面的create_job函数所调用
def authentication_check(src_role, src_party_id, dsl, runtime_conf, role, party_id):
    initiator = runtime_conf['initiator']
    roles = runtime_conf['role']
    if initiator['role'] != src_role or initiator['party_id'] != int(src_party_id) or int(party_id) not in roles[role]:
        if not int(party_id):
            return
        else:
            stat_logger.info('src_role {} src_party_id {} authentication check failed'.format(src_role, src_party_id))
            raise Exception('src_role {} src_party_id {} authentication check failed'.format(src_role, src_party_id))
    # 获取所有的组件
    components = get_all_components(dsl)
    if str(party_id) == str(src_party_id):
        return
    need_run_commond = list(set(PrivilegeAuth.ALL_PERMISSION['privilege_command'])-set(PrivilegeAuth.command_whitelist))
    if set(need_run_commond) != set(PrivilegeAuth.privilege_cache.get(src_party_id, {}).get(src_role, {}).get('privilege_command', [])):
        if set(need_run_commond) != set(PrivilegeAuth.get_permission_config(src_party_id, src_role).get('privilege_command', [])):
            stat_logger.info('src_role {} src_party_id {} commond authentication that needs to be run failed:{}'.format(
                    src_role, src_party_id, set(need_run_commond) - set(PrivilegeAuth.privilege_cache.get(src_party_id,
                        {}).get(src_role, {}).get('privilege_command', []))))
            raise Exception('src_role {} src_party_id {} commond authentication that needs to be run failed:{}'.format(
                    src_role, src_party_id, set(need_run_commond) - set(PrivilegeAuth.privilege_cache.get(src_party_id,
                        {}).get(src_role, {}).get('privilege_command', []))))
    if not set(components).issubset(PrivilegeAuth.privilege_cache.get(src_party_id, {}).get(src_role, {}).get(
            'privilege_component', [])):
        if not set(components).issubset(PrivilegeAuth.get_permission_config(src_party_id, src_role).get(
                'privilege_component', [])):
            stat_logger.info('src_role {} src_party_id {} component authentication that needs to be run failed:{}'.format(
                src_role, src_party_id, components))
            raise Exception('src_role {} src_party_id {} component authentication that needs to be run failed:{}'.format(
                src_role, src_party_id, set(components)-set(PrivilegeAuth.privilege_cache.get(src_party_id, {}).get(
                    src_role, {}).get('privilege_component', []))))
    stat_logger.info('src_role {} src_party_id {} authentication check success'.format(src_role, src_party_id))


# 检查约束
# 被调：
# 被fate_flow.scheduler.dag_scheduler.py里面的submit函数所调用
def check_constraint(job_runtime_conf, job_dsl):
    # Component constraint
    # 检查组件的约束
    check_component_constraint(job_runtime_conf, job_dsl)


# 检查组件的约束
# 被调：
# 被fate_flow.utils.authentication_utils.py里面的check_constraint函数所调用
def check_component_constraint(job_runtime_conf, job_dsl):
    if job_dsl:
        # 获取所有的组件
        all_components = get_all_components(job_dsl)
        glm = ['heterolr', 'heterolinr', 'heteropoisson']
        for cpn in glm:
            if cpn in all_components:
                roles = job_runtime_conf.get('role')
                if 'guest' in roles.keys() and 'arbiter' in roles.keys() and 'host' in roles.keys():
                    for party_id in set(roles['guest']) & set(roles['arbiter']):
                        if party_id not in roles['host'] or len(set(roles['guest']) & set(roles['arbiter'])) != len(roles['host']):
                            raise Exception("{} component constraint party id, please check role config:{}".format(cpn, job_runtime_conf.get('role')))


# 获取所有的组件
# 被调：
# 被fate_flow.utils.authentication_utils.py里面的authentication_check、check_component_constraint函数所调用
def get_all_components(dsl):
    return [dsl['components'][component_name]['module'].lower() for component_name in dsl['components'].keys()]