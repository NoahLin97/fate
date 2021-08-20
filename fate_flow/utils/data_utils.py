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
from fate_arch.common import base_utils
import numpy

from fate_arch import storage
from federatedml.feature.sparse_vector import SparseVector

# 将数据集转换为列表
# 被调：
# 被fate_flow.apps.tracking_app.py里面的get_component_output_data_line函数所调用
def dataset_to_list(src):
    if isinstance(src, numpy.ndarray):
        return src.tolist()
    elif isinstance(src, list):
        return src
    elif isinstance(src, SparseVector):
        vector = [0] * src.get_shape()
        for idx, v in src.get_all_data():
            vector[idx] = v
        return vector
    else:
        return [src]


# 获取表头
# 被调：
# 被fate_flow.components.reader.py里面的copy_table函数所调用
# 被fate_flow.components.upload.py里面的save_data_table函数所调用
def get_header_schema(header_line, id_delimiter):
    header_source_item = header_line.split(id_delimiter)
    # Python strip() 方法用于移除字符串头尾指定的字符（默认为空格）。
    return {'header': id_delimiter.join(header_source_item[1:]).strip(), 'sid': header_source_item[0].strip()}


# 将列表转换为字符串
# 被调：
# 被fate_flow.components.reader.py里面的copy_table函数所调用
# 被fate_flow.components.upload.py里面的save_data_table函数所调用
def list_to_str(input_list, id_delimiter):
    # delimiter 分隔符
    return id_delimiter.join(list(map(str, input_list)))


# 默认输出表信息
# 被调：
# 被fate_flow.components.reader.py里面的run函数所调用
# 被fate_flow.operation.job_tracker.py里面的save_output_data函数所调用
def default_output_table_info(task_id, task_version):
    # 使用uuid生成fate的id
    return f"output_data_{task_id}_{task_version}", base_utils.fate_uuid()


# 数据类型为输出时文件的默认系统路径
# 被调：
# 被fate_flow.components.reader.py里面的convert_check函数所调用
# 被fate_flow.operation.job_tracker.py里面的save_output_data函数所调用
def default_output_fs_path(name, namespace, prefix=None):
    return default_filesystem_path(data_type="output", name=name, namespace=namespace, prefix=prefix)


# 数据类型为输入时文件的默认系统路径
# 被调：
# 被fate_flow.components.upload.py里面的run函数所调用
def default_input_fs_path(name, namespace, prefix=None):
    return default_filesystem_path(data_type="input", name=name, namespace=namespace, prefix=prefix)


# 获取文件默认的系统路径
# 被fate_flow.utils.data_utils.py里面的default_input_fs_path、default_output_fs_path函数所调用
def default_filesystem_path(data_type, name, namespace, prefix=None):
    p = f"/fate/{data_type}_data/{namespace}/{name}"
    if prefix:
        p = f"{prefix}/{p}"
    return p


# 获取输入数据最小划分
# 被调：
# 被fate_flow.controller.job_controller.py里面的query_job_input_args函数所调用
def get_input_data_min_partitions(input_data, role, party_id):
    min_partition = None
    if role != 'arbiter':
        for data_type, data_location in input_data[role][party_id].items():
            table_info = {'name': data_location.split('.')[1], 'namespace': data_location.split('.')[0]}
            # 初始化StorageTableMeta类，获取里面的类变量partitions
            table_meta = storage.StorageTableMeta(name=table_info['name'], namespace=table_info['namespace'])
            if table_meta:
                table_partition = table_meta.get_partitions()
                if not min_partition or min_partition > table_partition:
                    min_partition = table_partition
    return min_partition
