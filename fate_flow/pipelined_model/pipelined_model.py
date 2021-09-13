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
import importlib
import inspect
import os
import shutil
import base64
from ruamel import yaml

from os.path import join, getsize
from fate_arch.common import file_utils
from fate_arch.protobuf.python import default_empty_fill_pb2
from fate_flow.settings import stat_logger, TEMP_DIRECTORY

# 定义一个管道模型
# 被调：
# 被fate_flow.apps.model_app.py里面的operate_model函数调用
# 被fate_flow.apps.model_app.py里面的get_predict_conf函数调用
# 被fate_flow.operation.job_tracker.py里面的__init__函数调用
# 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数调用
# 被fate_flow.pipelined_model.migrate_model.py里面的import_from_files函数调用
# 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
# 被fate_flow.pipelined_model.mysql_model_storage.py里面的store函数调用
# 被fate_flow.pipelined_model.mysql_model_storage.py里面的restore函数调用
# 被fate_flow.pipelined_model.publish_model.py里面的download_model函数调用
# 被fate_flow.pipelined_model.redis_model_storage.py里面的store函数调用
# 被fate_flow.pipelined_model.redis_model_storage.py里面的restore函数调用
# 被fate_flow.utils.model_utils.py里面的query_model_info_from_file函数调用
# 被fate_flow.utils.model_utils.py里面的gather_model_info_data函数调用
# 被fate_flow.utils.model_utils.py里面的check_before_deploy函数调用
# 被fate_flow.utils.model_utils.py里面的check_if_deployed函数调用
class PipelinedModel(object):
    def __init__(self, model_id, model_version):
        """
        Support operations on FATE PipelinedModels
        TODO: add lock
        :param model_id: the model id stored at the local party.
        :param model_version: the model version.
        """
        self.model_id = model_id
        self.model_version = model_version
        self.model_path = os.path.join(file_utils.get_project_base_directory(), "model_local_cache", model_id, model_version)
        self.define_proto_path = os.path.join(self.model_path, "define", "proto")
        self.define_meta_path = os.path.join(self.model_path, "define", "define_meta.yaml")
        self.variables_index_path = os.path.join(self.model_path, "variables", "index")
        self.variables_data_path = os.path.join(self.model_path, "variables", "data")
        self.default_archive_format = "zip"

    # 在本地缓存生成管道模型
    # 被fate_flow.operation.job_tracker.py里面的init_pipelined_model函数调用
    def create_pipelined_model(self):
        if os.path.exists(self.model_path):
            raise Exception("Model creation failed because it has already been created, model cache path is {}".format(
                self.model_path
            ))
        else:
            os.makedirs(self.model_path, exist_ok=False)
        for path in [self.variables_index_path, self.variables_data_path]:
            os.makedirs(path, exist_ok=False)
        shutil.copytree(os.path.join(file_utils.get_python_base_directory(), "federatedml", "protobuf", "proto"), self.define_proto_path)
        with open(self.define_meta_path, "w", encoding="utf-8") as fw:
            yaml.dump({"describe": "This is the model definition meta"}, fw, Dumper=yaml.RoundTripDumper)

    # 在保存组件模型
    # 被调：
    # 被fate_flow.operation.job_tracker.py里面的save_output_model函数调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
    def save_component_model(self, component_name, component_module_name, model_alias, model_buffers):
        model_proto_index = {}
        component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
        # os.makedirs() 方法用于递归创建目录。
        os.makedirs(component_model_storage_path, exist_ok=True)
        for model_name, buffer_object in model_buffers.items():
            storage_path = os.path.join(component_model_storage_path, model_name)
            buffer_object_serialized_string = buffer_object.SerializeToString()
            if not buffer_object_serialized_string:
                fill_message = default_empty_fill_pb2.DefaultEmptyFillMessage()
                fill_message.flag = 'set'
                buffer_object_serialized_string = fill_message.SerializeToString()
            with open(storage_path, "wb") as fw:
                fw.write(buffer_object_serialized_string)
            model_proto_index[model_name] = type(buffer_object).__name__   # index of model name and proto buffer class name
            stat_logger.info("Save {} {} {} buffer".format(component_name, model_alias, model_name))
        self.update_component_meta(component_name=component_name,
                                   component_module_name=component_module_name,
                                   model_alias=model_alias,
                                   model_proto_index=model_proto_index)
        stat_logger.info("Save {} {} successfully".format(component_name, model_alias))

    # 从文件中读取组件模型
    # 被调：
    # 被fate_flow.apps.model_app.py里面的operate_model函数调用
    # 被fate_flow.apps.model_app.py里面的get_predict_conf函数调用
    # 被fate_flow.operation.job_tracker.py里面的get_output_model函数调用
    # 被fate_flow.operation.job_tracker.py里面的save_machine_learning_model_info函数调用
    # 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
    # 被fate_flow.utils.model_utils.py里面的gather_model_info_data函数调用
    # 被fate_flow.utils.model_utils.py里面的check_before_deploy函数调用
    # 被fate_flow.utils.model_utils.py里面的check_if_deployed函数调用
    def read_component_model(self, component_name, model_alias):
        # eg：guest#9999#arbiter-10000#guest-9999#host-10000#model_202108291647004995364\variables\data\dataio_0\dataio
        component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
        # 获取模型原型索引
        model_proto_index = self.get_model_proto_index(component_name=component_name,
                                                       model_alias=model_alias)
        model_buffers = {}
        for model_name, buffer_name in model_proto_index.items():
            with open(os.path.join(component_model_storage_path, model_name), "rb") as fr:
                # eg：DataIOMeta
                buffer_object_serialized_string = fr.read()
                # 解析原型，返回buffer_object类
                model_buffers[model_name] = self.parse_proto_object(buffer_name=buffer_name,
                                                                    buffer_object_serialized_string=buffer_object_serialized_string)
        return model_buffers

    # 收集模型，返回模型数据
    # 被调：
    # 被fate_flow.operation.job_tracker.py里面的collect_model函数调用
    # 被fate_flow.pipelined_model.publish_model.py里面的download_model函数调用
    # 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
    def collect_models(self, in_bytes=False, b64encode=True):
        model_buffers = {}
        # 打开文件
        with open(self.define_meta_path, "r", encoding="utf-8") as fr:
            define_index = yaml.safe_load(fr)
            for component_name in define_index.get("model_proto", {}).keys():
                for model_alias, model_proto_index in define_index["model_proto"][component_name].items():
                    component_model_storage_path = os.path.join(self.variables_data_path, component_name, model_alias)
                    for model_name, buffer_name in model_proto_index.items():
                        with open(os.path.join(component_model_storage_path, model_name), "rb") as fr:
                            buffer_object_serialized_string = fr.read()
                            if not in_bytes:
                                model_buffers[model_name] = self.parse_proto_object(buffer_name=buffer_name,
                                                                                    buffer_object_serialized_string=buffer_object_serialized_string)
                            else:
                                if b64encode:
                                    buffer_object_serialized_string = base64.b64encode(buffer_object_serialized_string).decode()
                                model_buffers["{}.{}:{}".format(component_name, model_alias, model_name)] = buffer_object_serialized_string
        return model_buffers

    # 设置模型的路径
    def set_model_path(self):
        self.model_path = os.path.join(file_utils.get_project_base_directory(), "model_local_cache",
                                       self.model_id, self.model_version)

    # 判断该路径下的文件是否存在
    # 被调：
    # 被fate_flow.apps.model_app.py里面的operate_model函数调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
    # 被fate_flow.pipelined_model.pipelined_model.py里面的packaging_model函数调用
    # 被fate_flow.utils.model_utils.py里面的gather_model_info_data函数调用
    # 被fate_flow.utils.model_utils.py里面的check_if_deployed函数调用
    def exists(self):
        return os.path.exists(self.model_path)

    # 保存模型文件
    # 被调：
    # 被fate_flow.operation.job_tracker.py里面的save_pipelined_model函数调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
    # 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数调用
    def save_pipeline(self, pipelined_buffer_object):
        buffer_object_serialized_string = pipelined_buffer_object.SerializeToString()
        if not buffer_object_serialized_string:
            fill_message = default_empty_fill_pb2.DefaultEmptyFillMessage()
            fill_message.flag = 'set'
            buffer_object_serialized_string = fill_message.SerializeToString()
        with open(os.path.join(self.model_path, "pipeline.pb"), "wb") as fw:
            fw.write(buffer_object_serialized_string)

    # 打包模型
    # 被调：
    # 被fate_flow.pipelined_model.migrate_model.py里面的migration函数调用
    # 被fate_flow.apps.model_app.py里面的operate_model函数调用
    # 被fate_flow.pipelined_model.mysql_model_storage.py里面的store函数调用
    # 被fate_flow.pipelined_model.redis_model_storage.py里面的store函数调用
    def packaging_model(self):
        if not self.exists():
            raise Exception("Can not found {} {} model local cache".format(self.model_id, self.model_version))
        # shutil.make_archive 压缩文件
        archive_file_path = shutil.make_archive(base_name=self.archive_model_base_path(), format=self.default_archive_format, root_dir=self.model_path)
        stat_logger.info("Make model {} {} archive on {} successfully".format(self.model_id,
                                                                              self.model_version,
                                                                              archive_file_path))
        return archive_file_path

    # 拆解模型
    # 被调：
    # 被fate_flow.apps.model_app.py里面的operate_model函数调用
    # 被fate_flow.pipelined_model.mysql_model_storage.py里面的store函数调用
    # 被fate_flow.pipelined_model.redis_model_storage.py里面的store函数调用
    # 被fate_flow.pipelined_model.migrate_model.py里面的import_from_files函数调用
    def unpack_model(self, archive_file_path: str):
        if os.path.exists(self.model_path):
            raise Exception("Model {} {} local cache already existed".format(self.model_id, self.model_version))
        '''
        make_archive()
        功能：归档函数，归档操作
        格式：shutil.make_archive(‘目标文件路径’,‘归档文件后缀’,‘需要归档的目录’)
        返回值：归档文件的最终路径
        
        unpack_archive()
        功能：解包操作
        格式：shutil.unpack_archive(‘归档文件路径’,‘解包目标文件夹’)
        返回值:None
        注意：文件夹不存在会新建文件夹
        '''
        shutil.unpack_archive(archive_file_path, self.model_path)
        stat_logger.info("Unpack model archive to {}".format(self.model_path))


    # 更新组件元数据到yaml中
    # 被调：
    # 被fate_flow.pipelined_model.pipelined_model.py里面的save_component_model函数调用
    def update_component_meta(self, component_name, component_module_name, model_alias, model_proto_index):
        """
        update meta info yaml
        TODO: with lock
        :param component_name:
        :param component_module_name:
        :param model_alias:
        :param model_proto_index:
        :return:
        """
        with open(self.define_meta_path, "r", encoding="utf-8") as fr:
            define_index = yaml.safe_load(fr)
        with open(self.define_meta_path, "w", encoding="utf-8") as fw:
            define_index["component_define"] = define_index.get("component_define", {})
            define_index["component_define"][component_name] = define_index["component_define"].get(component_name, {})
            define_index["component_define"][component_name].update({"module_name": component_module_name})
            define_index["model_proto"] = define_index.get("model_proto", {})
            define_index["model_proto"][component_name] = define_index["model_proto"].get(component_name, {})
            define_index["model_proto"][component_name][model_alias] = define_index["model_proto"][component_name].get(model_alias, {})
            define_index["model_proto"][component_name][model_alias].update(model_proto_index)
            yaml.dump(define_index, fw, Dumper=yaml.RoundTripDumper)


    # 获取模型原型索引
    # 在guest#9999#arbiter-10000#guest-9999#host-10000#model_202108291647004995364\define目录下
    # 被调：
    # 被fate_flow.pipelined_model.pipelined_model.py里面的read_component_model函数调用
    def get_model_proto_index(self, component_name, model_alias):
        with open(self.define_meta_path, "r", encoding="utf-8") as fr:
            # 载入define_meta.yaml文件
            define_index = yaml.safe_load(fr)
            return define_index.get("model_proto", {}).get(component_name, {}).get(model_alias, {})

    '''
    define_meta.yaml文件部分内容：
    
    model_proto:
      dataio_0:
        dataio:
          DataIOMeta: DataIOMeta
          DataIOParam: DataIOParam
      homo_nn_0:
        homo_nn:
          HomoNNModelMeta: NNModelMeta
          HomoNNModelParam: NNModelParam
      homo_nn_1:
        homo_nn2:
          HomoNNModelMeta: NNModelMeta
          HomoNNModelParam: NNModelParam
      pipeline:
        pipeline:
          Pipeline: Pipeline
    '''


    # 获取组件定义
    # 被调:
    # 被fate_flow.operation.job_tracker.py里面的get_component_define函数调用
    def get_component_define(self, component_name=None):
        with open(self.define_meta_path, "r", encoding="utf-8") as fr:
            define_index = yaml.safe_load(fr)
            if component_name:
                return define_index.get("component_define", {}).get(component_name, {})
            else:
                return define_index.get("component_define", {})

    # 解析原型
    # 被调：
    # 被fate_flow.pipelined_model.pipelined_model.py里面的read_component_model函数调用
    # 被fate_flow.pipelined_model.pipelined_model.py里面的collect_models函数调用
    def parse_proto_object(self, buffer_name, buffer_object_serialized_string):
        try:
            # 获取原型缓冲区里的类
            buffer_object = self.get_proto_buffer_class(buffer_name)()
        except Exception as e:
            stat_logger.exception("Can not restore proto buffer object", e)
            raise e
        try:
            buffer_object.ParseFromString(buffer_object_serialized_string)
            stat_logger.info('parse {} proto object normal'.format(type(buffer_object).__name__))
            return buffer_object
        except Exception as e1:
            try:
                fill_message = default_empty_fill_pb2.DefaultEmptyFillMessage()
                fill_message.ParseFromString(buffer_object_serialized_string)
                buffer_object.ParseFromString(bytes())
                stat_logger.info('parse {} proto object with default values'.format(type(buffer_object).__name__))
                return buffer_object
            except Exception as e2:
                stat_logger.exception(e2)
                raise e1


    # 获取原型缓冲区里的类
    # 被调：
    # 被fate_flow.pipelined_model.pipelined_model.py里面的parse_proto_object函数调用
    @classmethod
    def get_proto_buffer_class(cls, buffer_name):
        # eg：/data/projects/fate/python/federatedml/protobuf/generated
        package_path = os.path.join(file_utils.get_python_base_directory(), 'federatedml', 'protobuf', 'generated')
        package_python_path = 'federatedml.protobuf.generated'
        # os.listdir() 方法用于返回指定的文件夹包含的文件或文件夹的名字的列表。
        for f in os.listdir(package_path):
            if f.startswith('.'):
                continue
            try:
                # Python rstrip() 删除 string 字符串末尾的指定字符（默认为空格）.
                proto_module = importlib.import_module(package_python_path + '.' + f.rstrip('.py'))
                for name, obj in inspect.getmembers(proto_module):
                    if inspect.isclass(obj) and name == buffer_name:
                        return obj
            except Exception as e:
                stat_logger.warning(e)
        else:
            return None

    # 获取模型基础路径
    # 被调：
    # 被fate_flow.pipelined_model.pipelined_model.py里面的packaging_model函数调用
    # 被fate_flow.pipelined_model.pipelined_model.py里面的archive_model_file_path函数调用
    def archive_model_base_path(self):
        return os.path.join(TEMP_DIRECTORY, "{}_{}".format(self.model_id, self.model_version))

    # 获取模型文件路径
    # 被调：
    # 被fate_flow.pipelined_model.mysql_model_storage.py里面的restore函数调用
    # 被fate_flow.pipelined_model.redis_model_storage.py里面的restore函数调用
    def archive_model_file_path(self):
        return "{}.{}".format(self.archive_model_base_path(), self.default_archive_format)

    # 计算模型文件大小
    # 被调：
    # 被fate_flow.apps.model_app.py里面的operate_model函数调用
    # 被fate_flow.operation.job_tracker.py里面的get_model_size函数调用
    # 被fate_flow.pipelined_model.deploy_model.py里面的deploy函数调用
    # 被fate_flow.utils.model_utils.py里面的query_model_info_from_file函数调用
    def calculate_model_file_size(self):
        size = 0
        for root, dirs, files in os.walk(self.model_path):
            size += sum([getsize(join(root, name)) for name in files])
        return round(size/1024)
