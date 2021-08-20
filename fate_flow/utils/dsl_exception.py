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

# 定义DSL异常基类
class BaseDSLException(Exception):
    def __init__(self, msg):
        self.msg = msg


# 定义DSL不存在错误
class DSLNotExistError(BaseDSLException):
    def __str__(self):
        return "There are no dsl, please check if the role and party id are correct"


# 定义提交的config不存在错误
class SubmitConfNotExistError(Exception):
    def __str__(self):
        return "SubMitConf is None, does not exist"


# 定义DSL中没有对应组件错误
class ComponentFieldNotExistError(Exception):
    def __str__(self):
        return "No components filed in dsl, please have a check!"


# 定义模块异常
class ModuleException(Exception):
    def __init__(self, component=None, module=None, input_model=None, input_data=None, output_model=None,
                 output_data=None, other_info=None):
        self.component = component
        self.module = module
        self.input_model = input_model
        self.input_data = input_data
        self.output_data = output_data
        self.output_model = output_model
        self.other_info = other_info


# 继承模块异常，定义组件不存在错误
class ComponentNotExistError(ModuleException):
    def __str__(self):
        return "Component {} does not exist, please have a check".format(self.component)


# 继承模块异常，定义DSL中不存在对应模块错误
class ModuleFieldNotExistError(ModuleException):
    def __str__(self):
        return "Component {}, module field does not exist in dsl, please have a check".format(self.component)


# 继承模块异常，定义模块不存在错误
class ModuleNotExistError(ModuleException):
    def __str__(self):
        return "Component {}, module {} does not exist under the fold federatedml.setting_conf".format(self.component,
                                                                                                       self.module)


# 继承模块异常，定义模块配置错误
class ModuleConfigError(ModuleException):
    def __str__(self):
        return "Component {}, module {} config error, message is {}".format(self.component, self.module,
                                                                            self.other_info[0])


# 定义在提交的config中不存在数据错误
class DataNotExistInSubmitConfError(BaseDSLException):
    def __str__(self):
        return "{} does not exist in submit conf's data, please check!".format(self.msg)


# 定义默认运行config不存在错误
class DefaultRuntimeConfNotExistError(ModuleException):
    def __str__(self):
        return "Default runtime conf of component {}, module {}, does not exist".format(self.component, self.module)


# 定义默认运行config不是json文件错误
class DefaultRuntimeConfNotJsonError(ModuleException):
    def __str__(self):
        return "Default runtime conf of component {}, module {} should be json format file, but error occur: {}".format(self.component, self.module, self.other_info)


# 定义模块的输入组件不存在错误
class ModelInputComponentNotExistError(ModuleException):
    def __str__(self):
        return "Component {}'s model input {} does not exist".format(self.component, self.input_model)


# 定义模块输入名不存在错误
class ModelInputNameNotExistError(ModuleException):
    def __str__(self):
        return "Component {}' s model input {}'s output model {} does not exist".format(self.component,
                                                                                        self.input_model,
                                                                                        self.other_info)

# 定义组件输入类型错误
class ComponentInputTypeError(ModuleException):
    def __str__(self):
        return "Input of component {} should be dict".format(self.component)


# 定义组件输出类型错误
class ComponentOutputTypeError(ModuleException):
    def __str__(self):
        return "Output of component {} should be dict, but {} does not match".format(self.component, self.other_info)


# 定义组件输入数据类型错误
class ComponentInputDataTypeError(ModuleException):
    def __str__(self):
        return "Component {}'s input data type should be dict".format(self.component)


# 定义组件输入数据数值类型错误
class ComponentInputDataValueTypeError(ModuleException):
    def __str__(self):
        return "Component {}'s input data type should be list, but {} not match".format(self.component, self.other_info)


# 定义组件输入模块数据类型错误
class ComponentInputModelValueTypeError(ModuleException):
    def __str__(self):
        return "Component {}'s input model value type should be list, but {} not match".format(self.component,
                                                                                               self.other_info)


# 定义组件输出key类型错误
class ComponentOutputKeyTypeError(ModuleException):
    def __str__(self):
        return "Component {}'s output key {} value type should be list".format(self.component,
                                                                               self.other_info)


# 定义数据输入组件不存在错误
class DataInputComponentNotExistError(ModuleException):
    def __str__(self):
        return "Component {}'s data input {} does not exist".format(self.component, self.input_data)


# 定义数据输入名字不存在错误
class DataInputNameNotExistError(ModuleException):
    def __str__(self):
        return "Component {}' data input {}'s output data {} does not exist".format(self.component,
                                                                                    self.input_data,
                                                                                    self.other_info)


# 定义参数异常
class ParameterException(Exception):
    def __init__(self, parameter, role=None, msg=None):
        self.parameter = parameter
        self.role = role
        self.msg = msg


# 定义参数类不存在错误
class ParamClassNotExistError(ModuleException):
    def __str__(self):
        return "Component {}, module {}'s param class {} does not exist".format(self.component, self.module,
                                                                                self.other_info)


# 定义角色参数不存在错误
class RoleParameterNotListError(ParameterException):
    def __str__(self):
        return "Role {} role parameter {} should be list".format(self.role, self.parameter)


# 定义角色参数前后不一致错误
class RoleParameterNotConsistencyError(ParameterException):
    def __str__(self):
        return "Role {} role parameter {} should be a list of same length with roles".format(self.role, self.parameter)


# 定义参数检查错误
class ParameterCheckError(ModuleException):
    def __str__(self):
        return "Component {}, module {}, does not pass component check, error msg is {}".format(self.component,
                                                                                                self.module,
                                                                                                self.other_info)


# 定义多余参数错误
class RedundantParameterError(ParameterCheckError):
    def __str__(self):
        return "Component {}, module {}, has redundant parameter {}".format(self.component,
                                                                            self.module,
                                                                            self.other_info)


# 定义组件重复错误
class ComponentDuplicateError(ModuleException):
    def __str__(self):
        return "Component {} is duplicated, running before".format(self.component)


# 定义组件的拓扑排序度数不为零错误
class DegreeNotZeroError(ModuleException):
    def __str__(self):
        return "Component {}' in degree should be zero for topological sort".format(self.component)


# 定义模式错误
class ModeError(BaseDSLException):
    def __str__(self):
        return "dsl' s mode should be train or predict"


# 定义循环错误
class LoopError(Exception):
    def __init__(self, components=None):
        self.components = components

    def __str__(self):
        if self.components is not None:
            return "{} form a loop".format("->".join(self.components))
        else:
            # 组件关系形成一个依赖循环
            return "component relationship forms a dependency loop"


# 定义命名错误
class NamingError(ModuleException):
    def __str__(self):
        return "Component's name should be format of name_index, index is start from 0 " + \
               "and be consecutive for same module, {} is error".format(self.component)


# 定义命名索引错误
class NamingIndexError(ModuleException):
    def __str__(self):
        return "Component {}'s index should be an integer start from 0".format(self.component)


# 定义命名格式错误
class NamingFormatError(ModuleException):
    def __str__(self):
        return "Component name {}'is not illegal, it should be consits of letter, digit, '-'  and '_'".format(self.component)


# 定义组件多映射错误
class ComponentMultiMappingError(ModuleException):
    def __str__(self):
        return "Component prefix {} should be used for only one module, but another".format(self.component)


# 定义部署组件不存在错误
class DeployComponentNotExistError(BaseDSLException):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return "components {} not exist in training dsl, can not deploy!!!".format(self.msg)

