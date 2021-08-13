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

import dotenv
import typing

from fate_arch.common.file_utils import get_project_base_directory


def get_versions() -> typing.Mapping[str, typing.Any]:
    # Dotenv 是一个零依赖的模块，它能将环境变量中的变量从 .env 文件加载到 process.env 中。
    return dotenv.dotenv_values(
        # get_versions()方法从fate_arch.common.file_utils中调用get_project_base_directory()方法
        # 获取项目的基础路径
        dotenv_path=os.path.join(get_project_base_directory(), "fate.env")
    )


def get_eggroll_version() -> typing.Optional[str]:
    return get_versions().get("EGGROLL")


def get_fate_version() -> typing.Optional[str]:
    return get_versions().get("FATE")
