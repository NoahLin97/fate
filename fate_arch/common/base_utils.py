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
import base64
import datetime
import io
import json
import os
import pickle
import socket
import time
import uuid
from enum import Enum, IntEnum

from fate_arch.common.conf_utils import get_base_config


use_deserialize_safe_module = get_base_config('use_deserialize_safe_module', False)


class CustomJSONEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        super(CustomJSONEncoder, self).__init__(**kwargs)

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, datetime.timedelta):
            return str(obj)
        elif issubclass(type(obj), Enum) or issubclass(type(obj), IntEnum):
            return obj.value
        else:
            return json.JSONEncoder.default(self, obj)

# 使用uuid生成fate的id
def fate_uuid():
    return uuid.uuid1().hex


def string_to_bytes(string):
    return string if isinstance(string, bytes) else string.encode(encoding="utf-8")


def bytes_to_string(byte):
    return byte.decode(encoding="utf-8")


def json_dumps(src, byte=False, indent=None):
    if byte:
        return string_to_bytes(json.dumps(src, indent=indent, cls=CustomJSONEncoder))
    else:
        return json.dumps(src, indent=indent, cls=CustomJSONEncoder)

# 载入json
def json_loads(src, object_pairs_hook=None):
    if isinstance(src, bytes):
        return json.loads(bytes_to_string(src), object_pairs_hook=object_pairs_hook)
    else:
        return json.loads(src, object_pairs_hook=object_pairs_hook)


# current_timestamp方法返回当前时间戳
def current_timestamp():
    return int(time.time() * 1000)


def timestamp_to_date(timestamp, format_string="%Y-%m-%d %H:%M:%S"):
    timestamp = int(timestamp) / 1000
    time_array = time.localtime(timestamp)
    str_date = time.strftime(format_string, time_array)
    return str_date


def serialize_b64(src, to_str=False):
    dest = base64.b64encode(pickle.dumps(src))
    if not to_str:
        return dest
    else:
        return bytes_to_string(dest)


def deserialize_b64(src):
    src = base64.b64decode(string_to_bytes(src) if isinstance(src, str) else src)
    if use_deserialize_safe_module:
        return restricted_loads(src)
    return pickle.loads(src)


safe_module = {
    'federatedml',
    'numpy',
    'fate_flow'
}


class RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        import importlib
        if module.split('.')[0] in safe_module:
            _module = importlib.import_module(module)
            return getattr(_module, name)
        # Forbid everything else.
        raise pickle.UnpicklingError("global '%s.%s' is forbidden" %
                                     (module, name))


def restricted_loads(src):
    """Helper function analogous to pickle.loads()."""
    return RestrictedUnpickler(io.BytesIO(src)).load()


def get_lan_ip():
    if os.name != "nt":
        import fcntl
        import struct

        def get_interface_ip(ifname):
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            return socket.inet_ntoa(
                fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', string_to_bytes(ifname[:15])))[20:24])

    ip = socket.gethostbyname(socket.getfqdn())
    if ip.startswith("127.") and os.name != "nt":
        interfaces = [
            "bond1",
            "eth0",
            "eth1",
            "eth2",
            "wlan0",
            "wlan1",
            "wifi0",
            "ath0",
            "ath1",
            "ppp0",
        ]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError as e:
                pass
    return ip or ''
