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
import threading
import time
import random

# cron：
# 计划任务，是任务在约定的时间执行已经计划好的工作
# 被调：
# 被fate_flow.scheduler.dag_scheduler.py里面的DAGScheduler类所继承
# 被fate_flow.scheduler.detector.py里面的Detector类所继承
class Cron(threading.Thread):
    def __init__(self, interval, run_second=None, rand_size=None, title='', logger=None, lock=None):
        """

        :param interval: interval by millisecond
        :param run_second:
        :param rand_size:
        :param title:
        :param logger:
        :param lock:
        """
        super(Cron, self).__init__()
        self.interval = interval
        self.run_second = run_second
        self.rand_size = rand_size
        # 通过threading.Event()可以创建一个事件管理标志，该标志（event）默认为False，event对象主要有四种方法可以调用：
        # event.wait(timeout=None)：调用该方法的线程会被阻塞，如果设置了timeout参数，超时后，线程会停止阻塞继续执行；
        # event.set()：将event的标志设置为True，调用wait方法的所有线程将被唤醒；
        # event.clear()：将event的标志设置为False，调用wait方法的所有线程将被阻塞；
        # event.isSet()：判断event的标志是否为True。
        self.finished = threading.Event()
        self.title = title
        self.logger = logger
        self.lock = lock


    # 取消
    def cancel(self):
        self.finished.set()

    # 运行
    def run(self):
        def do():
            try:
                if not self.lock or self.lock.acquire(0):
                    self.run_do()
            except Exception as e:
                if self.logger:
                    self.logger.exception(e)
                else:
                    raise e
            finally:
                if self.lock and self.lock.locked():
                    self.lock.release()
        try:
            if self.logger and self.title:
                self.logger.info('%s cron start.' % self.title)

            if self.run_second is None:
                first_interval = self.interval
            else:
                now = int(round(time.time()*1000))
                delta = -now % 60000 + self.run_second*1000
                if delta > 0:
                    first_interval = delta
                else:
                    first_interval = 60*1000 + delta
            self.finished.wait(first_interval/1000)
            if not self.finished.is_set():
                do()

            while True:
                self.finished.wait((self.interval if self.rand_size is None else self.interval - random.randint(0, self.rand_size))/1000)
                if not self.finished.is_set():
                    do()
        except Exception as e:
            if self.logger:
                self.logger.exception(e)
            else:
                raise e

    # 运行
    def run_do(self):
        pass

