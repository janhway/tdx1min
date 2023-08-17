import datetime
import math
import os
import threading
import time
from pathlib import Path
from typing import List, Tuple, Dict, Optional

from pytdx.hq import TdxHq_API
from tdx1min.trade_calendar import IS_TEST_RUN
from tdx1min.vnlog import loge, logd, logi, logw


def _tdx_hq_host(positions: List[str] = None) -> dict:
    file = os.path.dirname(__file__)
    file = os.path.join(file, 'connect.cfg')
    tmp_path = Path(file)
    if not tmp_path.exists():
        file = Path.cwd()
        file = os.path.join(file, 'connect.cfg')

    hq_hosts: dict = {}

    with open(file, "r", encoding='utf8') as fp:
        lines = fp.readlines()

    hq_start = False
    for line in lines:
        line = line.strip()
        if not line:
            continue

        if not hq_start:
            tmp_idx = line.find('[HQHOST]')
            if tmp_idx != -1:
                hq_start = True
            continue

        if line[0] == '[':
            break

        # HostName01 = 深圳双线主站1
        # IPAddress01 = 110.41.147.114
        # Port01 = 7709
        fields = ["HostName", "IPAddress", "Port"]
        for field in fields:
            if line.startswith(field):
                hid = line[len(field):len(field) + 2]
                if hid not in hq_hosts:
                    hq_hosts[hid]: dict = {}
                hq_hosts[hid][field] = line[len(field) + 3:]
                if field == 'Port' and hq_hosts[hid][field] != '7709':
                    del (hq_hosts[hid])

    if positions:
        hq_hosts_spec = {}
        for k in hq_hosts:
            for position in positions:
                if hq_hosts[k]["HostName"].find(position) != -1:
                    hq_hosts_spec[k] = hq_hosts[k]
                    break

        return hq_hosts_spec

    return hq_hosts


def get_hq_hosts() -> Tuple[List[str], Dict[str, Tuple[str, str, int]]]:
    use_test_host = 0
    if use_test_host and IS_TEST_RUN:
        from pytdx.config.hosts import hq_hosts
        hosts = hq_hosts
    else:
        tmp_hosts = _tdx_hq_host(positions=['深圳', '上海'])
        hosts = [(tmp_hosts[h]['HostName'], tmp_hosts[h]['IPAddress'], int(tmp_hosts[h]['Port'])) for h in tmp_hosts]

    ips = [h[1] for h in hosts]

    hosts_mp = {}
    for h in hosts:
        hosts_mp[h[1]] = h
    logi("hq_hosts num {}".format(len(ips)))
    return ips, hosts_mp


class ApiPool(object):

    def __init__(self, max_num):
        self.period = 5
        self.heartbeat_interval = 30

        self.worker_thread = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        tmp = get_hq_hosts()
        self.idle_ips: List[str] = tmp[0]
        self.host_mp: Dict[str, Tuple[str, str, int]] = tmp[1]
        self.dont_use_ips: List[str] = []
        self.max_num = max_num
        if self.max_num > len(self.idle_ips):
            self.max_num = len(self.idle_ips)
        self.min_num = math.ceil(self.max_num/2)
        logd("max_num={} min_num={} idle_ips_num={} idle_ips={}"
             .format(self.max_num, self.min_num, len(self.idle_ips), self.idle_ips))

        self.idle_pool: List[TdxHq_API] = []
        self.inuse_pool: List[TdxHq_API] = []
        self.fail_pool: List[TdxHq_API] = []

    # def __getitem__(self, item):
    #     return self.idle_pool[item]

    def dont_work_time(self):
        now = datetime.datetime.now()

        # 当前slot开始之后的若干秒内
        if (now.minute // self.period) * self.period == now.minute:
            if 0 <= now.second <= 20:
                return True

        # 当前slot结束之前的若干秒
        if (now.minute // self.period) * self.period + self.period - 1 == now.minute:
            if now.second >= 50:
                return True

        return False

    def alloc_api(self, num):  # 要尽快返回
        with self.lock:
            if num >= len(self.idle_pool):
                self.inuse_pool = self.idle_pool
                self.idle_pool = []
            else:
                self.inuse_pool = self.idle_pool[0:num]
                self.idle_pool = self.idle_pool[num:]

        logd("pool idle_num={} inuse_num={} fail_num={}, ip dont_use_num={}"
             .format(len(self.idle_pool), len(self.inuse_pool), len(self.fail_pool), len(self.dont_use_ips)))
        return self.inuse_pool

    def release_api(self):  # 要尽快返回
        with self.lock:
            for apix in self.inuse_pool:
                if apix.last_transaction_failed:
                    self.fail_pool.append(apix)
                else:
                    self.idle_pool.append(apix)
            self.inuse_pool = []

        logd("pool idle_num={} inuse_num={} fail_num={}, ip dont_use_num={}"
             .format(len(self.idle_pool), len(self.inuse_pool), len(self.fail_pool), len(self.dont_use_ips)))

    def start(self):
        logd("init idle api pool")
        # num_at_least = self.max_num//2 + 1
        while len(self.idle_ips) and len(self.idle_pool) < self.min_num:
            self.do_adding_api()

        logd("start work thread")
        self.worker_thread = threading.Thread(target=self.run)
        self.worker_thread.daemon = True
        self.worker_thread.start()

    def stop(self):
        self.stop_event.set()
        if self.worker_thread.is_alive():
            self.worker_thread.join()
        self.worker_thread = None

    def run(self):
        tmp_count = 0
        while not self.stop_event.is_set():
            fails: List[TdxHq_API] = []
            with self.lock:
                if self.fail_pool:
                    fails = self.fail_pool
                    self.fail_pool = []
            if fails:
                try:
                    for api in fails:
                        api.close()
                        self.idle_ips.append(api.ip)
                except Exception as e:
                    loge("".format(e))

            tmp_count += 1
            if tmp_count % 30 == 0:
                logd("pool idle_num={} inuse_num={} fail_num={}, ip dont_use_num={}"
                     .format(len(self.idle_pool), len(self.inuse_pool), len(self.fail_pool), len(self.dont_use_ips)))

            if self.inuse_pool or (self.dont_work_time() and len(self.idle_pool) >= self.min_num):
                self.stop_event.wait(2.)
                continue

            #  补全连接数
            has_do, spent = self.do_adding_api()
            if has_do:
                if spent < 0.2:
                    self.stop_event.wait(0.5)
                continue

            # 发送心跳
            has_do, spent = self.do_heartbeat()
            if not has_do:
                self.stop_event.wait(2.)
            elif spent < 0.5:
                self.stop_event.wait(1)

        logi("release resource.")
        try:
            for api in self.idle_pool:
                api.close()
            for api in self.inuse_pool:
                api.close()
            for api in self.fail_pool:
                api.close()
        except Exception as e:
            loge("exception {}".format(e))
        logi("thread quit.")

    # has_do, spent 返回是否有干活和花费时间
    def do_heartbeat(self):
        if self.dont_work_time():
            return False, 0.0

        hb_api: Optional[TdxHq_API] = None
        last_ack_time = time.time()
        idx = -1
        with self.lock:
            for i, api in enumerate(self.idle_pool):
                if api.last_ack_time < last_ack_time:
                    last_ack_time = api.last_ack_time
                    idx = i
            if last_ack_time + self.heartbeat_interval <= time.time():
                hb_api = self.idle_pool[idx]
                self.idle_pool.remove(hb_api)

        if hb_api:
            start = time.time()
            try:
                hb_api.do_heartbeat()
                if hb_api.last_transaction_failed:
                    self.idle_ips.append(hb_api.ip)
                    hb_api.close()
                    loge("host {} heartbeat fail. close it".format(hb_api.ip))
                else:
                    with self.lock:
                        self.idle_pool.append(hb_api)
            except Exception as e:
                loge("exception {}".format(e))

            spent = round(time.time() - start, 3)
            if spent >= 1.0:
                logw("host {} heatbeat spent={}".format(hb_api.ip, spent))
            return True, spent
        else:
            return False, 0.0

    # has_do, spent 返回是否有干活和花费时间
    def do_adding_api(self) -> Tuple[bool, float]:
        if len(self.idle_pool) >= self.max_num:
            return False, 0.0

        # 确保不超过最大数目
        with self.lock:
            pool_len = len(self.inuse_pool) + len(self.idle_pool)
            if pool_len >= self.max_num:
                return False, 0.0

        # 到这里，我们确定需要增加api
        if self.idle_ips:
            ip = self.idle_ips[0]
            self.idle_ips.remove(ip)
        elif self.dont_use_ips and (pool_len < self.min_num or not self.dont_work_time()):
            ip = self.dont_use_ips[0]
            self.dont_use_ips.remove(ip)
            logi("use ip from dont_use_ips. ip={}".format(ip))
        else:
            if pool_len < self.min_num:
                loge("no ip. pool idle_num={} inuse_num={} fail_num={}, ip dont_use_num={}"
                     .format(len(self.idle_pool), len(self.inuse_pool), len(self.fail_pool), len(self.dont_use_ips)))
            return False, 0.0

        port = self.host_mp[ip][2]
        start = time.time()
        try:
            api = TdxHq_API(auto_retry=False, heartbeat=False)
            r = api.connect(ip=ip, port=int(port), time_out=5)
            if r:
                with self.lock:
                    self.idle_pool.append(api)
                logi("succeed to add api {}".format(api.ip))
            else:
                api.close()
                self.dont_use_ips.append(ip)
                loge("fail to add api {} dont_use_ips={}".format(api.ip, self.dont_use_ips))
        except Exception as e:
            loge("exception {}".format(e))

        spent = round(time.time() - start, 3)

        logd("spent={} len(idle_ips)={} idle_ips={}".format(spent, len(self.idle_ips), self.idle_ips))

        logd("pool idle_num={} inuse_num={} fail_num={}, ip dont_use_num={}"
             .format(len(self.idle_pool), len(self.inuse_pool), len(self.fail_pool), len(self.dont_use_ips)))
        return True, spent


if __name__ == '__main__':
    api_pool: ApiPool = ApiPool(20)
    api_pool.start()
    while True:
        pool: List[TdxHq_API] = api_pool.alloc_api(3)
        for x in pool:
            print(x.ip)
            x.do_heartbeat()
        time.sleep(5)
        api_pool.release_api()
        time.sleep(20)
