import concurrent
import datetime
import math
import time
from typing import List, Tuple, Dict, Any

from pytdx.hq import TdxHq_API
from tdx1min.db_ticks import Bar1Min, crt_bar1min

from tdx1min.tdx_cfg import TDX_HQ_HOST
from tdx1min.tdx_ticks import read_cfg, cal_pre_tmap, day_1min_slots, need_query, CfgItData, write_stg_price, \
    cal_open_close_new, cur_date
from tdx1min.vnlog import logi, loge, logw


class ApiPool(object):

    def __init__(self, max_num):
        tmp_hosts = [TDX_HQ_HOST[k] for k in TDX_HQ_HOST]
        self.num = max_num if len(tmp_hosts) > max_num else len(tmp_hosts)
        self.hosts = [(h['IPAddress'], h['Port']) for h in tmp_hosts[0:self.num]]
        self.active = False
        self.api_pool: List[TdxHq_API] = []

    def __getitem__(self, item):
        return self.api_pool[item]

    def start(self):
        if self.active:
            return
        self.active = True
        self.api_pool = []
        for ip, port in self.hosts:
            api = TdxHq_API(auto_retry=True)
            api.connect(ip=ip, port=int(port), time_out=60)
            self.api_pool.append(api)

    def stop(self):
        if not self.active:
            return
        for api in self.api_pool:
            api.close()
        self.active = False


def query_bar1min_worker(market_code_list: List[Tuple[int, str]], api: TdxHq_API):
    # market_code_list = read_cfg()
    mp = {}
    logi("host={} num={}".format(api.ip, len(market_code_list)))
    start = time.time()

    now = datetime.datetime.now()
    # my_slot = (now - datetime.timedelta(minutes=1)).strftime("%H%M")
    tdx_slot = datetime.datetime.now().strftime("%H%M")
    for i in range(0, len(market_code_list), 15):
        end = i + 15 if i + 15 < len(market_code_list) else len(market_code_list)
        data = api.get_security_bars_x(8, market_code_list[i:end], 0, 2)
        for code in data:
            for it in data[code]:
                # 'datetime', '2023-08-09 15:00'
                it_tdx_slot = it['datetime'][11:].replace(":", "")
                if it_tdx_slot == tdx_slot:
                    mp[code] = it
            # mp[code] = data[code][0] # test code

    spent = round(time.time() - start, 2)
    logi("host={} num={} spent {} seconds".format(api.ip, len(market_code_list), spent))
    return mp


# def create_api(host: str):
#     api = TdxHq_API(auto_retry=True)
#     api.connect(ip=host, port=7709, time_out=60)
#     return api


def query_bar1min(market_code_list: List[Tuple[int, str]], api_pool: ApiPool):
    # market_code_list = read_cfg()
    #
    # hosts = [TDX_HQ_HOST[k] for k in TDX_HQ_HOST]
    # max_num = 8 if len(hosts) > 8 else len(hosts)
    # api_pools = []
    # for i in range(max_num):
    #     api_pools.append(create_api(hosts[i]['IPAddress']))

    start = time.time()
    step = math.ceil(len(market_code_list) / api_pool.num)
    logi("worker num={} step={}".format(api_pool.num, step))
    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=api_pool.num) as executor:
        for i in range(0, len(market_code_list), step):
            idx = int(i / step)
            end = i + step if i + step < len(market_code_list) else len(market_code_list)
            future = executor.submit(query_bar1min_worker, market_code_list[i:end], api_pool[idx])
            futures.append(future)

    mp = {}
    for future in concurrent.futures.as_completed(futures):
        err = future.exception()
        if not err:
            r = future.result()
            mp.update(r)
        else:
            loge("error={}".format(err))
    spent = round(time.time() - start, 2)
    logi("num={} spent {} seconds".format(len(market_code_list), spent))
    return mp


def tdx_bar_main():
    mcodes, cfg = read_cfg()
    pre_tmap = cal_pre_tmap(cfg)
    valid_slots = day_1min_slots()
    logi("pre_tmap={} stock num={} valid_slots={}".format(pre_tmap, len(mcodes), valid_slots))

    tnow = datetime.datetime.now()
    t = datetime.datetime(year=tnow.year, month=tnow.month, day=tnow.day,
                          hour=tnow.hour, minute=tnow.minute, second=5, microsecond=0)
    t += datetime.timedelta(minutes=1)
    que = [t]
    logi("timer que {}".format(que))
    today_date = cur_date()

    api_pool: ApiPool = ApiPool(5)
    api_pool.start()

    while True:
        now = datetime.datetime.now()
        exp = []
        for tt in que:
            if tt < now:
                # print("=============",tt,now)
                exp.append(tt)
        if not exp:
            time.sleep(0.1)
            continue

        for tt in exp:
            que.remove(tt)
            que.append(tt + datetime.timedelta(minutes=1))

        if not need_query():
            continue

        now = datetime.datetime.now()
        last_slot = (now - datetime.timedelta(minutes=1)).strftime("%H%M")
        if last_slot not in valid_slots:
            continue
            pass
        mp = query_bar1min(mcodes, api_pool)

        # 计算和保存stg指数信息
        start = time.time()
        stg_open, stg_close = cal_open_close_new(last_slot, pre_tmap, cfg, mp)
        write_stg_price(last_slot, stg_open, stg_close)
        spent = time.time() - start
        logi("cal stg {} spent={}".format(last_slot, round(spent, 3)))

        # begin save db. option
        start = time.time()
        db_bars = []
        for c in mp:
            b: dict = mp[c]
            db_bar: Bar1Min = Bar1Min(date=today_date, time=last_slot, code=c,
                                      open_st=last_slot, open=b['open'],
                                      close_st=last_slot, close=b['close'], fill_date=b['datetime'])
            db_bars.append(db_bar)
        crt_bar1min(db_bars)
        spent = round(time.time() - start, 3)
        logi("save #{} bars to db spent {} seconds".format(len(mp), spent))
        if spent >= 1.:
            logw("save db spent too many time {}".format(spent))
        # end save db. option


def tst_query_bar1min():
    market_code_list, _ = read_cfg()
    api_pool: ApiPool = ApiPool(5)
    api_pool.start()
    date = query_bar1min(market_code_list, api_pool)
    print(date)
    api_pool.stop()


if __name__ == '__main__':
    tdx_bar_main()
    pass
