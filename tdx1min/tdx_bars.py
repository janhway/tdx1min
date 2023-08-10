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


def tdx_slot_equal(tdx_slot_std, tdx_slot_option):
    if tdx_slot_std == tdx_slot_option:
        return True
    # 在11:29到11:30这一分钟，在11:30后查询，tdx可能用1300表示，而不是1130
    if tdx_slot_std == "1130" and tdx_slot_option == "1300":
        return True
    return False


def query_bar1min_worker(mc_list: List[Tuple[int, str]],
                         api: TdxHq_API, exact: bool = True) -> Tuple[dict, List, List]:
    start = time.time()
    logi("host={} num={}".format(api.ip, len(mc_list)))

    if not exact:
        logi("non-exact query. market_code_list={}".format(mc_list))

    mp = {}
    non_exist = []
    lost = []
    # now = datetime.datetime.now()
    # 分钟线我们用开始时间点标识，tdx用结束时间点表示，比如 从09:30到09:31这一分钟，我们用0930表示，tdx用0931表示
    # 在11:29到11:30这一分钟，在11:30后查询，tdx可能用1300表示
    # my_slot = (now - datetime.timedelta(minutes=1)).strftime("%H%M")
    tdx_slot_std = datetime.datetime.now().strftime("%H%M")
    today = cur_date()

    for i in range(0, len(mc_list), 15):
        end = i + 15 if i + 15 < len(mc_list) else len(mc_list)
        data = api.get_security_bars_x(8, mc_list[i:end], 0, 2)
        if not exact:
            logi("non-exact query. return data={}".format(data))

        for market, code in mc_list[i:end]:
            if code not in data:
                non_exist.append((market, code))
                loge("code {} has no bar info.".format(code))
                continue

            d: List[dict] = data[code]
            found = False
            for it in d:
                # 'datetime', '2023-08-09 15:00'
                tmp_tdx_datetime = it['datetime'].replace("-", "").replace(":", "").replace(" ", "")
                it_tdx_date = tmp_tdx_datetime[0:8]
                it_tdx_slot = tmp_tdx_datetime[8:]
                if it_tdx_date == today and tdx_slot_equal(tdx_slot_std, it_tdx_slot):
                    mp[code] = it
                    found = True

            if (not found) and (not exact):
                j = len(d) - 1
                mp[code] = d[j]  # 取最后一个最新的
                mp[code]['open'] = d[j]['close']  # open设置成跟close一样
                found = True

            if not found:
                lost.append((market, code))
                loge("code {} has no valid data {}  exact={}".format(code, d, exact))

            # mp[code] = data[code][0] # test code

    spent = round(time.time() - start, 2)
    logi("code num={} spent {} seconds. lost={} non_exist={} host={}"
         .format(len(mc_list), spent, api.ip, lost, non_exist))

    return mp, lost, non_exist


def query_bar1min(market_code_list: List[Tuple[int, str]],
                  api_pool: ApiPool, exact: bool = True) -> Tuple[dict, List, List]:

    start = time.time()

    min_step = 150

    step = math.ceil(len(market_code_list) / api_pool.num)
    max_workers = api_pool.num
    if step < min_step:
        step = min_step
        max_workers = math.ceil(len(market_code_list) / step)
    logi("worker num={} step={}".format(max_workers, step))

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in range(0, len(market_code_list), step):
            idx = int(i / step)
            end = i + step if i + step < len(market_code_list) else len(market_code_list)
            future = executor.submit(query_bar1min_worker, market_code_list[i:end], api_pool[idx], exact)
            futures.append(future)

    mp = {}
    lost = []
    non_exist = []
    for future in concurrent.futures.as_completed(futures):
        err = future.exception()
        if not err:
            tmp_mp, tmp_lost, tmp_non_exist = future.result()
            mp.update(tmp_mp)
            lost.extend(tmp_lost)
            non_exist.extend(tmp_non_exist)
        else:
            loge("error={}".format(err))
    spent = round(time.time() - start, 2)
    logi("code num={} spent {} seconds. lost={} non_exist={}".format(len(market_code_list), spent, lost, non_exist))

    return mp, lost, non_exist


def tdx_bar_main():
    mcodes, cfg = read_cfg()
    pre_tmap = cal_pre_tmap(cfg)
    valid_slots = day_1min_slots()
    logi("pre_tmap={} stock num={}".format(pre_tmap, len(mcodes)))

    tnow = datetime.datetime.now()
    t = datetime.datetime(year=tnow.year, month=tnow.month, day=tnow.day,
                          hour=tnow.hour, minute=tnow.minute, second=5, microsecond=0)
    # t += datetime.timedelta(minutes=1)
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

        mp, lost, non_exist = query_bar1min(mcodes, api_pool, exact=True)
        if lost:
            lost.extend(non_exist)
            tmp_mp, tmp_lost, non_exist = query_bar1min(lost, api_pool, exact=False)
            assert len(lost) == len(tmp_mp.keys())
            mp.update(tmp_mp)

        # 计算和保存stg指数信息
        # start = time.time()
        stg_open, stg_close = cal_open_close_new(last_slot, pre_tmap, cfg, mp)
        write_stg_price(last_slot, stg_open, stg_close)
        # spent = time.time() - start
        # logi("cal stg {} spent={}".format(last_slot, round(spent, 3)))

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