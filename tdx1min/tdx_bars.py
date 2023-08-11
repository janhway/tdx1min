import concurrent
import datetime
import math
import sys
import time
from typing import List, Tuple

from pytdx.hq import TdxHq_API
from tdx1min.db_ticks import BarMin, crt_barmin

from tdx1min.tdx_cfg import TDX_HQ_HOST, BAR_PERIOD
from tdx1min.tdx_stg import read_cfg, cal_pre_tmap, day_bar_slots, need_query, write_stg_price, cal_open_close_new
from tdx1min.trade_calendar import cur_date, now_is_tradedate
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


def get_tdx_last_slot(dt=None):
    if not dt:
        dt = datetime.datetime.now()
    # 分钟线我们用开始时间点标识，tdx用结束时间点表示，比如 从09:30到09:31这一分钟，我们用0930表示，tdx用0931表示
    # 在11:29到11:30这一分钟，在11:30后查询，tdx可能用1300表示
    m = (dt.minute // BAR_PERIOD) * BAR_PERIOD
    return f"{dt.hour:02d}{m:02d}"


def get_our_last_slot(dt=None):
    if not dt:
        dt = datetime.datetime.now()
    # 分钟线我们用开始时间点标识，tdx用结束时间点表示，比如 从09:30到09:31这一分钟，我们用0930表示，tdx用0931表示
    my_slot_time = (dt - datetime.timedelta(minutes=BAR_PERIOD))
    m = (my_slot_time.minute // BAR_PERIOD) * BAR_PERIOD
    return f"{my_slot_time.hour:02d}{m:02d}"


def query_bar_min_worker(mc_list: List[Tuple[int, str]],
                         api: TdxHq_API, exact: bool = True) -> Tuple[dict, List, List]:
    start = time.time()
    logi("host={} num={}".format(api.ip, len(mc_list)))

    if not exact:
        logi("non-exact query. market_code_list={}".format(mc_list))

    mp = {}
    non_exist = []
    lost = []
    tdx_slot_std = get_tdx_last_slot()
    today = cur_date()

    for i in range(0, len(mc_list), 15):
        end = i + 15 if i + 15 < len(mc_list) else len(mc_list)
        cat = 0 if BAR_PERIOD == 5 else 8
        data = api.get_security_bars_x(cat, mc_list[i:end], 0, 2)
        if not exact and (not tdx_slot_std > '1500'):
            logi("non-exact query. return data={}".format(data))

        for market, code in mc_list[i:end]:
            if code not in data:
                non_exist.append((market, code))
                loge("code {} has no bar info.".format(code))
                continue

            d: List[dict] = data[code]
            found = False
            for it in d:
                # 'datetime', '2023-08-09 15:00'   2023-08-10 14:55
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
         .format(len(mc_list), spent, lost, non_exist, api.ip))

    return mp, lost, non_exist


def query_bar_min(market_code_list: List[Tuple[int, str]],
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
            future = executor.submit(query_bar_min_worker, market_code_list[i:end], api_pool[idx], exact)
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


RUN_STATE = True


def set_run_state(state):
    global RUN_STATE
    RUN_STATE = state


def check_run_period():
    """"""
    if not now_is_tradedate():
        return False
    if not RUN_STATE:
        return False
    start = datetime.time(8, 45)
    end = datetime.time(17, 0)
    current_time = datetime.datetime.now().time()

    if start <= current_time <= end:
        return True

    return False


def tdx_bars(api_pool: ApiPool, stgtrd_cfg_path=None, output_path=None):
    mcodes, cfg = read_cfg(stgtrd_cfg_path)
    pre_tmap = cal_pre_tmap(cfg)
    valid_slots = day_bar_slots()
    logi("pre_tmap={} stock num={}".format(pre_tmap, len(mcodes)))

    tnow = datetime.datetime.now()
    m = (tnow.minute // BAR_PERIOD) * BAR_PERIOD
    t = datetime.datetime(year=tnow.year, month=tnow.month, day=tnow.day,
                          hour=tnow.hour, minute=m, second=5, microsecond=0)
    # t += datetime.timedelta(minutes=BAR_PERIOD)
    que = [t]
    logi("timer que {}".format(que))
    today_date = cur_date()

    mp0925 = None
    mp1455 = None
    while check_run_period():
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
            que.append(tt + datetime.timedelta(minutes=BAR_PERIOD))

        if not need_query():
            continue

        last_slot = get_our_last_slot()
        if last_slot not in valid_slots:
            continue  # comment for test
            pass

        mp, lost, non_exist = query_bar_min(mcodes, api_pool, exact=True)
        if lost:
            lost.extend(non_exist)
            tmp_mp, tmp_lost, non_exist = query_bar_min(lost, api_pool, exact=False)
            assert len(lost) == len(tmp_mp.keys())
            mp.update(tmp_mp)

        if last_slot == '0925':
            mp0925 = mp
            continue

        if last_slot == '1455':
            mp1455 = mp
            continue

        # 第一个应该是9.25-9.35 标记为0930
        if last_slot == '0930' and mp0925 is not None:
            logi("merge slot 0925 and 0930")
            for mp_code in mp:
                if mp_code in mp0925:
                    # 暂时注释掉合并功能 因为 9点之前查询的都会返回 0930-0935 这条K线  不太合理
                    # mp[mp_code]['open'] = mp0925[mp_code]['open']
                    pass
            mp0925 = None

        if last_slot == '1500' and mp1455 is not None:
            logi("merge slot 1455 and 1500")
            for mp_code in mp:
                if mp_code in mp1455:
                    mp[mp_code]['open'] = mp1455[mp_code]['open']
            mp1455 = None
            last_slot = '1455'

        # 计算和保存stg指数信息
        # start = time.time()
        stg_open, stg_close = cal_open_close_new(last_slot, pre_tmap, cfg, mp)
        write_stg_price(last_slot, stg_open, stg_close, output_path=output_path)
        # spent = time.time() - start
        # logi("cal stg {} spent={}".format(last_slot, round(spent, 3)))

        # begin save db. option
        start = time.time()
        db_bars = []
        for c in mp:
            b: dict = mp[c]
            db_bar: BarMin = BarMin(date=today_date, time=last_slot, code=c,
                                    open=b['open'], close=b['close'], fill_date=b['datetime'])
            db_bars.append(db_bar)
        crt_barmin(db_bars)
        spent = round(time.time() - start, 3)
        logi("save #{} bars to db spent {} seconds".format(len(mp), spent))
        if spent >= 1.:
            logw("save db spent too many time {}".format(spent))
        # end save db. option


def tdx_bar_main(stgtrd_cfg_path=None, output_path=None):
    api_pool: ApiPool = ApiPool(5)
    api_pool.start()

    try:
        tdx_bars(api_pool, stgtrd_cfg_path, output_path)

    except KeyboardInterrupt:
        logi("receive KeyboardInterrupt")

    logi("quit child process.")
    api_pool.stop()


def tst_query_barmin():
    market_code_list, _ = read_cfg()
    api_pool: ApiPool = ApiPool(5)
    api_pool.start()
    date = query_bar_min(market_code_list, api_pool)
    print(date)
    api_pool.stop()


if __name__ == '__main__':
    print("argv={}".format(sys.argv))
    tdx_bar_main()
