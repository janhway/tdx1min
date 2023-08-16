import concurrent
import datetime
import math
import sys
import time
from typing import List, Tuple

from pytdx.hq import TdxHq_API
from tdx1min.api_pool import ApiPool
from tdx1min.db_ticks import BarMin, crt_barmin

from tdx1min.tdx_cfg import BAR_PERIOD
from tdx1min.tdx_stg import read_cfg, cal_pre_tmap, day_bar_slots, write_stg_price, cal_open_close_new
from tdx1min.trade_calendar import IS_TEST_RUN, cur_date, now_is_tradedate
from tdx1min.vnlog import logi, loge, logw


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

    xnum = 10
    if not exact:
        if len(mc_list) < xnum:
            logi("non-exact query. market_code_list={}".format(mc_list))
        else:
            logi("non-exact query. market_code_num={}".format(len(mc_list)))

    mp = {}
    non_exist = []
    lost = []
    tdx_slot_std = get_tdx_last_slot()
    today = cur_date()

    for i in range(0, len(mc_list), 15):
        end = i + 15 if i + 15 < len(mc_list) else len(mc_list)
        cat = 0 if BAR_PERIOD == 5 else 8
        mc_slice = mc_list[i:end]
        data = api.get_security_bars_x(cat, mc_slice, 0, 2)
        if not data:
            loge("query return nothing, len(mc_list)={}. host={}".format(len(mc_slice), api.ip))
            non_exist.extend(mc_slice)
            continue

        if not exact:
            if len(data.keys()) < xnum:
                logi("non-exact query. return data={}".format(data))
            else:
                logi("non-exact query. return data_key_num={}".format(len(data.keys())))

        for market, code in mc_slice:
            # api故障时查询的data可能为None
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
                    break

            if (not found) and (not exact):
                j = len(d) - 1
                mp[code] = d[j]  # 取最后一个最新的
                mp[code]['open'] = mp[code]['close']  # open设置成跟close一样
                found = True

            if not found:
                lost.append((market, code))
                if not IS_TEST_RUN:
                    loge("code {} has no valid data {}  exact={}".format(code, d, exact))

    spent = round(time.time() - start, 2)
    if len(lost) < xnum and len(non_exist) < xnum:
        logi("code num={} spent {} seconds. lost={} non_exist={} host={}"
             .format(len(mc_list), spent, lost, non_exist, api.ip))
    else:
        logi("code num={} spent {} seconds. lost_num={} non_exist_num={} host={}"
             .format(len(mc_list), spent, len(lost), len(non_exist), api.ip))

    return mp, lost, non_exist


def query_bar_min(market_code_list: List[Tuple[int, str]],
                  api_pool: ApiPool, exact: bool = True) -> Tuple[dict, List, List]:
    start = time.time()
    min_step = 150
    xnum = 10
    pl: List[TdxHq_API] = api_pool.alloc_api(5)
    step = math.ceil(len(market_code_list) / len(pl))
    max_workers = len(pl)
    if step < min_step:
        step = min_step
        max_workers = math.ceil(len(market_code_list) / step)
    logi("worker num={} step={}".format(max_workers, step))

    mp = {}
    lost = []
    non_exist = []
    try:
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for i in range(0, len(market_code_list), step):
                idx = int(i / step)
                end = i + step if i + step < len(market_code_list) else len(market_code_list)
                future = executor.submit(query_bar_min_worker, market_code_list[i:end], pl[idx], exact)
                futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            err = future.exception()
            if not err:
                tmp_mp, tmp_lost, tmp_non_exist = future.result()
                mp.update(tmp_mp)
                lost.extend(tmp_lost)
                non_exist.extend(tmp_non_exist)
            else:
                loge("error={}".format(err))
    except Exception as e:
        loge("exception {}".format(e))
    finally:
        api_pool.release_api()

    spent = round(time.time() - start, 2)
    if len(lost) < xnum and len(non_exist) < xnum:
        logi("code num={} spent {} seconds. lost={} non_exist={}"
             .format(len(market_code_list), spent, lost, non_exist))
    else:
        logi("code num={} spent {} seconds. lost_num={} non_exist_num={}"
             .format(len(market_code_list), spent, len(lost), len(non_exist)))
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

    if IS_TEST_RUN:
        return True

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

        # 增加下一个定时器
        for tt in exp:
            que.remove(tt)
            tt += datetime.timedelta(minutes=BAR_PERIOD)
            if tt.hour == 15 and tt.minute == 0:
                tt += datetime.timedelta(minutes=1)  # 1455~1500 这根K线晚一分钟再查询
            elif tt.hour == 15 and tt.minute == BAR_PERIOD + 1:
                tt -= datetime.timedelta(minutes=1)  # 1500~1505 恢复之前的晚1分钟 这根K线其实不存在，查询会返回1455~1500的K线
            logi("next query time={}".format(tt))
            que.append(tt)

        if not now_is_tradedate():
            continue

        last_slot = get_our_last_slot()
        if not IS_TEST_RUN and last_slot not in valid_slots:
            continue

        mp, lost, non_exist = query_bar_min(mcodes, api_pool, exact=True)
        if lost or non_exist:
            lost.extend(non_exist)
            tmp_mp, tmp_lost, non_exist = query_bar_min(lost, api_pool, exact=False)
            assert len(lost) == len(tmp_mp.keys())
            mp.update(tmp_mp)

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
    api_pool: ApiPool = ApiPool(7)
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
