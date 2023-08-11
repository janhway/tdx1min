import datetime
import random
import time

from typing import List, Optional, Dict

import dataclasses

from pytdx.hq import TdxHq_API
from tdx1min.tdx_cfg import rand_hq_host

from tdx1min.tdx_stg import read_cfg, cal_pre_tmap, day_bar_slots, need_query, write_stg_price, CfgItData
from tdx1min.trade_calendar import cur_date
from tdx1min.vnlog import logi, logd, loge, logw
from tdx1min.db_ticks import Bar1Min, crt_bar1min

# HOST = "110.41.147.114"
_hq_host = rand_hq_host('深圳')
logi("HQ HOST={}".format(_hq_host))


@dataclasses.dataclass
class BarMinData(object):
    code: str
    date: str
    time: str
    open: float
    open_st: str  # servertime
    close: float
    close_st: str  # servertime
    fill_date: str = None


def cal_open_close(slot: str, pre_tmap: float,
                   cfg: Dict[str, CfgItData], bars: Dict[str, BarMinData]):
    # 对stg_cfg列表中每个品种下一个交易日实盘的open_price,close_price,以及Ltg，分别求乘数Open_price*Ltg,close_price*Ltg,
    # 然后统计所有品种各乘数的累计值（汇总求和），由此计算出Stg指数的点位的open，close价格
    # Open = sum(open_price*Ltg)/Pretmap*net0
    # Close = sum(close_price*Ltg)/Pretmap*net0
    open_price = 0.
    close_price = 0.
    net0 = 0.
    bar_no_code = []
    for code in cfg.keys():
        # cfg_code = vt_symbol_to_cfg(code)
        if code not in bars:
            bar_no_code.append(code)  # loge("bars has no code {}".format(code))
            continue
        open_price += float(bars[code].open) * cfg[code].Ltg
        close_price += float(bars[code].close) * cfg[code].Ltg
        net0 = cfg[code].net0
    if bar_no_code:
        loge("slot {} bars has no code #{} {}".format(slot, len(bar_no_code), bar_no_code))
    open_price = open_price / pre_tmap * net0
    close_price = close_price / pre_tmap * net0
    return open_price, close_price


def get_query_timer():
    timer_list = []

    ts = []
    ts.extend([0, 1.5])
    for i in range(3, 59, 3):  # [3,57]
        ts.append(i)
    ts.append(57 + 1.5)
    print(len(ts), ts)

    tnow = datetime.datetime.now()
    logi("now={}".format(tnow))
    # tn = float(tnow.second) + float(tnow.microsecond) / 1000000.0

    for d in ts:
        sec = int(d)
        micro_sec = 0
        if not isinstance(d, int):
            micro_sec = int((float(d) - int(d)) * 1000000)
        t = datetime.datetime(year=tnow.year, month=tnow.month, day=tnow.day,
                              hour=tnow.hour, minute=tnow.minute, second=sec, microsecond=micro_sec)
        timer_list.append(t)
        logi("add start timer={}".format(t))
    return timer_list


def tst_get_query_timer():
    while 1:
        r = random.randint(50, 100)
        f = float(r) / 100.
        time.sleep(f)

        now = datetime.datetime.now()
        if 3 < now.second < 57:
            continue

        print(get_query_timer())


def slot_from_servertime(servertime: str) -> str:
    ridx = servertime.rfind(":")
    if ridx >= 0:
        tmp = servertime[0:ridx].replace(":", "")
        if tmp[0] == '9':
            tmp = "0" + tmp
        return tmp
    else:
        return ""


def query_ticks(api, ss) -> dict:
    now = datetime.datetime.now().time()
    start = time.time()
    step = 80
    ticks_map = {}
    for i in range(0, len(ss), step):
        end = i + step if i + step <= len(ss) else len(ss)
        stocks = api.get_security_quotes(ss[i:end])  # 每次最大返回80个，参数大于200会查询不出
        if stocks:
            logd("{} stock quotes num={}".format(i, len(stocks)))
            for s in stocks:
                # slot = datetime.datetime.now().strftime("%H%M")
                slot = slot_from_servertime(s['servertime'])
                # s['servertime'][0:5] if s['servertime'][0] != '9' else s['servertime'][0:4]
                code = s['code']
                if slot not in ticks_map:
                    ticks_map[slot] = {}

                ticks_map[slot][code] = {"servertime": s['servertime'], "price": s['price']}
            if i == 1040:
                logd("i={} stocks={}".format(i, stocks))
        else:
            loge("{} failed".format(i))
        # print(stocks)
    spent = round(time.time() - start, 3)
    logi("got #{} slot from tdx. spent time={} start time={} \n slot={} \ndetail={}"
         .format(len(ticks_map.keys()), spent, now, ticks_map.keys(), ticks_map))
    if spent >= 1.:
        logw("query tick spent too many time {}".format(spent))
    return ticks_map


def find_info_from_prev_slot(cur_slot: str, code, one_min_map: dict) -> Optional[BarMinData]:
    keys = [k for k in one_min_map.keys()]
    keys.sort(reverse=True)
    # print("keys=", keys)

    for slot in keys:
        if cur_slot <= slot:
            continue
        if slot in one_min_map and code in one_min_map[slot]:
            return one_min_map[slot][code]

    return None


def tst_find_info_from_prev_slot():
    one_min_map = {
        "0945": {
            "600036": [("", 1.35), ("", 1.36)],
            "600030": [("", 2.35), ("", 2.36)],
        },
        "0946": {
            "600036": [("", 11.35), ("", 11.36)],
            "600030": [("", 12.35), ("", 12.36)],
        },
        "1301": {
            "600036": [("", 21.35), ("", 21.36)],
            "600030": [("", 22.35), ("", 22.36)],
        }
    }

    cur_slot = "1300"
    code = "600036"
    r = find_info_from_prev_slot(cur_slot, code, one_min_map)
    print(cur_slot, code, r)

    cur_slot = "1250"
    r = find_info_from_prev_slot(cur_slot, code, one_min_map)
    print(cur_slot, code, r)

    cur_slot = "0946"
    r = find_info_from_prev_slot(cur_slot, code, one_min_map)
    print(cur_slot, code, r)


def tdx_tick():
    mcodes, cfg = read_cfg()
    pre_tmap = cal_pre_tmap(cfg)
    valid_slots = day_bar_slots()
    logi("pre_tmap={} stock num={} valid_slots={}".format(pre_tmap, len(mcodes), valid_slots))

    que = get_query_timer()

    one_min_map = {}
    # fill_date_map = {}  # 可能停牌导致
    first_slot = None
    # today_date = cur_date()
    api = TdxHq_API(auto_retry=True, heartbeat=True)
    with api.connect(ip=_hq_host['IPAddress'], port=int(_hq_host['Port']), time_out=60):
        while True:
            now = datetime.datetime.now()
            exp = []
            for t in que:
                if t < now:
                    exp.append(t)
            if not exp:
                time.sleep(0.1)
                continue

            for t in exp:
                que.remove(t)
                que.append(t + datetime.timedelta(minutes=1))

            if not need_query():
                continue

            # 1.从tdx查询tick信息
            ticks_tmp: dict = query_ticks(api, mcodes)

            # 2.使用新的tick信息更新1分钟Bar信息
            start = time.time()
            update_bar1min(ticks_tmp, one_min_map)
            spent = round(time.time() - start, 3)
            logi("update 1min bar. spent={} now={} expire_timer={}".format(spent, datetime.datetime.now(), exp))
            if spent >= 1:
                logw("update 1min bar spent too much time {}".format(spent))

            if len(exp) > 1 and first_slot:
                loge("miss some query. exp={}".format(exp))
            last = exp[len(exp) - 1]

            if last.second == 3 and last.microsecond == 0:
                # start = time.time()
                cur_slot_x = datetime.datetime(year=1900, month=1, day=1,
                                               hour=last.hour, minute=last.minute, second=0, microsecond=0)
                last_slot_x = cur_slot_x - datetime.timedelta(minutes=1)
                last_slot = last_slot_x.strftime("%H%M")
                if last_slot not in valid_slots:
                    if not first_slot:
                        first_slot = last_slot
                    continue
                elif not first_slot:  # 忽略第一个不完整的slot
                    first_slot = last_slot
                    continue

                # 填充缺少信息
                start = time.time()
                bars: List[BarMinData] = fill_slot_date(api, mcodes, one_min_map, last_slot)
                if last_slot in one_min_map:
                    assert (len(one_min_map[last_slot].keys()) == len(mcodes))
                spent = time.time() - start
                logi("fill data  spent={}".format(round(spent, 3)))
                if spent >= 1:
                    logw("fill data spent too much time {}".format(spent))

                # 计算和保存stg指数信息
                start = time.time()
                stg_open, stg_close = cal_open_close(last_slot, pre_tmap, cfg, one_min_map)
                write_stg_price(last_slot, stg_open, stg_close)
                spent = time.time() - start
                logi("cal stg {} spent={}".format(last_slot, round(spent, 3)))

                # begin save db. option
                start = time.time()
                db_bars = []
                for b in bars:
                    db_bar: Bar1Min = Bar1Min(date=b.date, time=b.time, code=b.code,
                                              open_st=b.open_st, open=b.open,
                                              close_st=b.close_st, close=b.close, fill_date=b.fill_date)
                    db_bars.append(db_bar)
                crt_bar1min(db_bars)

                spent = round(time.time() - start, 3)
                logi("save #{} bars to db spent {} seconds".format(len(bars), spent))
                if spent >= 1.:
                    logw("save db spent too many time {}".format(spent))
                # end save db. option


# 更新1分钟K线
def update_bar1min(ticks_tmp, one_min_map):
    today_date = cur_date()
    for slot in ticks_tmp:
        if slot not in one_min_map:
            one_min_map[slot] = {}
        for code in ticks_tmp[slot]:
            tmp_st = ticks_tmp[slot][code]['servertime']
            tmp_price = ticks_tmp[slot][code]['price']
            if code not in one_min_map[slot]:
                tmp_bar: BarMinData = BarMinData(date=today_date, time=slot, code=code,
                                                 open_st=tmp_st, open=tmp_price,
                                                 close_st=tmp_st, close=tmp_price)
                one_min_map[slot][code] = tmp_bar
            else:
                one_min_map[slot][code].clost_st = tmp_st
                one_min_map[slot][code].clost = tmp_price


# 补充本slot没有tick数据的情况
# 停牌、正常情况也可能出现的本slot没有tick数据的情况
def fill_slot_date(api, mcodes, one_min_map, slot) -> List[BarMinData]:
    today_date = cur_date()
    bars: List[BarMinData] = []
    if slot not in one_min_map:
        loge("no tick in slot {}".format(slot))
        return bars

    for market, code in mcodes:
        if code in one_min_map[slot]:
            tmp_data: BarMinData = one_min_map[slot][code]
            assert slot == tmp_data.time
            bars.append(tmp_data)
        else:
            # logw("code {} has no slot {} in one_min_map".format(code, slot))
            tmp_data: BarMinData = find_info_from_prev_slot(slot, code, one_min_map)
            if tmp_data:
                logi("code {} slot {} fill data. fill_data={}".format(code, slot, tmp_data))
                filled_date = tmp_data.fill_date if tmp_data.fill_date else tmp_data.date + tmp_data.time
                b: BarMinData = BarMinData(date=today_date, time=slot, code=code,
                                           open_st=tmp_data.close_st, open=tmp_data.close,
                                           close_st=tmp_data.close_st, close=tmp_data.close,
                                           fill_date=filled_date)

                one_min_map[slot][code] = b
                bars.append(b)
            else:
                data = api.get_security_bars(8, market, code, 0, 1)  # 查询最近的1分钟线
                if data:
                    logi("code {} succeed to get data data={}".format(code, data))
                    # d['datetime'], d['open'], d['close']
                    # datetime=2023-07-31 15:00 open=53.58 close=53.58
                    d = data[0]
                    tmp_datetime = d['datetime'].replace("-", "").replace(" ", "")  # ==2023073115:00
                    st = tmp_datetime[8:] + ":00.000"  # == 15:00:00.000
                    filled_date = tmp_datetime.replace(":", "")  # ==202307311500
                    b: BarMinData = BarMinData(date=today_date, time=slot, code=code,
                                               open_st=st, open=d['close'],  # open和close使用一样的值填充
                                               close_st=st, close=d['close'],
                                               fill_date=filled_date)
                    one_min_map[slot][code] = b
                    logi("code {} slot{} filled data. fill_data={}".format(code, slot, b))
                    bars.append(b)
                else:
                    loge("code {} fail to get data by get_security_bars".format(code))

    if bars:
        logd("bars={}".format(bars))
    return bars


if __name__ == '__main__':
    # tst_get_query_timer()
    # tst_find_info_from_prev_slot()
    # tdx_tick()
    pass
