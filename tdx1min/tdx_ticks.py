import datetime
import os
import random
import time
from pathlib import Path

from typing import List, Optional, Tuple, Dict

import dataclasses
from pytdx.hq import TdxHq_API

from tdx1min.tdx_cfg import rand_hq_host, WORK_DIR
from tdx1min.vnlog import logi, logd, loge, logw
from tdx1min.db_ticks import Bar1Min, crt_bar1min
from tdx1min.trade_calendar import now_is_tradedate, CHINA_TZ

# HOST = "110.41.147.114"
_hq_host = rand_hq_host('深圳')
logi("HQ HOST={}".format(_hq_host))


@dataclasses.dataclass
class Bar1MinData(object):
    code: str
    date: str
    time: str
    open: str
    open_st: str  # servertime
    close: str
    close_st: str  # servertime
    fill_date: str = None


@dataclasses.dataclass
class CfgItData(object):
    # SecuCode, Cur_TradingDay, updatetime, Ltg, preLtg, pre_cp, net0
    SecuCode: str
    Cur_TradingDay: str
    updatetime: str
    Ltg: float
    preLtg: float
    pre_cp: float
    net0: float


def read_cfg() -> Tuple[List[Tuple[int, str]], Dict[str, CfgItData]]:
    path = os.path.dirname(__file__)
    path = os.path.join(path, 'Actvty_cfg.csv')

    codes = []
    cfg: Dict[str, CfgItData] = {}
    with open(path, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            tmp = line.strip().split(',')
            if tmp[0] == 'SecuCode':
                continue
            it: CfgItData = CfgItData(SecuCode=tmp[0], Cur_TradingDay=tmp[1], updatetime=tmp[2],
                                      Ltg=float(tmp[3]), preLtg=float(tmp[4]), pre_cp=float(tmp[5]), net0=float(tmp[6]))
            code = tmp[0].strip()
            market = 1 if code[0:2].lower() == 'sh' else 0
            codes.append((market, tmp[0].strip()[2:]))
            cfg[code] = it
    logi("read_cfg code_num={} codes={} cfg={}".format(len(codes), codes, cfg))
    return codes, cfg


def cal_pre_tmap(cfg: Dict[str, CfgItData]):
    # 首先计算初始除数：
    # Pretmap = sum(pre_cp * preLtg) ，即分别计算stg_cfg列表中每个品种的pre_cp和preLtg的乘数，再全部汇总求和。
    pre_tmap = 0.0
    for code in cfg.keys():
        pre_tmap += cfg[code].pre_cp * cfg[code].preLtg
    return pre_tmap


def cal_open_close(slot: str, pre_tmap: float,
                   cfg: Dict[str, CfgItData], one_min_map: Dict[str, Dict[str, Bar1MinData]]):
    # 对stg_cfg列表中每个品种下一个交易日实盘的open_price,close_price,以及Ltg，分别求乘数Open_price*Ltg,close_price*Ltg,然后统计所有品种各乘数的累计值（汇总求和），由此计算出Stg指数的点位的open，close价格
    # Open = sum(open_price*Ltg)/Pretmap*net0
    # Close = sum(close_price*Ltg)/Pretmap*net0
    open = 0.
    close = 0.
    for code in cfg.keys():
        open += float(one_min_map[slot][code].open) * cfg[code].Ltg
        close += float(one_min_map[slot][code].close) * cfg[code].Ltg
    open = open / pre_tmap * cfg[code].net0
    close = close / pre_tmap * cfg[code].net0
    return open, close


def get_stg_path():
    folder_path = Path(WORK_DIR)
    folder_path = folder_path.joinpath("stg")
    if not folder_path.exists():
        folder_path.mkdir()
    print("get_stg_path, current path={} log_path={}".format(Path.cwd(), folder_path))
    return folder_path


def write_stg_price(slot: str, open_price: float, close_price: float):
    file = get_stg_path()
    file = file.joinpath("stg_" + datetime.datetime.now(tz=CHINA_TZ).strftime("%Y%m%d") + ".csv")
    if not file.exists():
        with open(file, "w") as fp:
            fp.write('Code,open,close,dt\n')
    open_price = round(open_price, 3)
    close_price = round(close_price, 3)

    tmp = ['Stg', str(open_price), str(close_price), slot, "\n"]
    with open(file, "a") as fp:
        fp.write(','.join(tmp))
    return


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


def need_query():
    if not now_is_tradedate():
        return False

    fstart = datetime.time(9, 24, 59, tzinfo=CHINA_TZ)
    fend = datetime.time(11, 30, 59, tzinfo=CHINA_TZ)
    sstart = datetime.time(12, 59, 59, tzinfo=CHINA_TZ)
    send = datetime.time(15, 0, 59, tzinfo=CHINA_TZ)

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    if (fstart <= current_time <= fend) or (sstart <= current_time <= send):
        return True
    return False


def day_1min_slots():
    fstart = datetime.time(9, 25, 0, tzinfo=CHINA_TZ)
    fend = datetime.time(11, 30, 0, tzinfo=CHINA_TZ)
    sstart = datetime.time(13, 0, 0, tzinfo=CHINA_TZ)
    send = datetime.time(15, 0, 0, tzinfo=CHINA_TZ)

    ret = []
    start = datetime.datetime(year=1900, month=1, day=1,
                              hour=fstart.hour, minute=fstart.minute, second=0, microsecond=0)
    while start.time() < send:
        if fstart <= start.time() < fend or sstart <= start.time() < send:
            slot = start.strftime("%H%M")
            ret.append(slot)
        start = start + datetime.timedelta(minutes=1)
    return ret


def cur_date():
    n = datetime.datetime.now(tz=CHINA_TZ)
    return n.strftime("%Y%m%d")


def find_info_from_prev_slot(cur_slot: str, code, one_min_map: dict) -> Optional[Bar1MinData]:
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
    valid_slots = day_1min_slots()
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

            if len(exp) > 1:
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
                bars: List[Bar1MinData] = fill_slot_date(api, mcodes, one_min_map, last_slot)
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
                tmp_bar: Bar1MinData = Bar1MinData(date=today_date, time=slot, code=code,
                                                   open_st=tmp_st, open=tmp_price,
                                                   close_st=tmp_st, close=tmp_price)
                one_min_map[slot][code] = tmp_bar
            else:
                one_min_map[slot][code].clost_st = tmp_st
                one_min_map[slot][code].clost = tmp_price


# 补充本slot没有tick数据的情况
# 停牌、正常情况也可能出现的本slot没有tick数据的情况
def fill_slot_date(api, mcodes, one_min_map, last_slot) -> List[Bar1MinData]:
    today_date = cur_date()
    bars: List[Bar1MinData] = []
    if last_slot not in one_min_map:
        loge("no tick in slot {}".format(last_slot))
        return bars

    for market, code in mcodes:
        if code in one_min_map[last_slot]:
            tmp_data: Bar1MinData = one_min_map[last_slot][code]
            assert last_slot == tmp_data.time
            bars.append(tmp_data)
        else:
            logw("code {} has no slot {} in one_min_map".format(code, last_slot))
            tmp_data: Bar1MinData = find_info_from_prev_slot(last_slot, code, one_min_map)
            if tmp_data:
                logi("code {} slot {} fill data {}".format(code, last_slot, tmp_data))
                b: Bar1MinData = Bar1MinData(date=today_date, time=last_slot, code=code,
                                             open_st=tmp_data.close_st, open=tmp_data.close,
                                             close_st=tmp_data.close_st, close=tmp_data.close,
                                             fill_date=tmp_data.fill_date)

                one_min_map[last_slot][code] = b
                bars.append(b)
            else:
                data = api.get_security_bars(8, market, code, 0, 1)  # 查询最近的1分钟线
                if data:
                    logi("code {} succeed to get data data={}".format(code, data))
                    # d['datetime'], d['open'], d['close']ar
                    d = data[0]
                    st = d['datetime'][12:17] + ":00.000"
                    filled_date = d['datetime'].replace("-", "")[0:8]
                    b: Bar1MinData = Bar1MinData(date=today_date, time=last_slot, code=code,
                                                 open_st=st, open=d['close'],  # open和close使用一样的值填充
                                                 close_st=st, close=d['close'],
                                                 fill_date=filled_date)
                    one_min_map[last_slot][code] = b
                    logi("code {} slot{} filled data. fill_data={}".format(code, last_slot, b))
                    bars.append(b)
                else:
                    loge("code {} fail to get data by get_security_bars".format(code))

    if bars:
        logd("bars={}".format(bars))
    return bars


if __name__ == '__main__':
    # tst_get_query_timer()
    # tst_find_info_from_prev_slot()
    tdx_tick()
    pass
