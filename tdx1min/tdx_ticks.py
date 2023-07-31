import datetime
import os
import random
import time

from pytdx.hq import TdxHq_API

from tdx1min.tdx_cfg import rand_hq_host
from tdx1min.vnlog import logi, logd, loge, logw
from tdx1min.db_ticks import Bar1Min, crt_bar1min
from tdx1min.trade_calendar import now_is_tradedate, CHINA_TZ

# HOST = "110.41.147.114"
_hq_host = rand_hq_host('深圳')
logi("HQ HOST={}".format(_hq_host))


def read_cfg():
    path = os.path.dirname(__file__)
    path = os.path.join(path, 'Actvty_cfg.csv')

    codes = []
    with open(path, "r") as fp:
        lines = fp.readlines()
        for line in lines:
            tmp = line.strip().split(',')
            if tmp[0] == 'SecuCode':
                continue
            code = tmp[0].strip()
            market = 1 if code[0:2].lower() == 'sh' else 0
            codes.append((market, tmp[0].strip()[2:]))
    logi("read_cfg code_num={} codes={}".format(len(codes), codes))
    return codes


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


def find_info_from_prev_slot(cur_slot: str, code, one_min_map: dict):
    keys = [k for k in one_min_map.keys()]
    keys.sort(reverse=True)
    # print("keys=", keys)

    for slot in keys:
        if cur_slot <= slot:
            continue
        if slot in one_min_map and code in one_min_map[slot]:
            return slot, one_min_map[slot][code]

    return None, None


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
    mcodes = read_cfg()
    valid_slots = day_1min_slots()
    logi("stock num={} valid_slots={}".format(len(mcodes), valid_slots))

    que = get_query_timer()

    one_min_map = {}
    fill_date_map = {}  # 可能停牌导致
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

            ticks_tmp = query_ticks(api, mcodes)
            start = time.time()
            for slot in ticks_tmp:
                if slot not in one_min_map:
                    one_min_map[slot] = {}
                for code in ticks_tmp[slot]:
                    tick_tmp_servertime = ticks_tmp[slot][code]['servertime']
                    tick_tmp_price = ticks_tmp[slot][code]['price']
                    tick_tmp_sp = (tick_tmp_servertime, tick_tmp_price)
                    if code not in one_min_map[slot]:
                        one_min_map[slot][code] = [tick_tmp_sp, tick_tmp_sp]
                    else:
                        one_min_map[slot][code][1] = tick_tmp_sp
            spent = round(time.time() - start, 3)
            logi("end query. spent={} now={} expire_timer={}".format(spent, datetime.datetime.now(), exp))
            if spent >= 1:
                logw("merge spent too much time {}".format(spent))
            if len(exp) > 1:
                loge("miss some query. exp={}".format(exp))
            last = exp[len(exp) - 1]

            if last.second == 3 and last.microsecond == 0:
                start = time.time()
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

                bars = save_bars_of_last_slot(api, mcodes, one_min_map, fill_date_map, last_slot)

                spent = round(time.time() - start, 3)
                logi("save #{} bars to db spent {} seconds".format(len(bars), spent))
                if spent >= 1.:
                    logw("save db spent too many time {}".format(spent))


def save_bars_of_last_slot(api, mcodes, one_min_map, fill_date_map, last_slot):
    today_date = cur_date()
    bars = []
    if last_slot in one_min_map:
        for market, code in mcodes:
            if code in one_min_map[last_slot]:
                open_close = one_min_map[last_slot][code]
                bars.append(Bar1Min(date=today_date, time=last_slot, code=code,
                                    open_st=open_close[0][0], open=open_close[0][1],
                                    close_st=open_close[1][0], close=open_close[1][1]))
            else:
                logw("code {} has no slot {} in one_min_map".format(code, last_slot))
                fill_slot, open_close = find_info_from_prev_slot(last_slot, code, one_min_map)
                if open_close:
                    logi("code {} slot {} use some prev slot as filled data. fill_slot={} open_close={}"
                         .format(code, last_slot, fill_slot, open_close))
                    bars.append(Bar1Min(date=today_date, time=last_slot, code=code,
                                        open_st=open_close[1][0], open=open_close[1][1],  # open和close使用一样的值填充
                                        close_st=open_close[1][0], close=open_close[1][1]))
                else:
                    if code in fill_date_map:
                        b: Bar1Min = fill_date_map[code]
                        b.time = last_slot
                        logi("code {} slot{} use 1min kdata as filled data. fill_data={} fill_time={}"
                             .format(code, last_slot, b.fill_date, b.open_st))
                        bars.append(b)
                    else:
                        data = api.get_security_bars(8, market, code, 0, 1)  # 查询最近的1分钟线
                        if data:
                            logi("code {} succeed to get data data={}".format(code, data))
                            # d['datetime'], d['open'], d['close']ar
                            d = data[0]
                            servertime = d['datetime'][12:17] + ":00.000"
                            actural_date = d['datetime'].replace("-", "")[0:8]
                            b: Bar1Min = Bar1Min(date=today_date, time=last_slot, code=code,
                                                 open_st=servertime, open=d['close'],  # open和close使用一样的值填充
                                                 close_st=servertime, close=d['close'],
                                                 fill_date=actural_date)
                            fill_date_map[code] = b
                            logi("code {} slot{} use 1min kdata as filled data. fill_data={} fill_time={}"
                                 .format(code, last_slot, b.fill_date, b.open_st))
                            bars.append(b)
                        else:
                            loge("code {} fail to get data by get_security_bars".format(code))
        # del one_min_map[last_slot]
    else:
        logi("no tick in slot {}".format(last_slot))
    if bars:
        logd("bars={}".format(bars))
        crt_bar1min(bars)
    return bars


if __name__ == '__main__':
    # tst_get_query_timer()
    # tst_find_info_from_prev_slot()
    tdx_tick()
    pass
