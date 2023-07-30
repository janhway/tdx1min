import datetime
import os
import random
import time

from pytdx.hq import TdxHq_API

from tdx1min.tdx_cfg import rand_hq_host
from tdx1min.vnlog import logi, logd, loge
from tdx1min.db_ticks import TdxTick, crt_ticks, Bar1Min, crt_bar1min
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

    tnow = datetime.datetime.now()
    logi("now={}".format(tnow))
    tn = float(tnow.second) + float(tnow.microsecond) / 1000000.0
    if tn < 1:
        deltas = [1, 2, 58, 59, 60]
    elif tn < 2:
        deltas = [2, 58, 59, 60, 61]
    elif tn < 58:
        deltas = [58, 59, 60, 61, 62]
    elif tn < 59:
        deltas = [59, 60, 61, 62, 60 + 58]
    else:
        deltas = [60, 61, 62, 60 + 58, 60 + 59]

    for d in deltas:
        if d < 60:
            t = datetime.datetime(year=tnow.year, month=tnow.month, day=tnow.day,
                                  hour=tnow.hour, minute=tnow.minute, second=d, microsecond=0)
        else:
            tmp = tnow + datetime.timedelta(minutes=1)
            t = datetime.datetime(year=tmp.year, month=tmp.month, day=tmp.day,
                                  hour=tmp.hour, minute=tmp.minute, second=d % 60, microsecond=0)
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
                slot = s['servertime'][0:5]
                code = s['code']
                if slot not in ticks_map:
                    ticks_map[slot] = {}

                ticks_map[slot][code] = {"servertime":s['servertime'], "price":s['price']}
        else:
            loge("{} failed".format(i))
        # print(stocks)
    spent = time.time() - start
    logi("got #{} ticks from tdx. spent time={} start time={}".format(len(ticks_map.keys()), spent, now))
    return ticks_map


def need_query():
    if not now_is_tradedate():
        return False

    fstart = datetime.time(9, 24, 59, tzinfo=CHINA_TZ)
    fend = datetime.time(11, 30, 3, tzinfo=CHINA_TZ)
    sstart = datetime.time(12, 59, 59, tzinfo=CHINA_TZ)
    send = datetime.time(15, 0, 3, tzinfo=CHINA_TZ)

    current_time = datetime.datetime.now(tz=CHINA_TZ).time()
    if (fstart <= current_time <= fend) or (sstart <= current_time <= send):
        return True
    return False


def tdx_tick():
    ss = read_cfg()
    logi("stock num={}".format(len(ss)))

    que = get_query_timer()

    one_min_map = {}

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
            # if not need_query():
            #     continue

            ticks_tmp = query_ticks(api, ss)
            for slot in ticks_tmp:
                if slot not in one_min_map:
                    one_min_map[slot] = {}
                for code in ticks_tmp[slot]:
                    if code not in one_min_map[slot]:
                        one_min_map[slot][code] = [ticks_tmp[slot][code]['price'], ticks_tmp[slot][code]['price']]
                    else:
                        one_min_map[slot][code][1] = ticks_tmp[slot][code]['price']
            logi("end query. now={} expire_timer={}".format(datetime.datetime.now(), exp))
            if len(exp) > 1:
                loge("miss some query. exp={}".format(exp))
            last = exp[len(exp) - 1]

            if last.second == 2:
                start = time.time()
                cur_slot_x = datetime.datetime(year=1900, month=1, day=1,
                                               hour=last.hour, minute=last.minute, second=0, microsecond=0)
                last_slot_x = cur_slot_x - datetime.timedelta(minutes=1)
                last_slot = last_slot_x.strftime("%H:%M")
                bars = []
                if last_slot in one_min_map:
                    for market,code in ss:
                        if code in one_min_map[last_slot]:
                            open_close = one_min_map[last_slot][code]
                            bars.append(Bar1Min(time=last_slot, code=code, open=open_close[0],close=open_close[1]))
                        else:
                            pass
                    del one_min_map[last_slot]
                else:
                    logi("no tick in slot {}".format(last_slot))
                    pass

                if bars:
                    logd("bars={}".format(bars))
                    crt_bar1min(bars)

                spent = round(time.time() - start, 3)
                logi("save #{} bars to db spent {} seconds".format(len(bars), spent))


if __name__ == '__main__':
    tdx_tick()
    pass
