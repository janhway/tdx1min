import datetime
import random
import time

from pytdx.hq import TdxHq_API

import pathlib

from tdx1min.tdx_ticks import read_cfg, day_1min_slots, slot_from_servertime, cur_date, tst_find_info_from_prev_slot
from tdx1min.vnlog import logi

HOST = "110.41.147.114"


def read_stocks():
    sz_stocks = []
    p = pathlib.Path(r"D:\new_tdx\vipdoc\sz\lday")
    for c in p.iterdir():
        if c.name.startswith("sz000") or c.name.startswith("sz300") or c.name.startswith("sz002"):
            sz_stocks.append((0, c.name[2:-4]))
    return sz_stocks


def tst_tdx01():
    api = TdxHq_API()
    with api.connect(ip=HOST, port=7709, time_out=60):
        sc = api.get_security_count(0)
        print("sz security count={}".format(sc))
        sc = api.get_security_count(1)
        print("sh security count={}".format(sc))

        stock_list = api.get_security_list(0, 0)  # 深市从0开始
        print(len(stock_list), stock_list)
        stock_list = api.get_security_list(1, 517)  # 沪市从517开始 ???
        print(len(stock_list), stock_list)


def tst_tdx02():
    api = TdxHq_API()
    with api.connect(ip=HOST, port=7709, time_out=60):
        while 1:
            # data = api.get_security_bars(8, 1, '688567', 0, 3)
            data = api.get_security_bars(8, 0, '003043', 0, 3)
            for i,d in enumerate(data):
                logi("#{} datetime={} open={} close={}".format(i,d['datetime'],d['open'],d['close']))
            time.sleep(3)


def query_ticks(api, ss):
    tmp = datetime.datetime.now()
    n = datetime.datetime(year=1900, month=1, day=1, hour=tmp.hour, minute=tmp.minute, second=tmp.second,
                          microsecond=tmp.microsecond)
    print("now={}".format(n))
    max_interval = 0
    max_microsecond = 0
    max_q = None
    start = time.time()
    step = 80
    for i in range(0, len(ss), step):
        end = i + step if i + step <= len(ss) else len(ss)
        stocks = api.get_security_quotes(ss[i:end])  # 每次最大返回80个，参数大于200会查询不出
        if stocks:
            print("{} stock quotes num={}".format(i, len(stocks)))
            if 1:  # i == 1600-80:
                for s in stocks:
                    q = datetime.datetime.strptime(s['servertime'], "%H:%M:%S.%f")
                    diff = n - q if n >= q else q - n
                    diff_sec = diff.days * 24 * 3600 + diff.seconds  # 忽略microsecond

                    if diff_sec > max_interval:
                        max_interval = diff_sec
                        max_microsecond = diff.microseconds
                        max_q = s
                    elif diff_sec == max_interval and diff.microseconds > max_microsecond:
                        # max_interval = diff_sec
                        max_microsecond = diff.microseconds
                        max_q = s
            if i == 1600-800:
                print(stocks)

        else:
            print("{} failed".format(i))
        # print(stocks)
    spent = time.time() - start
    print("spent time={} max_interval={} max_microsecond={} \n max_q={}"
          .format(spent, max_interval, max_microsecond, max_q))


def tst_tdx():
    api = TdxHq_API()
    with api.connect(ip=HOST, port=7709, time_out=60):
        # sc = api.get_security_count(0)
        # print("sz security count={}".format(sc))
        # sc = api.get_security_count(1)
        # print("sh security count={}".format(sc))
        #
        # # stock_list = api.get_security_list(1, 517)  # 沪市从517开始 ???
        # # print(len(stock_list), stock_list)
        #
        # log.info("获取股票行情")
        ss = read_cfg()
        print("stock num={}".format(len(ss)))

        query_ticks(api, ss)


if __name__ == '__main__':
    # ss = read_stocks()
    # print(len(ss),ss)
    # read_cfg()
    # tst_tdx()
    tst_tdx02()
    # print(day_1min_slots())
    # print(slot_from_servertime('10:18:30.486'))
    # print(slot_from_servertime('9:18:30.486'))
    # print(cur_date())
    # tst_find_info_from_prev_slot()
    pass
