import datetime
import json
import random
import time

from pytdx.hq import TdxHq_API

import pathlib

from tdx1min.collect import CollectEngine
from tdx1min.db_ticks import Bar1Min
from tdx1min.api_pool import ApiPool
from tdx1min.tdx_stg import read_cfg, BarMinData
from tdx1min.vnlog import logi
from pytdx.reader import TdxDailyBarReader, TdxFileNotFoundException

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
        # sc = api.get_security_count(0)
        # print("sz security count={}".format(sc))
        # sc = api.get_security_count(1)
        # print("sh security count={}".format(sc))
        #
        # stock_list = api.get_security_list(0, 0)  # 深市从0开始
        # print(len(stock_list), stock_list)
        # stock_list = api.get_security_list(1, 517)  # 沪市从517开始 ???
        # print(len(stock_list), stock_list)
        while 1:
            stocks = api.get_security_quotes([(1, '600036')])
            for d in stocks:
                # d = stocks[0]
                now = datetime.datetime.now()
                stx = datetime.datetime.strptime(d['servertime'], "%H:%M:%S.%f")
                sty = datetime.datetime(year=now.year, month=now.month, day=now.day,
                                        hour=stx.hour, minute=stx.minute,
                                        second=stx.second, microsecond=stx.microsecond)
                st = sty.timestamp()
                diff = round(now.timestamp() - st, 3)
                logi("now={} servertime={} difftime={} code={} price={} "
                     .format(now.time(), d['servertime'], diff, d['code'], d['price']))
            time.sleep(3)


def tst_tdx02():
    api = TdxHq_API()
    mcodes, _ = read_cfg()
    idx = 0
    with api.connect(ip=HOST, port=7709, time_out=60):
        while 1:
            # data = api.get_security_bars(8, 1, '688567', 0, 3)
            # data = api.get_security_bars(8, 0, '003043', 0, 2)
            # data = api.get_security_bars(8, 1, '603290', 0, 3)
            data = api.get_security_bars(8, mcodes[idx][0], mcodes[idx][1], 0, 2)
            # print(type(data))
            # print(data)
            # data.reverse()
            idx += 1
            if idx >= len(mcodes):
                idx = 0
            for i, d in enumerate(data):
                logi("#{} code={} datetime={} open={} close={}"
                     .format(i, mcodes[idx], d['datetime'], d['open'], d['close']))
            time.sleep(3)


def tst_tdx03():
    api = TdxHq_API()

    idx = 0

    # mcodes, _ = read_cfg()
    mcodes = [(1, '600036'), (1, '600519')]
    with api.connect(ip=HOST, port=7709, time_out=60):
        while 1:
            bars = api.get_security_bars(8, mcodes[idx][0], mcodes[idx][1], 0, 1)
            b = bars[0]
            quotes = api.get_security_quotes([mcodes[idx]])
            q = quotes[0]

            idx += 1
            if idx >= len(mcodes):
                idx = 0

            logi("code={} bar.datetime={} bar.open={} bar.close={} q.servertime={} q.price={}"
                 .format(mcodes[idx], b['datetime'], b['open'], b['close'], q['servertime'], q['price']))

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
            if i == 1600 - 800:
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


def read_log():
    file = r'E:\chenzhenwei\PycharmProjects\quant\tdx1min\.workdir\logs\vt_20230808.log'
    # code = '603290'
    # code = '600620'
    with open(file, 'r') as fp:
        while 1:
            line = fp.readline()
            if not line:
                break
            idx = line.find('query_ticks 192 INFO')
            if idx >= 0:
                t = line[0:idx].strip()
                line = fp.readline()  # 忽略stocks
                line = fp.readline().strip()
                assert line.startswith('detail=')
                idx = line.find('detail=')
                data = line[idx + len('detail='):]
                # print(ticks_tmp)
                data = data.replace("\'", "\"")
                ticks_tmp: dict = json.loads(data)
                # print(t,ticks_tmp)
                print_time = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S,%f")
                # print('=====',type(print_timex), print_timex, print_time)
                pt: float = print_time.timestamp()
                for slot in ticks_tmp:
                    for code in ticks_tmp[slot]:
                        stx = datetime.datetime.strptime(ticks_tmp[slot][code]['servertime'], "%H:%M:%S.%f")
                        sty = datetime.datetime(year=print_time.year, month=print_time.month, day=print_time.day,
                                                hour=stx.hour, minute=stx.minute,
                                                second=stx.second, microsecond=stx.microsecond)
                        st = sty.timestamp()
                        diff = round(pt - st, 5)

                        print(slot, code, t, ticks_tmp[slot][code], diff)
                # break


def tst_tdx04():
    api = TdxHq_API()
    with api.connect(ip=HOST, port=7709, time_out=60):
        market, code = 1, '600000'
        # data = api.get_security_bars(8, market, code, 0, 1)
        # print(data)
        data = api.get_minute_time_data(market, code)
        print(data)


def ttt():
    # HOST = "110.41.147.114"
    api = TdxHq_API()
    mcodes, _ = read_cfg()
    idx = 0
    with api.connect(ip=HOST, port=7709, time_out=60):
        while 1:
            # data = api.get_security_bars(8, 1, '688567', 0, 3)
            # data = api.get_security_bars(8, 0, '003043', 0, 2)
            data = api.get_security_bars(8, mcodes[idx][0], mcodes[idx][1], 0, 2)
            # print(type(data))
            # data.reverse()
            idx += 1
            if idx >= len(mcodes):
                idx = 0
            # d['datetime'], d['open'], d['close']
            # datetime=2023-07-31 15:00 open=53.58 close=53.58
            d = data[0]
            tmp_datetime = d['datetime'].replace("-", "").replace(" ", "")  # ==2023073115:00
            st = tmp_datetime[8:] + ":00.000"  # == 15:00:00.000
            filled_date = tmp_datetime.replace(":", "")  # ==202307311500
            b: BarMinData = BarMinData(date="11111", time="slot", code="code",
                                       open_st=st, open=d['close'],  # open和close使用一样的值填充
                                       close_st=st, close=d['close'],
                                       fill_date=filled_date)
            db_bar: Bar1Min = Bar1Min(date=b.date, time=b.time, code=b.code,
                                      open_st=b.open_st, open=b.open,
                                      close_st=b.close_st, close=b.close, fill_date=b.fill_date)
            print(b)
            print(db_bar)
            time.sleep(3)


def tst_tdx_reader():
    reader = TdxDailyBarReader(r"D:\new_tdx\vipdoc")  # r"C:\zd_zsone\vipdoc")

    df = reader.get_df("600036", "sh")
    # df = reader.get_df(r"D:\new_tdx\vipdoc\sh\minline\sh600000.lc1")
    print(df)


if __name__ == '__main__':
    # ss = read_stocks()
    # print(len(ss),ss)
    # read_cfg()
    # tst_tdx()
    # tst_tdx01()
    tst_tdx02()
    # tst_tdx03()
    # tst_tdx04()
    # tst_tdx_reader()
    # print(day_1min_slots())
    # print(slot_from_servertime('10:18:30.486'))
    # print(slot_from_servertime('9:18:30.486'))
    # print(cur_date())
    # tst_find_info_from_prev_slot()
    # write_stg_price('0931', 11.3, 12.33333)
    # read_log()
    pass
