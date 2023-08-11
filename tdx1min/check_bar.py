import datetime

import logging
from functools import partial
from typing import Tuple, List, Dict

from pytdx.hq import TdxHq_API
from tdx1min.tdx_bars import get_tdx_last_slot, get_our_last_slot
from tdx1min.tdx_cfg import BAR_PERIOD

from tdx1min.tdx_stg import CfgItData, read_cfg, day_bar_slots
from tdx1min.trade_calendar import cur_date
from tdx1min.vnlog import logi,logw,loge,logd

from tdx1min.db_ticks import BarMin, find_barmin

HOST = "110.41.147.114"

# xxLog = LogEngine(f"check_bar_{cur_date()}.log")
# logd = partial(xxLog.logger.log, logging.DEBUG)
# logi = partial(xxLog.logger.log, logging.INFO)
# logw = partial(xxLog.logger.log, logging.WARN)
# loge = partial(xxLog.logger.log, logging.ERROR)


def check_barmin_from_db():
    ret: Tuple[List[str], Dict[str, CfgItData]] = read_cfg()
    codes: List[str] = ret[0]
    today = cur_date()

    valid_slots = day_bar_slots()
    sel_slot = [f"{slot:04d}" for slot in range(930, 1500, 5) if f"{slot:04d}" in valid_slots]
    logi("len(sel_slot)={} len(valid_slots)={} sel_slot={}".format(len(sel_slot), len(valid_slots), sel_slot))
    api = TdxHq_API()

    with api.connect(ip=HOST, port=7709, time_out=60):

        for market, code in codes:
            db_bars_x: List[BarMin] = find_barmin(today, code)
            if not db_bars_x:
                loge("db_bars not found for {}".format(code))
                continue
            logi(">>> code={} len(db_bars_x)={}".format(code, len(db_bars_x)))
            cat = 0 if BAR_PERIOD==5 else 8
            data = api.get_security_bars(cat, market, code, 0, 270//BAR_PERIOD)
            # logd(data)
            # [OrderedDict([('open', 14.48), ('close', 14.5), ('high', 14.5), ('low', 14.48), ('vol', 22900.0),
            #               ('amount', 331680.0), ('year', 2023), ('month', 8), ('day', 1),
            #               ('hour', 11), ('minute', 20), ('datetime', '2023-08-01 11:20')]),
            #  OrderedDict([('open', 14.5), ('close', 14.48), ('high', 14.5), ('low', 14.47), ('vol', 65000.0),
            #               ('amount', 941361.0), ('year', 2023), ('month', 8), ('day', 1),
            #               ('hour', 11), ('minute', 21), ('datetime', '2023-08-01 11:21')]),
            #  ......]
            for d in data:
                tmp_dt = datetime.datetime.strptime(d['datetime'], "%Y-%m-%d %H:%M")
                dt_str = tmp_dt.strftime("%Y%m%d")
                tdx_slot_str = get_tdx_last_slot(dt=tmp_dt)
                slot_str = get_our_last_slot(dt=tmp_dt)
                if tdx_slot_str == '1300':
                    slot_str = '1129' if BAR_PERIOD == 1 else '1125'

                d['date'] = dt_str
                d['slot'] = slot_str

            db_bars = []
            for b in db_bars_x:
                if b.time in sel_slot:
                    db_bars.append(b)
            # if len(db_bars) != len(sel_slot):
            #     loge("{} {}".format(len(db_bars), len(sel_slot)))

            match_count = 0
            mis_open = 0
            mis_close = 0
            mis_all = 0
            for b in db_bars:
                found = None
                for d in data:
                    if d['date'] == b.date and d['slot'] == b.time:
                        found = d
                        break
                if not found:
                    loge("db_bars slot {} not found in data".format(b.time))
                    continue

                if float(b.open) != d['open'] and float(b.close) == d['close']:
                    loge("code {} slot {} open price mismatch. db vs tdx open {} vs {}"
                         .format(code, b.time, b.open, d['open']))
                    mis_open += 1
                elif float(b.open) == d['open'] and float(b.close) != d['close']:
                    loge("code {} slot {} close price mismatch. db vs tdx close {} vs {}"
                         .format(code, b.time, b.close, d['close']))
                    mis_close += 1
                elif float(b.open) != d['open'] and float(b.close) != d['close']:
                    loge("code {} slot {} all price mismatch. db vs tdx open {} vs {}, close {} vs {}"
                         .format(code, b.time, b.open, d['open'], b.close, d['close']))
                    mis_all += 1
                else:
                    match_count += 1
            logi("match stat: code={} total={} matched={} mis_open={} mis_close={} miss_all={}"
                 .format(code, len(db_bars), match_count, mis_open, mis_close, mis_all))


# def create_bar1min_from_log():
#     file = r'E:\chenzhenwei\PycharmProjects\quant\tdx1min\.workdir\logs\vt_20230808.log'
#     # code = '603290'
#     # code = '600620'
#     bar1min_map: dict = {}
#     with open(file, 'r') as fp:
#         while 1:
#             line = fp.readline()
#             if not line:
#                 break
#             idx = line.find('query_ticks 192 INFO')
#             if idx >= 0:
#                 t = line[0:idx].strip()
#                 line = fp.readline()  # 忽略slot
#                 line = fp.readline().strip()
#                 assert line.startswith('detail=')
#                 idx = line.find('detail=')
#                 data = line[idx + len('detail='):]
#                 # print(ticks_tmp)
#                 data = data.replace("\'", "\"")
#                 ticks_tmp_x: dict = json.loads(data)
#                 # print(t,ticks_tmp)
#                 print_time = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S,%f")
#                 # print('=====',type(print_timex), print_timex, print_time)
#                 pt: float = print_time.timestamp()
#                 pt_slot = print_time.strftime("%H%M")
#                 ticks_tmp = {pt_slot: {}}
#                 for slot_tmp in ticks_tmp_x:
#                     ticks_tmp[pt_slot].update(ticks_tmp_x[slot_tmp])
#
#                 update_bar1min(ticks_tmp, bar1min_map)
#
#                 # for slot in ticks_tmp:
#                 #     for code in ticks_tmp[slot]:
#                 #         stx = datetime.datetime.strptime(ticks_tmp[slot][code]['servertime'], "%H:%M:%S.%f")
#                 #         sty = datetime.datetime(year=print_time.year, month=print_time.month, day=print_time.day,
#                 #                                 hour=stx.hour, minute=stx.minute,
#                 #                                 second=stx.second, microsecond=stx.microsecond)
#                 #         st = sty.timestamp()
#                 #         diff = round(pt - st, 5)
#                 #
#                 #         print(slot, code, t, ticks_tmp[slot][code], diff)
#     return bar1min_map
#
#
# def check_bar1min_from_log():
#     ret: Tuple[List[str], Dict[str, CfgItData]] = read_cfg()
#     codes: List[str] = ret[0]
#     today = cur_date()
#
#     api = TdxHq_API()
#     valid_slots = day_bar_slots()
#
#     bar1min_map = create_bar1min_from_log()
#
#     with api.connect(ip=HOST, port=7709, time_out=60):
#
#         sel_slots = [str(slot) for slot in range(1000, 1100, 1) if str(slot) in valid_slots]
#
#         for market, code in codes:
#
#             data = api.get_security_bars(8, market, code, 0, 250)
#             # logd(data)
#             # [OrderedDict([('open', 14.48), ('close', 14.5), ('high', 14.5), ('low', 14.48), ('vol', 22900.0),
#             #               ('amount', 331680.0), ('year', 2023), ('month', 8), ('day', 1),
#             #               ('hour', 11), ('minute', 20), ('datetime', '2023-08-01 11:20')]),
#             #  OrderedDict([('open', 14.5), ('close', 14.48), ('high', 14.5), ('low', 14.47), ('vol', 65000.0),
#             #               ('amount', 941361.0), ('year', 2023), ('month', 8), ('day', 1),
#             #               ('hour', 11), ('minute', 21), ('datetime', '2023-08-01 11:21')]),
#             #  ......]
#             for d in data:
#                 tmp_dt = datetime.datetime.strptime(d['datetime'], "%Y-%m-%d %H:%M")
#                 tmp_dt -= datetime.timedelta(minutes=1)  # slot表示方式不一样 我们用开始时间，通达信用结束时间
#                 dt_str = tmp_dt.strftime("%Y%m%d")
#                 slot_str = tmp_dt.strftime("%H%M")
#                 d['date'] = dt_str
#                 d['slot'] = slot_str
#
#             match_count = 0
#             mis_open = 0
#             mis_close = 0
#             mis_all = 0
#             for sel_slot in sel_slots:
#                 if code not in bar1min_map[sel_slot]:
#                     loge("bar1min_map has no slot {}".format(sel_slot))
#                     continue
#
#                 found = None
#                 for d in data:
#                     if d['date'] == today and d['slot'] == sel_slot:
#                         found = d
#                         break
#                 if not found:
#                     loge("data has no slot {}".format(sel_slot))
#                     continue
#
#                 b = bar1min_map[sel_slot][code]
#                 if float(b.open) != d['open'] and float(b.close) == d['close']:
#                     loge("code {} slot {} open price mismatch. db vs tdx open {} vs {}"
#                          .format(code, b.time, b.open, d['open']))
#                     mis_open += 1
#                 elif float(b.open) == d['open'] and float(b.close) != d['close']:
#                     loge("code {} slot {} close price mismatch. db vs tdx close {} vs {}"
#                          .format(code, b.time, b.close, d['close']))
#                     mis_close += 1
#                 elif float(b.open) != d['open'] and float(b.close) != d['close']:
#                     loge("code {} slot {} all price mismatch. db vs tdx open {} vs {}, close {} vs {}"
#                          .format(code, b.time, b.open, d['open'], b.close, d['close']))
#                     mis_all += 1
#                 else:
#                     match_count += 1
#             logi("match stat: code={} total={} matched={} mis_open={} mis_close={} miss_all={}"
#                  .format(code, len(sel_slots), match_count, mis_open, mis_close, mis_all))


if __name__ == "__main__":
    check_barmin_from_db()
