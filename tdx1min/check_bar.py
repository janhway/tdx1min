import datetime
from typing import Tuple, List, Dict

from pytdx.hq import TdxHq_API

from tdx1min.tdx_ticks import CfgItData, read_cfg, cur_date, day_1min_slots
from tdx1min.vnlog import loge, logd, logi

from tdx1min.db_ticks import Bar1Min, find_bar1min

HOST = "110.41.147.114"


def check_bar1min():
    ret: Tuple[List[str], Dict[str, CfgItData]] = read_cfg()
    codes: List[str] = ret[0]
    today = cur_date()

    api = TdxHq_API()
    valid_slots = day_1min_slots()
    with api.connect(ip=HOST, port=7709, time_out=60):

        for market,code in codes:
            db_bars_x: List[Bar1Min] = find_bar1min(today, code)
            if not db_bars_x:
                loge("db_bars not found for {}".format(code))
                continue
            data = api.get_security_bars(8, market, code, 0, 250)
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
                tmp_dt -= datetime.timedelta(minutes=1)  # slot表示方式不一样 我们用开始时间，通达信用结束时间
                dt_str = tmp_dt.strftime("%Y%m%d")
                slot_str = tmp_dt.strftime("%H%M")
                d['date'] = dt_str
                d['slot'] = slot_str

            sel_slot = [str(slot) for slot in range(1000, 1100, 1) if str(slot) in valid_slots]
            db_bars = []
            for b in db_bars_x:
                if b.time in sel_slot:
                    db_bars.append(b)
            if len(db_bars) != len(sel_slot):
                loge("{} {}".format(len(db_bars),len(sel_slot)))

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


if __name__ == "__main__":
    check_bar1min()
