import datetime
import os
from pathlib import Path

from typing import List, Tuple, Dict, Any

import dataclasses

from tdx1min.tdx_cfg import WORK_DIR, BAR_PERIOD
from tdx1min.vnlog import logi, loge, logw
from tdx1min.trade_calendar import now_is_tradedate, cur_date


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


def read_cfg(path=None) -> Tuple[List[Tuple[int, str]], Dict[str, CfgItData]]:
    fn = 'Stgtrd_cfg.csv'
    if not path:
        path = os.path.join(r"C:\ftp\params", fn)
        if not os.path.exists(path):
            path = os.path.join(os.path.dirname(__file__), fn)
    else:
        path = os.path.join(path, fn)
    logi("path={}".format(path))
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
    logi("read_cfg code_num={}".format(len(codes)))
    # logi("read_cfg codes={}".format(codes))
    # logi("read_cfg cfg={}".format(cfg))
    return codes, cfg


def cal_pre_tmap(cfg: Dict[str, CfgItData]):
    # 首先计算初始除数：
    # Pretmap = sum(pre_cp * preLtg) ，即分别计算stg_cfg列表中每个品种的pre_cp和preLtg的乘数，再全部汇总求和。
    pre_tmap = 0.0
    for code in cfg.keys():
        pre_tmap += cfg[code].pre_cp * cfg[code].preLtg
    return pre_tmap


def cal_open_close_new(slot: str, pre_tmap: float,
                   cfg: Dict[str, CfgItData], mp: Dict[str, Dict[str, Any]]):
    # 对stg_cfg列表中每个品种下一个交易日实盘的open_price,close_price,以及Ltg，分别求乘数Open_price*Ltg,close_price*Ltg,
    # 然后统计所有品种各乘数的累计值（汇总求和），由此计算出Stg指数的点位的open，close价格
    # Open = sum(open_price*Ltg)/Pretmap*net0
    # Close = sum(close_price*Ltg)/Pretmap*net0
    open_price = 0.
    close_price = 0.
    net0 = 0.
    bar_no_code = []
    # print(mp)
    for code in cfg.keys():
        # cfg_code = vt_symbol_to_cfg(code)
        codex = code[2:]
        if codex not in mp:
            bar_no_code.append(codex)  # loge("bars has no code {}".format(code))
            continue
        open_price += float(mp[codex]['open']) * cfg[code].Ltg
        close_price += float(mp[codex]['close']) * cfg[code].Ltg
        net0 = cfg[code].net0
    if bar_no_code:
        loge("slot {} bars has no code #{} {}".format(slot, len(bar_no_code), bar_no_code))
    open_price = open_price / pre_tmap * net0
    close_price = close_price / pre_tmap * net0
    return open_price, close_price


def get_stg_path():
    folder_path = Path(WORK_DIR)
    folder_path = folder_path.joinpath("stg")
    if not folder_path.exists():
        folder_path.mkdir()
    print("get_stg_path, current path={} log_path={}".format(Path.cwd(), folder_path))
    return folder_path


def write_stg_price(slot_time: str, open_price: float, close_price: float, output_path=None):
    if not output_path:
        file = get_stg_path()
    else:
        file = Path(output_path)
    file = file.joinpath("stg_" + datetime.datetime.now().strftime("%Y%m%d") + ".csv")

    title = 'Code,open,close,dt,CreateTime\n'

    if not file.exists():
        with open(file, "w") as fp:
            fp.write(title)

    slot = cur_date() + slot_time

    open_price = round(open_price, 5)
    close_price = round(close_price, 5)
    create_time = datetime.datetime.now().strftime("%H:%M:%S")
    tmp = ['Stg', str(open_price), str(close_price), slot, create_time]
    new_last_line = ','.join(tmp) + "\n"

    with open(file, "r") as fp:
        lines = fp.readlines()

    if lines:
        last_line = lines[-1].strip()
        if last_line:
            info = last_line.split(",")
            if info[3] == slot:
                if info[1] == str(open_price) and info[2] == str(close_price):
                    logi("dup equal slot {}.  do nothing".format(slot))
                    return

                logw("dup slot {}. replace it.".format(slot))
                lines[-1] = new_last_line
                with open(file, "w") as fp:
                    fp.writelines(lines)
                return

    with open(file, "a") as fp:
        if not lines:
            fp.write(title)
        fp.write(new_last_line)
    logi("new slot {}.".format(slot))
    return


def need_query():
    # return True  # comment for test code

    if not now_is_tradedate():
        return False

    fstart = datetime.time(9, 19, 59)
    fend = datetime.time(11, 30, 59)
    sstart = datetime.time(12, 59, 59)
    send = datetime.time(15, 5, 59)

    current_time = datetime.datetime.now().time()
    if (fstart <= current_time <= fend) or (sstart <= current_time <= send):
        return True
    return False


def day_bar_slots():
    fstart = datetime.time(9, 30, 0)
    fend = datetime.time(11, 30, 0)
    sstart = datetime.time(13, 0, 0)
    send = datetime.time(15, 0, 0)

    ret = []
    start = datetime.datetime(year=1900, month=1, day=1,
                              hour=fstart.hour, minute=fstart.minute, second=0, microsecond=0)
    while start.time() < send:
        if fstart <= start.time() < fend or sstart <= start.time() < send:
            slot = start.strftime("%H%M")
            ret.append(slot)
        start = start + datetime.timedelta(minutes=BAR_PERIOD)
    return ret


if __name__ == '__main__':
    # print(day_bar_slots())
    pass
