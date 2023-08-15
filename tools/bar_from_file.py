import datetime
import json
import os
import time

from mootdx.reader import Reader
import pandas as pd

from tdx1min.tdx_cfg import WORK_DIR
from tdx1min.tdx_stg import read_cfg_file, day_bar_slots
from tdx1min.vnlog import loge, logi


def file_his_component_bar():
    today = '20230814'
    cfg_file = f'Stgtrd_cfg_{today}.csv'
    cfg_file = os.path.join(r"c:\ftp\params", cfg_file)
    mcodes, _ = read_cfg_file(cfg_file)
    # mcodes = [(1, '600216'), (0, '300757')]
    # mcodes = [(0, '300757')]
    start = time.time()
    slots = day_bar_slots()

    tdx_dir = r'D:\new_tdx'
    reader = Reader.factory(market='std', tdxdir=tdx_dir)

    # 读取时间线数据
    # r: pd.DataFrame = reader.fzline(symbol='600036')
    # print(r.tail(5))

    mp_all = {}

    for market, code in mcodes:
        df: pd.DataFrame = reader.fzline(symbol=code)

        for slot in slots:
            if slot not in mp_all:
                mp_all[slot] = {}
            dt_str = today + slot
            dt = datetime.datetime.strptime(dt_str, "%Y%m%d%H%M")
            dt = dt + datetime.timedelta(minutes=5)
            # if dt.hour == 11 and dt.minute == 30:
            #     dt = dt + datetime.timedelta(hours=1, minutes=30)
            #     assert dt.hour == 13 and dt.minute == 0
            tdx_slot = dt.strftime("%H%M")
            idx = dt.strftime("%Y-%m-%d %H:%M:%S")
            if idx in df.index:
                mp_all[slot][code] = {'open': round(df.loc[idx]['open'],2), 'close': round(df.loc[idx]['close'],2)}
                continue
            print("lack of record code={} slot={} idx={}".format(code, slot, idx))
            found = False
            for i in range(len(df) - 1, -1, -1):
                row_idx = str(df.index[i])
                row_slot = row_idx.replace(" ", "").replace(":", "")[8:12]
                row = df.iloc[i]
                if idx > row_idx or (idx == row_idx and tdx_slot > row_slot):
                    mp_all[slot][code] = {'open': round(row['close'],2), 'close': round(row['close'],2)}
                    found = True
                    print("lack of record code={} slot={} idx={} instead_idx={}".format(code, slot, idx, row_idx))

                    break

            assert found

    output_path = WORK_DIR

    mp_filename = os.path.join(output_path, f"file_bar_{today}.txt")
    with open(mp_filename, 'w') as fp:
        json.dump(mp_all, fp)
    spent = time.time() - start
    logi("finished. spent={}".format(spent))


if __name__ == "__main__":
    file_his_component_bar()
